package netclient

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"math"
	"net"
	"readnetfs/internal/pkg/cacheclient"
	"readnetfs/internal/pkg/common"
	"readnetfs/internal/pkg/fsclient"
	"sync"
	"time"
)

var DEADLINE = 10 * time.Second

type PeerInfo struct {
	Load            int64
	Rate            int64
	CurrentRequests *semaphore.Weighted
}

type NetClient struct {
	statsdSocket     net.Conn
	peerNodes        map[string]*PeerInfo
	plock            sync.Mutex
	fPathRemoteCache *expirable.LRU[fsclient.RemotePath, []string]
}

func NewNetClient(statsdAddrPort string, peerNodes []string) *NetClient {
	fPathRemoteCache := expirable.NewLRU[fsclient.RemotePath, []string](cacheclient.MEM_TOTAL_CACHE_B/cacheclient.MEM_PER_FILE_CACHE_B,
		func(key fsclient.RemotePath, value []string) {}, cacheclient.PATH_TTL)
	pMap := make(map[string]*PeerInfo)
	for _, peer := range peerNodes {
		pMap[peer] = &PeerInfo{CurrentRequests: semaphore.NewWeighted(int64(cacheclient.MAX_CONCURRENT_REQUESTS))}
	}
	statsdSocket := common.NewStatsdConn(statsdAddrPort)
	return &NetClient{statsdSocket: statsdSocket, peerNodes: pMap, fPathRemoteCache: fPathRemoteCache}
}

type netReply interface {
	common.FileResponse | common.DirInfo | common.NetInfo
}

func (f *NetClient) getPeer(path fsclient.RemotePath) (string, error) {
	candidates, ok := f.fPathRemoteCache.Get(path)
	if !ok {
		candidates = make([]string, 0)
		var thisInfo fs.FileInfo
		peers := f.peers()
		for _, peer := range peers {
			info, err := f.fileInfo(path, peer)
			if err == nil {
				if thisInfo == nil {
					thisInfo = info
				} else {
					if info.Size() != thisInfo.Size() {
						return "", errors.New("file has different sizes on different peers" + string(path))
					}
				}
				candidates = append(candidates, peer)
			}
		}
		if len(candidates) > 0 {
			f.fPathRemoteCache.Add(path, candidates)
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("no peer candidates for file" + string(path))
	}
	//find candidate with the lowest load
	lowest := int64(math.MaxInt64)
	lowestPeer := ""
	for _, peer := range candidates {
		f.plock.Lock()
		if f.peerNodes[peer].Load < lowest {
			lowest = f.peerNodes[peer].Load
			lowestPeer = peer
		}
		f.plock.Unlock()
	}
	if lowest > 3000 {
		time.Sleep(3 * time.Second)
	}
	return lowestPeer, nil
}

func (f *NetClient) peers() []string {
	f.plock.Lock()
	peers := make([]string, 0)
	for peer, _ := range f.peerNodes {
		peers = append(peers, peer)
	}
	defer f.plock.Unlock()
	return peers
}

func (f *NetClient) openConn(peer string) (net.Conn, error) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to connect to %s", peer)
		return nil, err
	}
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	return conn, nil
}

func getReply[T netReply](f *NetClient, req *common.FsRequest, peer string) (*T, error) {
	nextLoad := new(int64)
	*nextLoad = 3000
	defer f.calcDelay(*nextLoad, peer)
	f.plock.Lock()
	peerInfo := f.peerNodes[peer]
	f.plock.Unlock()
	start := time.Now()
	err := peerInfo.CurrentRequests.Acquire(context.Background(), 1)
	defer peerInfo.CurrentRequests.Release(1)
	stop := time.Now()
	log.Debug().Msgf("Waited %d millis to dial conn", stop.Sub(start).Milliseconds())
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to set deadline")
		return nil, err
	}
	err = struc.Pack(conn, req)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var reply T
	switch any(reply).(type) {
	case common.DirInfo:
		err = any(&reply).(*common.DirInfo).Unmarshal(conn)
	default:
		err = struc.Unpack(conn, &reply)
	}
	if err != nil {
		return nil, err
	}
	*nextLoad = time.Since(start).Milliseconds()
	return &reply, nil
}

func (f *NetClient) FileInfo(path fsclient.RemotePath) (fs.FileInfo, error) {
	for _, peer := range f.peers() {
		info, err := f.fileInfo(path, peer)
		if err == nil {
			return info, nil
		}
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.file_info:1|c\n")
	return nil, errors.New("no peer has file" + string(path))
}

func (f *NetClient) fileInfo(path fsclient.RemotePath, peer string) (fs.FileInfo, error) {
	var info *common.NetInfo
	info, err := getReply[common.NetInfo](f, &common.FsRequest{
		Type:       byte(common.FILE_INFO),
		Offset:     0,
		Length:     0,
		PathLength: 0,
		Path:       string(path),
	}, peer)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (f *NetClient) calcDelay(nextLoad int64, peer string) {
	f.plock.Lock()
	info, ok := f.peerNodes[peer]
	f.plock.Unlock()
	if !ok {
		log.Debug().Msgf("Peer %s not found in peerNodes", peer)
	} else {
		info.Load = (nextLoad + info.Load*5) / 6
	}
	log.Trace().Msgf("Peer %s load is now %d", peer, info.Load)
}

func (f *NetClient) Read(path fsclient.RemotePath, offset int64, dest []byte) ([]byte, error) {
	peer, err := f.getPeer(path)
	if err != nil {
		return nil, err
	}
	log.Trace().Msgf("doing netread at %d for len %d", offset, len(dest))
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer")
		return nil, err
	}
	response, err := getReply[common.FileResponse](f, &common.FsRequest{
		Type:   byte(common.READ_CONTENT),
		Offset: offset,
		Length: int64(len(dest)),
		Path:   string(path),
	}, peer)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.read_content:1|c\n")
	log.Debug().Msgf("read %d bytes", len(response.Content))
	return response.Content, nil
}

func deduplicate(ls map[string][]fs.FileInfo) []fs.FileInfo {
	m := make(map[string]fs.FileInfo)
	for _, vs := range ls {
		for _, e := range vs {
			m[e.Name()] = e
		}
	}
	r := make([]fs.FileInfo, 0)
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func (f *NetClient) ReadDir(path fsclient.RemotePath) ([]fs.FileInfo, error) {
	netEntries := make(map[string][]fs.FileInfo)
	peers := f.peers()
	for _, peer := range peers {
		netEntryList, err := f.readDir1(path, peer)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to Read remote dir %s from %s", path, peer)
			continue
		}
		netEntries[peer] = netEntryList
	}
	for k, v := range netEntries {
		for _, e := range v {
			f.fPathRemoteCache.Add(path.Append(e.Name()), []string{k})
		}
	}
	return deduplicate(netEntries), nil
}

func (f *NetClient) readDir1(path fsclient.RemotePath, peer string) ([]fs.FileInfo, error) {
	response, err := getReply[common.DirInfo](f, &common.FsRequest{
		Type: byte(common.READDIR_INFO),
		Path: string(path),
	}, peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.readdir_content:1|c\n")
	infos := make([]fs.FileInfo, len(response.Infos))
	for i, info := range response.Infos {
		infos[i] = info
	}
	return infos, nil
}
