package fileretriever

import (
	"context"
	"errors"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"math"
	"net"
	"readnetfs/cache"
	"readnetfs/common"
	"strings"
	"sync"
	"time"
)

type netClient struct {
	statsdSocket     net.Conn
	peerNodes        map[string]*PeerInfo
	plock            sync.Mutex
	fPathRemoteCache *expirable.LRU[RemotePath, []string]
	client           *Client
}

func newNetClient(statsdSocket net.Conn, peerNodes []string, client *Client) *netClient {
	fPathRemoteCache := expirable.NewLRU[RemotePath, []string](cache.MEM_TOTAL_CACHE_B/cache.MEM_PER_FILE_CACHE_B,
		func(key RemotePath, value []string) {}, PATH_TTL)
	pMap := make(map[string]*PeerInfo)
	for _, peer := range peerNodes {
		pMap[peer] = &PeerInfo{CurrentRequests: semaphore.NewWeighted(int64(MAX_CONCURRENT_REQUESTS))}
	}
	return &netClient{statsdSocket: statsdSocket, peerNodes: pMap, fPathRemoteCache: fPathRemoteCache, client: client}
}

type netReply interface {
	FileResponse | DirResponse | DirFInfo | netInfo
}

func (f *netClient) getPeer(path RemotePath) (string, error) {
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

func (f *netClient) peers() []string {
	f.plock.Lock()
	peers := make([]string, 0)
	for peer, _ := range f.peerNodes {
		peers = append(peers, peer)
	}
	defer f.plock.Unlock()
	return peers
}

func (f *FileClient) openConn(peer string) (net.Conn, error) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to connect to %s", peer)
		return nil, err
	}
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	return conn, nil
}

func getReply[T netReply](f *netClient, req FsRequest, peer string) (*T, error) {
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
	err = struc.Unpack(conn, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (f *netClient) FileInfo(path RemotePath) (fs.FileInfo, error) {
	peer, err := f.getPeer(path)
	if err != nil {
		return nil, err
	}
	info, err := f.fileInfo(path, peer)
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.file_info:1|c\n")
	return info, nil
}

func (f *netClient) fileInfo(path RemotePath, peer string) (fs.FileInfo, error) {
	var info *netInfo
	info, err := getReply[netInfo](f, FsRequest{
		Type:       byte(FILE_INFO),
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

func (f *netClient) Read(path RemotePath, offset int64, dest []byte) ([]byte, error) {
	nextLoad := new(int64)
	*nextLoad = 3000
	log.Trace().Msgf("doing net Read at %d for len %d", offset, len(dest))
	peer, err := f.getPeer(path)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer")
		return nil, err
	}

	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to acquire semaphore")
		return nil, err
	}
	defer func() {
		f.plock.Lock()
		info, ok := f.peerNodes[peer]
		f.plock.Unlock()
		if !ok {
			log.Debug().Msgf("Peer %s not found in peerNodes", peer)
		} else {
			info.Load = (*nextLoad + info.Load*5) / 6
		}
		log.Trace().Msgf("Peer %s load is now %d", peer, info.Load)
	}()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.Write([]byte{byte(READ_CONTENT)})
	if err != nil {
		log.Debug().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FsRequest{
		Offset: offset,
		Length: int64(len(dest)),
		Path:   string(path),
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.read_content:1|c\n")
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var response FileResponse
	start := time.Now()
	err = struc.Unpack(conn, &response)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	log.Debug().Msgf("Read %d bytes from %s in %s", len(response.Content), peer, elapsed)
	*nextLoad = elapsed.Milliseconds()
	return response.Content, nil
}

func (f *netClient) netReadDirAllPeers(path RemotePath) (map[string][]fuse.DirEntry, error) {
	netEntries := make(map[string][]fuse.DirEntry)
	peers := f.peers()
	for _, peer := range peers {
		netEntryList, err := f.netReadDir(path, peer)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to Read remote dir %s from %s", path, peer)
			continue
		}
		netEntries[peer] = netEntryList
		//try to cache finfo
		dirFinfo, err := f.net(path, peer)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to acquire associated file infos of path %s from %s", path, peer)
			continue
		}
		for _, fInfo := range dirFinfo.FInfos {
			fc := new(netInfo)
			*fc = fInfo
			f.fInfoCache.Add(path.Append(fInfo.Name), fc)
			log.Trace().Msg(fInfo.Name + " added to file cache")
		}
	}
	return netEntries, nil
}

func (f *netClient) netReadDir(path RemotePath, peer string) ([]fs.FileInfo, error) {
	response, err := getReply[DirResponse](f, FsRequest{
		Type: byte(READDIR_CONTENT),
		Path: string(path),
	}, peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	//TODO consistent stats location
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.readdir_content:1|c\n")
	dirs := strings.Split(string(response.Dirs), "\x00")
	if len(dirs) > 0 && dirs[0] == "" {
		dirs = []string{}
	}
	files := strings.Split(string(response.Files), "\x00")
	if len(files) > 0 && files[0] == "" {
		files = []string{}
	}
	return , nil
}

func (f *FileClient) netFileInfoDir(path RemotePath, peer string) (*DirFInfo, error) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	err = conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to set deadline")
		return nil, err
	}
	defer conn.Close()
	write, err := conn.Write([]byte{byte(READ_DIR_FINFO)})
	if err != nil || write != 1 {
		log.Debug().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FsRequest{
		Offset: 0,
		Length: 0,
		Path:   string(path),
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.read_dir_finfo:1|c\n")
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	nFinfo := make([]byte, 1)
	read, err := conn.Read(nFinfo)
	if err != nil || read != 1 {
		log.Debug().Err(err).Msgf("Failed to Read num of file infos for dir %s", request.Path)
	}
	var dirFInfo DirFInfo
	dirFInfo.FInfos = make([]netInfo, 0)
	for i := 0; i < int(nFinfo[0]); i++ {
		fInfo := new(netInfo)
		err = struc.Unpack(conn, fInfo)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to write file info for dir %s", request.Path)
			continue
		}
		dirFInfo.FInfos = append(dirFInfo.FInfos, *fInfo)
	}
	return &dirFInfo, nil
}
