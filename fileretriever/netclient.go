package fileretriever

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"math"
	"net"
	"readnetfs/common"
	"strings"
	"time"
)

type netClient struct {
	statsdSocket net.Conn
}

type netReply interface {
	FileResponse | DirResponse | DirFInfo | netFileInfo
}

func (f *FileClient) getPeer(path RemotePath) (string, error) {
	candidates, ok := f.fPathRemoteCache.Get(path)
	if !ok {
		candidates = make([]string, 0)
		var thisFInfo *netFileInfo
		peers := f.peers()
		for _, peer := range peers {
			fInfo, err := f.netClient.netFileInfo(path, peer)
			if err == nil {
				if thisFInfo == nil {
					thisFInfo = fInfo
				} else {
					if fInfo.Size != thisFInfo.Size {
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
	//find candidate with lowest load
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
	conn, err := net.Dial("tcp", lowestPeer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return "", err
	}
	err = conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Msg("Failed to set deadline")
		return "", err
	}
	return lowestPeer, nil
}

func getReply[T netReply](f *netClient, req FsRequest, peer string) (*T, error) {
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

func (f *netClient) netFileInfo(path RemotePath, peer string) (*netFileInfo, error) {
	var nFileInfo *netFileInfo
	nFileInfo, err := getReply[netFileInfo](f, FsRequest{
		Type:       byte(FILE_INFO),
		Offset:     0,
		Length:     0,
		PathLength: 0,
		Path:       string(path),
	}, peer)
	if err != nil {
		return nil, err
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.file_info:1|c\n")
	return nFileInfo, nil
}

func (f *netClient) netRead(path RemotePath, offset int64, length int64) ([]byte, error) {
	nextLoad := new(int64)
	*nextLoad = 3000
	log.Trace().Msgf("doing net read at %d for len %d", offset, length)
	peer, err := f.getPeer(path)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer")
		return nil, err
	}
	f.plock.Lock()
	peerInfo := f.peerNodes[peer]
	f.plock.Unlock()
	start := time.Now()
	err = peerInfo.CurrentRequests.Acquire(context.Background(), 1)
	defer peerInfo.CurrentRequests.Release(1)
	stop := time.Now()
	log.Debug().Msgf("Waited %d millis to dial conn", stop.Sub(start).Milliseconds())
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
	conn.Write([]byte{READ_CONTENT})
	if err != nil {
		log.Debug().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FsRequest{
		Offset: offset,
		Length: length,
		Path:   string(path),
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.read_content:1|c\n")
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var response FileResponse
	start = time.Now()
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
			log.Debug().Err(err).Msgf("Failed to read remote dir %s from %s", path, peer)
			continue
		}
		netEntries[peer] = netEntryList
		//try to cache finfo
		dirFinfo, err := f.netFileInfoDir(path, peer)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to acquire associated file infos of path %s from %s", path, peer)
			continue
		}
		for _, fInfo := range dirFinfo.FInfos {
			fc := new(netFileInfo)
			*fc = fInfo
			f.fInfoCache.Add(path.Append(fInfo.Name), fc)
			log.Trace().Msg(fInfo.Name + " added to file cache")
		}
	}
	return netEntries, nil
}

func (f *netClient) netReadDir(path RemotePath, peer string) ([]fuse.DirEntry, error) {
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
	write, err := conn.Write([]byte{READDIR_CONTENT})
	if err != nil || write != 1 {
		log.Warn().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FsRequest{
		Offset: 0,
		Length: 0,
		Path:   string(path),
	}
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.outgoing.readdir_content:1|c\n")
	err = struc.Pack(conn, request)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to write request")
		return nil, err
	}
	var dirResponse DirResponse
	err = struc.Unpack(conn, &dirResponse)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to unpack response")
		return nil, err
	}
	dirs := strings.Split(string(dirResponse.Dirs), "\x00")
	if len(dirs) > 0 && dirs[0] == "" {
		dirs = []string{}
	}
	files := strings.Split(string(dirResponse.Files), "\x00")
	if len(files) > 0 && files[0] == "" {
		files = []string{}
	}
	r := make([]fuse.DirEntry, len(dirs)+len(files))
	for i, file := range files {
		r[i] = fuse.DirEntry{
			Mode: fuse.S_IFREG,
			Ino:  f.ThisFsToInode(path.Append(file)),
			Name: file,
		}
	}
	for i, dir := range dirs {
		r[i+len(files)] = fuse.DirEntry{
			Mode: fuse.S_IFDIR,
			Ino:  f.ThisFsToInode(path.Append(dir)),
			Name: dir,
		}
	}
	return r, nil
}
