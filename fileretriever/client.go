package fileretriever

import (
	"errors"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
	"io/fs"
	"net"
	"os"
	"readnetfs/cache"
	"readnetfs/common"
	"sync"
	"time"
)

var PATH_TTL = 15 * time.Minute
var DEADLINE = 10 * time.Second
var MAX_CONCURRENT_REQUESTS = 2
var PATH_CACHE_SIZE = 5000

type netFileInfo struct {
	NameLength int64 `struc:"int16,sizeof=Name"`
	Name       string
	Size       int64
	IsDir      bool
	ModTime    int64
}

type DirFInfo struct {
	FInfoLength int64 `struc:"int16,sizeof=FInfos"`
	FInfos      []netFileInfo
}

type LocalPath string

func (l LocalPath) Append(name string) LocalPath {
	return LocalPath(string(l) + "/" + name)
}

func (l LocalPath) String() string {
	return string(l)
}

type RemotePath string

func (r RemotePath) Append(name string) RemotePath {
	return RemotePath(string(r) + "/" + name)
}

type PeerInfo struct {
	Load            int64
	Rate            int64
	CurrentRequests *semaphore.Weighted
}

type client interface {
	read(path RemotePath, offset int64, dest []byte) ([]byte, error)
	readDir(path RemotePath) ([]*fs.FileInfo, error)
	fileInfo(path RemotePath) (*fs.FileInfo, error)
}

type FileClient struct {
	peerNodes        map[string]*PeerInfo
	plock            sync.Mutex
	iCounter         uint64
	iMap             map[RemotePath]uint64
	iLock            sync.Mutex
	fcache           *lru.Cache[RemotePath, *cache.CachedFile]
	fclock           sync.Mutex
	fPathRemoteCache *expirable.LRU[RemotePath, []string]
	fplock           sync.Mutex
	fInfoCache       *expirable.LRU[RemotePath, *netFileInfo]
	fDirCache        *expirable.LRU[RemotePath, *[]fuse.DirEntry]
	statsdSocket     net.Conn
	localClient      *localClient
	netClient        *netClient
}

func NewFileClient(localclient *localClient, peerNodes []string, statsdAddrPort string) *FileClient {
	fcache, _ := lru.New[RemotePath, *cache.CachedFile](cache.MEM_TOTAL_CACHE_B / cache.MEM_PER_FILE_CACHE_B)
	fPathRemoteCache := expirable.NewLRU[RemotePath, []string](cache.MEM_TOTAL_CACHE_B/cache.MEM_PER_FILE_CACHE_B,
		func(key RemotePath, value []string) {}, PATH_TTL)
	fInfoCache := expirable.NewLRU[RemotePath, *netFileInfo](PATH_CACHE_SIZE, func(key RemotePath, value *netFileInfo) {}, PATH_TTL)
	fDirCache := expirable.NewLRU[RemotePath, *[]fuse.DirEntry](PATH_CACHE_SIZE, func(key RemotePath, value *[]fuse.DirEntry) {}, PATH_TTL)
	pMap := make(map[string]*PeerInfo)
	for _, peer := range peerNodes {
		pMap[peer] = &PeerInfo{CurrentRequests: semaphore.NewWeighted(int64(MAX_CONCURRENT_REQUESTS))}
	}

	statsdSocket := func() net.Conn {
		if statsdAddrPort != "" {
			socket, err := net.Dial("udp", statsdAddrPort)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to establish statsd connection")
				return common.DummyConn{}
			}
			return socket
		} else {
			return common.DummyConn{}
		}
	}()

	return &FileClient{localClient: localclient, peerNodes: pMap, iMap: make(map[RemotePath]uint64), fcache: fcache,
		fPathRemoteCache: fPathRemoteCache, fInfoCache: fInfoCache, fDirCache: fDirCache, statsdSocket: statsdSocket}
}

func (f *FileClient) GetCachedFile(path RemotePath) *cache.CachedFile {
	f.fclock.Lock()
	defer f.fclock.Unlock()
	cf, ok := f.fcache.Get(path)
	if !ok {
		return nil
	}
	return cf
}

// PutOrGet tries to put a CachedFile, returns existing if already exists
func (f *FileClient) PutOrGet(rpath RemotePath, cf *cache.CachedFile) *cache.CachedFile {
	f.fclock.Lock()
	defer f.fclock.Unlock()
	if existing, ok := f.fcache.Get(rpath); ok {
		return existing
	}
	f.fcache.Add(rpath, cf)
	return cf
}

func (f *FileClient) peers() []string {
	f.plock.Lock()
	peers := make([]string, 0)
	for peer, _ := range f.peerNodes {
		peers = append(peers, peer)
	}
	defer f.plock.Unlock()
	return peers
}

func (f *FileClient) FileInfo(path RemotePath) (*netFileInfo, error) {
	if fInfo, ok := f.fInfoCache.Get(path); ok && fInfo != nil {
		return fInfo, nil
	}
	fInfo, err := f.fileInfo(path)
	if err != nil && fInfo != nil {
		return nil, err
	}
	f.fInfoCache.Add(path, fInfo)
	return fInfo, err
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
	write, err := conn.Write([]byte{READ_DIR_FINFO})
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
		log.Debug().Err(err).Msgf("Failed to read num of file infos for dir %s", request.Path)
	}
	var dirFInfo DirFInfo
	dirFInfo.FInfos = make([]netFileInfo, 0)
	for i := 0; i < int(nFinfo[0]); i++ {
		fInfo := new(netFileInfo)
		err = struc.Unpack(conn, fInfo)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to write file info for dir %s", request.Path)
			continue
		}
		dirFInfo.FInfos = append(dirFInfo.FInfos, *fInfo)
	}
	return &dirFInfo, nil
}

func (f *FileClient) fileInfo(path RemotePath) (*os.FileInfo, error) {
	if f.localClient != nil {
		fInfo, err := f.localClient.fileInfo(path)
		if err == nil {
			return fInfo, nil
		}
	}
	for _, peer := range f.peers() {
		fInfo, err := f.netClient.netFileInfo(path, peer)
		if err == nil {
			return fInfo, nil
		}
	}
	return nil, errors.New("Failed to find finfo" + string(path) + "on any peer")
}

func (f *FileClient) Read(path RemotePath, off, length int64) ([]byte, error) {
	log.Trace().Msgf("doing read at %d for len %d", off, length)
	buf, err := f.localRead(path, off, length)
	if err != nil {
		log.Debug().Msgf("Reading from net %s", path)
		for {
			buf, err = f.netRead(path, off, length)
			if err != nil {
				log.Debug().Err(err).Msg("Failed to read from net")
				continue
			}
			break
		}
		return buf, nil
	}
	return buf, nil
}

func (f *FileClient) ReadDir(path RemotePath) ([]fuse.DirEntry, error) {
	if fInfo, ok := f.fDirCache.Get(path); ok {
		return *fInfo, nil
	}
	fDirs, err := f.readDir(path)
	if err != nil {
		return nil, err
	}
	f.fDirCache.Add(path, &fDirs)
	return fDirs, err
}

func (f *FileClient) readDir(path RemotePath) ([]fuse.DirEntry, error) {
	localEntries, err := f.localReadDir(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read local dir %s", path)
	}
	netEntries, err := f.netReadDirAllPeers(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read remote dir %s", path)
	}
	//get vals from netEntries
	netList := make([][]fuse.DirEntry, 0)
	for _, v := range netEntries {
		netList = append(netList, v)
	}
	entries := deduplicate(append(netList, localEntries))
	return entries, nil
}

func deduplicate(ls [][]fuse.DirEntry) []fuse.DirEntry {
	m := make(map[string]fuse.DirEntry)
	for _, l := range ls {
		for _, e := range l {
			m[e.Name] = e
		}
	}
	r := make([]fuse.DirEntry, 0)
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func (f *FileClient) ThisFsToInode(path RemotePath) uint64 {
	f.iLock.Lock()
	defer f.iLock.Unlock()
	if val, ok := f.iMap[path]; ok {
		return val
	}
	f.iCounter++
	f.iMap[path] = f.iCounter
	return f.iCounter
}
