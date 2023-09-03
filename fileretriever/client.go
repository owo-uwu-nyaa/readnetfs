package fileretriever

import (
	"errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/golang-lru/v2"
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

type DirFInfo struct {
	FInfoLength int64 `struc:"int16,sizeof=FInfos"`
	FInfos      []netInfo
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

type Client interface {
	Read(path RemotePath, offset int64, dest []byte) ([]byte, error)
	ReadDir(path RemotePath) ([]*fs.FileInfo, error)
	FileInfo(path RemotePath) (*fs.FileInfo, error)
}

type FileClient struct {
	plock        sync.Mutex
	iCounter     uint64
	iMap         map[RemotePath]uint64
	iLock        sync.Mutex
	statsdSocket net.Conn
	localClient  *localClient
	netClient    *netClient
	fcache       *lru.Cache[RemotePath, *cache.CachedFile]
}

func NewFileClient(localclient *localClient, peerNodes []string, statsdAddrPort string) *FileClient {
	fcache, _ := lru.New[RemotePath, *cache.CachedFile](cache.MEM_TOTAL_CACHE_B / cache.MEM_PER_FILE_CACHE_B)

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

func (f *FileClient) FileInfo(path RemotePath) (*netInfo, error) {
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
	log.Trace().Msgf("doing Read at %d for len %d", off, length)
	buf, err := f.localRead(path, off, length)
	if err != nil {
		log.Debug().Msgf("Reading from net %s", path)
		for {
			buf, err = f.netRead(path, off, length)
			if err != nil {
				log.Debug().Err(err).Msg("Failed to Read from net")
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
		log.Debug().Err(err).Msgf("Failed to Read local dir %s", path)
	}
	netEntries, err := f.netReadDirAllPeers(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to Read remote dir %s", path)
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
