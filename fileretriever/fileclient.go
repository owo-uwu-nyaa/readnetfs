package fileretriever

import (
	"errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"math"
	"net"
	"os"
	"readnetfs/cache"
	"readnetfs/common"
	"strings"
	"sync"
	"time"
)

var PATH_TTL = 60 * time.Second
var DEADLINE = 1 * time.Second

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
	Load int64
	Rate int
}

type FileClient struct {
	srcDir           string
	peerNodes        map[string]*PeerInfo
	iCounter         uint64
	iMap             map[RemotePath]uint64
	iLock            sync.Mutex
	fcache           *lru.Cache[RemotePath, *cache.CachedFile]
	fclock           sync.Mutex
	fPathRemoteCache *expirable.LRU[RemotePath, []string]
	fplock           sync.Mutex
}

func NewFileClient(srcDir string, peerNodes []string) *FileClient {
	fcache, _ := lru.New[RemotePath, *cache.CachedFile](cache.MEM_TOTAL_CACHE_B / cache.MEM_PER_FILE_CACHE_B)
	fPathRemoteCache := expirable.NewLRU[RemotePath, []string](cache.MEM_TOTAL_CACHE_B/cache.MEM_PER_FILE_CACHE_B,
		func(key RemotePath, value []string) {}, PATH_TTL)
	pMap := make(map[string]*PeerInfo)
	for _, peer := range peerNodes {
		pMap[peer] = &PeerInfo{}
	}
	return &FileClient{srcDir: srcDir, peerNodes: pMap, iMap: make(map[RemotePath]uint64), fcache: fcache,
		fPathRemoteCache: fPathRemoteCache}
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

func (f *FileClient) Re2Lo(remote RemotePath) LocalPath {
	return LocalPath(f.srcDir + "/" + string(remote))
}

func (f *FileClient) getPeerConn(path RemotePath) (net.Conn, string, error) {
	candidates, ok := f.fPathRemoteCache.Get(path)
	if !ok {
		candidates = make([]string, 0)
		var thisFInfo *common.Finfo
		for peer, _ := range f.peerNodes {
			fInfo, err := f.netFileInfo(path, peer)
			if err == nil {
				if thisFInfo == nil {
					thisFInfo = fInfo
				} else {
					if fInfo.Size != thisFInfo.Size {
						return nil, "", errors.New("file has different sizes on different peers" + string(path))
					}
				}
				candidates = append(candidates, peer)
			}
			f.fPathRemoteCache.Add(path, candidates)
		}
	}
	if len(candidates) == 0 {
		return nil, "", errors.New("no peer candidates for file" + string(path))
	}
	//find candidate with lowest load
	lowest := int64(math.MaxInt64)
	lowestPeer := ""
	for _, peer := range candidates {
		if f.peerNodes[peer].Load < lowest {
			lowest = f.peerNodes[peer].Load
			lowestPeer = peer
		}
	}
	if lowest > 3000 {
		time.Sleep(3 * time.Second)
	}
	conn, err := net.Dial("tcp", lowestPeer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, "", err
	}
	err = conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Msg("Failed to set deadline")
		return nil, "", err
	}
	return conn, lowestPeer, nil
}

func (f *FileClient) FileInfo(path RemotePath) (*common.Finfo, error) {
	fInfo, err := f.localFileInfo(path)
	if err == nil {
		return fInfo, nil
	}
	for peer, _ := range f.peerNodes {
		fInfo, err := f.netFileInfo(path, peer)
		if err == nil {
			return fInfo, nil
		}
	}
	return nil, errors.New("Failed to find finfo" + string(path) + "on any peer")
}

func (f *FileClient) netFileInfo(path RemotePath, peer string) (*common.Finfo, error) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	err = conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to set deadline")
		return nil, err
	}
	defer conn.Close()
	write, err := conn.Write([]byte{FILE_INFO})
	if err != nil || write != 1 {
		log.Debug().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FileRequest{
		Offset: 0,
		Length: 0,
		Path:   string(path),
	}
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var fInfo common.Finfo
	err = struc.Unpack(conn, &fInfo)
	if err != nil {
		return nil, err
	}
	return &fInfo, nil
}

func (f *FileClient) localFileInfo(path RemotePath) (*common.Finfo, error) {
	file, err := os.Open(f.Re2Lo(path).String())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &common.Finfo{
		Name:    fInfo.Name(),
		Size:    fInfo.Size(),
		IsDir:   fInfo.IsDir(),
		ModTime: fInfo.ModTime().Unix(),
	}, nil
}

func (f *FileClient) netRead(path RemotePath, offset int, length int) ([]byte, error) {
	nextLoad := new(int64)
	*nextLoad = 3000
	log.Trace().Msgf("doing net read at %d for len %d", offset, length)
	conn, peer, err := f.getPeerConn(path)
	defer func() {
		info, ok := f.peerNodes[peer]
		if !ok {
			log.Debug().Msgf("Peer %s not found in peerNodes", peer)
		} else {
			info.Load = (*nextLoad + info.Load*5) / 5
		}
		log.Trace().Msgf("Peer %s load is now %d", peer, info.Load)
	}()
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.Write([]byte{READ_CONTENT})
	if err != nil {
		log.Debug().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FileRequest{
		Offset: offset,
		Length: length,
		Path:   string(path),
	}
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var response FileResponse
	//time read
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

func (f *FileClient) localRead(remotePath RemotePath, off, length int) ([]byte, error) {
	localPath := f.Re2Lo(remotePath)
	file, err := os.Open(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to open file %s", localPath)
		return nil, err
	}
	finfo, err := file.Stat()
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to stat file %s", localPath)
		return nil, err
	}
	if off >= int(finfo.Size()) {
		return []byte{}, nil
	}
	seek, err := file.Seek(int64(off), 0)
	if err != nil || seek != int64(off) {
		log.Warn().Err(err).Msgf("Failed to seek to %d in file %s", off, localPath)
		return nil, err
	}
	buf := make([]byte, length)
	read, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:read], nil
}

func (f *FileClient) Read(path RemotePath, off, length int) ([]byte, error) {
	log.Trace().Msgf("doing read at %d for len %d", off, length)
	buf, err := f.localRead(path, off, length)
	if err != nil {
		log.Debug().Msgf("Reading from net %s", path)
		buf, err := f.netRead(path, off, length)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to read from net")
			return nil, errors.New("failed to read from net")
		}
		return buf, nil
	}
	return buf, nil
}

func (f *FileClient) ReadDir(path RemotePath) ([]fuse.DirEntry, error) {
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
	entries := deduplicateLists(append(netList, localEntries))
	return entries, nil
}

func deduplicateLists(ls [][]fuse.DirEntry) []fuse.DirEntry {
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

func (f *FileClient) netReadDirAllPeers(path RemotePath) (map[string][]fuse.DirEntry, error) {
	netEntries := make(map[string][]fuse.DirEntry)
	for peer, _ := range f.peerNodes {
		netEntryList, err := f.netReadDir(path, peer)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to read remote dir %s from %s", path, peer)
			continue
		}
		netEntries[peer] = netEntryList
	}
	return netEntries, nil
}

func (f *FileClient) localReadDir(path RemotePath) ([]fuse.DirEntry, error) {
	localPath := f.Re2Lo(path)
	log.Trace().Msgf("doing read dir at %s", path)
	dir, err := os.ReadDir(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read dir %s", path)
		return nil, err
	}
	r := make([]fuse.DirEntry, len(dir))
	for i, file := range dir {
		r[i] = f.thisFsFuseDirEntry(path, file.Name())
	}
	return r, nil
}

func (f *FileClient) localToMode(localpath string) uint32 {
	file, err := os.Open(localpath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fInfo, _ := file.Stat()
	if fInfo.IsDir() {
		return fuse.S_IFDIR
	}
	return fuse.S_IFREG
}

func (f *FileClient) thisFsFuseDirEntry(path RemotePath, name string) fuse.DirEntry {
	return fuse.DirEntry{
		Mode: f.localToMode(f.Re2Lo(path).Append(name).String()),
		Ino:  f.ThisFsToInode(path.Append(name)),
		Name: name,
	}
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

func (f *FileClient) netReadDir(path RemotePath, peer string) ([]fuse.DirEntry, error) {
	conn, err := net.Dial("tcp", peer)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(DEADLINE * time.Second))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to set deadline")
		return nil, err
	}
	write, err := conn.Write([]byte{READDIR_CONTENT})
	if err != nil || write != 1 {
		log.Warn().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FileRequest{
		Offset: 0,
		Length: 0,
		Path:   string(path),
	}
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
