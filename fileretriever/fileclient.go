package fileretriever

import (
	"errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"math/rand"
	"net"
	"os"
	"readnetfs/common"
	"strings"
	"sync"
)

type FileClient struct {
	srcDir    string
	peerNodes []string
	iCounter  uint64
	iMap      map[string]uint64
	iLock     sync.Mutex
}

func (f *FileClient) SrcDir() string {
	return f.srcDir
}

func NewFileClient(srcDir string, peerNodes []string) *FileClient {
	return &FileClient{srcDir: srcDir, peerNodes: peerNodes, iMap: make(map[string]uint64)}
}

func (f *FileClient) getPeerConn(path string) (net.Conn, error) {
	return net.Dial("tcp", f.peerNodes[rand.Intn(len(f.peerNodes))])
}

func (f *FileClient) FileInfo(path string) (*common.Finfo, error) {
	fInfo, err := f.localFileInfo(path)
	if err != nil {
		return f.netFileInfo(path)
	}
	return fInfo, nil
}

func (f *FileClient) netFileInfo(path string) (*common.Finfo, error) {
	conn, err := f.getPeerConn(path)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer conn")
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
		Path:   path,
	}
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var fInfo common.Finfo
	struc.Unpack(conn, &fInfo)
	return &fInfo, nil
}

func (f *FileClient) localFileInfo(path string) (*common.Finfo, error) {
	file, err := os.Open(f.srcDir + "/" + path)
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

func (f *FileClient) netRead(path string, offset int, length int) ([]byte, error) {
	log.Trace().Msgf("doing net read at %d for len %d", offset, length)
	conn, err := f.getPeerConn(path)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to get peer conn")
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
		Path:   path,
	}
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, err
	}
	var response FileResponse
	struc.Unpack(conn, &response)
	return response.Content, nil
}

func (f *FileClient) localRead(localPath string, off, length int) ([]byte, error) {
	file, err := os.Open(localPath)
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

func (f *FileClient) Read(path string, off, length int) ([]byte, error) {
	log.Trace().Msgf("doing read at %d for len %d", off, length)
	buf, err := f.localRead(f.srcDir+"/"+path, off, length)
	if err != nil {
		wg := sync.WaitGroup{}
		wg.Add(1)
		var p1 []byte
		var e1 error
		log.Debug().Msgf("Reading from net %s", path)
		go func() {
			p1, e1 = f.netRead(path, off, length)
			wg.Done()
		}()
		wg.Wait()
		if e1 != nil {
			log.Debug().Err(e1).Msg("Failed to read from net")
			return nil, errors.New("failed to read from net")
		}
		return p1, nil
	}
	return buf, nil
}

func deduplicateList(l1, l2 []fuse.DirEntry) []fuse.DirEntry {
	m := make(map[string]fuse.DirEntry)
	for _, e := range l1 {
		m[e.Name] = e
	}
	for _, e := range l2 {
		m[e.Name] = e
	}
	r := make([]fuse.DirEntry, 0)
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func (f *FileClient) ReadDir(path string) ([]fuse.DirEntry, error) {
	localEntries, err := f.localReadDir(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read local dir %s", path)
	}
	netEntries, err := f.netReadDir(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read remote dir %s", path)
	}
	entries := deduplicateList(localEntries, netEntries)
	return entries, nil
}

func (f *FileClient) localReadDir(path string) ([]fuse.DirEntry, error) {
	log.Trace().Msgf("doing read dir at %s", path)
	dir, err := os.ReadDir(f.srcDir + "/" + path)
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

func (f *FileClient) thisFsFuseDirEntry(path, name string) fuse.DirEntry {
	return fuse.DirEntry{
		Mode: f.localToMode(f.srcDir + path + "/" + name),
		Ino:  f.ThisFsToInode(path + "/" + name),
		Name: name,
	}
}

func (f *FileClient) ThisFsToInode(path string) uint64 {
	f.iLock.Lock()
	defer f.iLock.Unlock()
	if val, ok := f.iMap[path]; ok {
		return val
	}
	f.iCounter++
	f.iMap[path] = f.iCounter
	return f.iCounter
}

func (f *FileClient) netReadDir(path string) ([]fuse.DirEntry, error) {
	conn, err := f.getPeerConn(path)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get peer conn")
		return nil, err
	}
	defer conn.Close()
	write, err := conn.Write([]byte{READDIR_CONTENT})
	if err != nil || write != 1 {
		log.Warn().Err(err).Msg("Failed to write message type")
		return nil, err
	}
	request := &FileRequest{
		Offset: 0,
		Length: 0,
		Path:   path,
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
			Ino:  f.ThisFsToInode(path + "/" + file),
			Name: file,
		}
	}
	for i, dir := range dirs {
		r[i+len(files)] = fuse.DirEntry{
			Mode: fuse.S_IFDIR,
			Ino:  f.ThisFsToInode(path + "/" + dir),
			Name: dir,
		}
	}
	return r, nil
}
