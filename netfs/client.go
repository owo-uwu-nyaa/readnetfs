package netfs

import (
	"errors"
	"github.com/rs/zerolog/log"
	"io/fs"
	"sync"
	"time"
)

var DEADLINE = 10 * time.Second

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

func (r RemotePath) String() string {
	return string(r)
}

type Client interface {
	Read(path RemotePath, offset int64, dest []byte) ([]byte, error)
	ReadDir(path RemotePath) ([]fs.FileInfo, error)
	FileInfo(path RemotePath) (fs.FileInfo, error)
}

type FileClient struct {
	clients  []Client
	iCounter uint64
	iMap     map[RemotePath]uint64
	iLock    sync.Mutex
}

// Order of clients is priority
func NewFileClient(clients ...Client) *FileClient {
	return &FileClient{clients: clients, iMap: make(map[RemotePath]uint64)}
}

func (f *FileClient) FileInfo(path RemotePath) (fs.FileInfo, error) {
	for _, client := range f.clients {
		info, err := client.FileInfo(path)
		if err != nil || info == nil {
			log.Debug().Err(err).Msgf("Failed to get fInfo from %s", path)
			continue
		}
		return info, nil
	}
	return nil, errors.New("Failed to get fInfo from any client")
}

func (f *FileClient) Read(path RemotePath, off int64, dest []byte) ([]byte, error) {
	for _, client := range f.clients {
		buf, err := client.Read(path, off, dest)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to read from %s", path)
			continue
		}
		return buf, nil
	}
	return nil, errors.New("Failed to read from any client")
}

func (f *FileClient) ReadDir(path RemotePath) ([]fs.FileInfo, error) {
	entries := make([]fs.FileInfo, 0)
	for _, client := range f.clients {
		newEntries, err := client.ReadDir(path)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to read dir from %s", path)
			continue
		}
		entries = append(entries, newEntries...)
	}
	return entries, nil
}

func (f *FileClient) PathToInode(path RemotePath) uint64 {
	f.iLock.Lock()
	defer f.iLock.Unlock()
	if inode, ok := f.iMap[path]; ok {
		return inode
	}
	f.iCounter++
	f.iMap[path] = f.iCounter
	return f.iCounter
}
