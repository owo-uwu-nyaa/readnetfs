package fsclient

import (
	"errors"
	"github.com/rs/zerolog/log"
	"io/fs"
	"sync"
)

var MAX_RETRIES = 3

type Client interface {
	Read(path RemotePath, offset int64, dest []byte) ([]byte, error)
	ReadDir(path RemotePath) ([]fs.FileInfo, error)
	FileInfo(path RemotePath) (fs.FileInfo, error)
	Purge()
}

type FileClient struct {
	clients  []Client
	iCounter uint64
	iMap     map[RemotePath]uint64
	iLock    sync.Mutex
}

// NewFileClient with argument clients: order of clients is priority
func NewFileClient(clients ...Client) *FileClient {
	return &FileClient{clients: clients, iMap: make(map[RemotePath]uint64)}
}
func (f *FileClient) Purge() {
	for _, client := range f.clients {
		client.Purge()
	}
}

func (f *FileClient) FileInfo(path RemotePath) (fs.FileInfo, error) {
	for i := 0; i < MAX_RETRIES; i++ {
		if i == MAX_RETRIES-1 {
			log.Warn().Msgf("Something bad happened, purging caches")
			f.Purge()
		}
		for _, client := range f.clients {
			info, err := client.FileInfo(path)
			if err != nil || info == nil {
				log.Debug().Err(err).Msgf("failed to get fInfo from %s", path)
				continue
			}
			return info, nil
		}
	}
	return nil, errors.New("failed to get fInfo from any client")
}

func (f *FileClient) Read(path RemotePath, off int64, dest []byte) ([]byte, error) {
	for i := 0; i < MAX_RETRIES; i++ {
		if i == MAX_RETRIES-1 {
			log.Warn().Msgf("Something bad happened, purging caches")
			f.Purge()
		}
		for _, client := range f.clients {
			buf, err := client.Read(path, off, dest)
			if err != nil {
				log.Debug().Err(err).Msgf("failed to read from %s", path)
				continue
			}
			return buf, nil
		}
	}
	return nil, errors.New("failed to read from any client")
}

func (f *FileClient) ReadDir(path RemotePath) ([]fs.FileInfo, error) {
	entries := make([]fs.FileInfo, 0)
	for _, client := range f.clients {
		var newEntries []fs.FileInfo
		var err error
		for i := 0; i < MAX_RETRIES; i++ {
			if i == MAX_RETRIES-1 {
				log.Warn().Msgf("Something bad happened, purging caches")
				f.Purge()
			}
			newEntries, err = client.ReadDir(path)
			if err != nil || newEntries == nil {
				log.Debug().Err(err).Msg("failed to read dir")
				continue
			}
			break
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

type RemotePath string

func (r RemotePath) Append(name string) RemotePath {
	return RemotePath(string(r) + "/" + name)
}

func (r RemotePath) String() string {
	return string(r)
}
