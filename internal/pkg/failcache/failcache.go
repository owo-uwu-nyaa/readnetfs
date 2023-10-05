package failcache

//TODO base this and other path caches on inotify and not on polling

import (
	"github.com/hashicorp/golang-lru/v2/expirable"
	"io/fs"
	"readnetfs/internal/pkg/fsclient"
	"time"
)

var FAILED_TTL = 30 * time.Minute
var FAILED_NUM = 100

type FailCache struct {
	failedPaths *expirable.LRU[fsclient.RemotePath, interface{}]
	client      fsclient.Client
}

func NewFailCache(client fsclient.Client) *FailCache {
	fPaths := expirable.NewLRU[fsclient.RemotePath, interface{}](FAILED_NUM, func(key fsclient.RemotePath, empty interface{}) {}, FAILED_TTL)
	return &FailCache{failedPaths: fPaths, client: client}
}

func (f *FailCache) Purge() {
	f.failedPaths.Purge()
	f.client.Purge()
}

func (f *FailCache) Read(path fsclient.RemotePath, offset int64, dest []byte) ([]byte, error) {
	if _, ok := f.failedPaths.Get(path); ok {
		return nil, fs.ErrNotExist
	}
	buf, err := f.client.Read(path, offset, dest)
	if err != nil {
		f.failedPaths.Add(path, nil)
	}
	return buf, err
}

func (f *FailCache) ReadDir(path fsclient.RemotePath) ([]fs.FileInfo, error) {
	if _, ok := f.failedPaths.Get(path); ok {
		return nil, fs.ErrNotExist
	}
	finfos, err := f.client.ReadDir(path)
	if err != nil {
		f.failedPaths.Add(path, nil)
	}
	return finfos, err
}

func (f FailCache) FileInfo(path fsclient.RemotePath) (fs.FileInfo, error) {
	if _, ok := f.failedPaths.Get(path); ok {
		return nil, fs.ErrNotExist
	}
	finfo, err := f.client.FileInfo(path)
	if err != nil {
		f.failedPaths.Add(path, nil)
	}
	return finfo, err
}
