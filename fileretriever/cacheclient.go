package fileretriever

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"io/fs"
)

type cacheClient struct {
	fInfoCache *expirable.LRU[RemotePath, *netInfo]
	fDirCache  *expirable.LRU[RemotePath, *[]fuse.DirEntry]
	client     *Client
}

func newCacheClient(client *Client) *cacheClient {

	fInfoCache := expirable.NewLRU[RemotePath, *netInfo](PATH_CACHE_SIZE, func(key RemotePath, value *netInfo) {}, PATH_TTL)
	fDirCache := expirable.NewLRU[RemotePath, *[]fuse.DirEntry](PATH_CACHE_SIZE, func(key RemotePath, value *[]fuse.DirEntry) {}, PATH_TTL)
	return &cacheClient{client: client, fInfoCache: fInfoCache, fDirCache: fDirCache}
}

func (c cacheClient) Read(path RemotePath, offset int64, dest []byte) ([]byte, error) {

	panic("implement me")
}

func (c cacheClient) ReadDir(path RemotePath) ([]*fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (c cacheClient) FileInfo(path RemotePath) (*fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}
