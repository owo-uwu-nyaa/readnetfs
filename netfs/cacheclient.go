package cache

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog/log"
	"io/fs"
	"readnetfs/fileretriever"
	"syscall"
)

type cacheClient struct {
	fInfoCache *expirable.LRU[fileretriever.RemotePath, *fileretriever.netInfo]
	fDirCache  *expirable.LRU[fileretriever.RemotePath, *[]fuse.DirEntry]
	fcache     *lru.Cache[RemotePath, *CachedFile]
	client     *fileretriever.Client
}

func newCacheClient(client *fileretriever.Client) *cacheClient {

	fInfoCache := expirable.NewLRU[fileretriever.RemotePath, *fileretriever.netInfo](fileretriever.PATH_CACHE_SIZE, func(key fileretriever.RemotePath, value *fileretriever.netInfo) {}, fileretriever.PATH_TTL)
	fDirCache := expirable.NewLRU[fileretriever.RemotePath, *[]fuse.DirEntry](fileretriever.PATH_CACHE_SIZE, func(key fileretriever.RemotePath, value *[]fuse.DirEntry) {}, fileretriever.PATH_TTL)
	return &cacheClient{client: client, fInfoCache: fInfoCache, fDirCache: fDirCache}
}

func (c *cacheClient) Read(path fileretriever.RemotePath, offset int64, dest []byte) ([]byte, error) {
	cacheEntry := c.GetCachedFile(n.path)
	if cacheEntry != nil {
		buf, err := cacheEntry.Read(off, dest)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to read %s", n.path)
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(buf), 0
	}
	fInfo, err := n.fc.FileInfo(n.path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", n.path)
		return nil, syscall.EIO
	}
	cf := cache.NewCachedFile(fInfo.Size(), func(offset, length int64) ([]byte, error) {
		return n.fc.Read(n.path, offset, length)
	})
	cf = n.fc.PutOrGet(n.path, cf)
	buf, err := cf.Read(off, dest)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", n.path)
		return nil, syscall.EIO
	}
}

func (c cacheClient) ReadDir(path fileretriever.RemotePath) ([]*fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (c cacheClient) FileInfo(path fileretriever.RemotePath) (*fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}
