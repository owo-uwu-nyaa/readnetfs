package netfs

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog/log"
	"io/fs"
	"readnetfs/cache"
	"sync"
	"syscall"
	"time"
)

var MAX_CONCURRENT_REQUESTS = 2
var PATH_CACHE_SIZE = 5000
var PATH_TTL = 1 * time.Second

// CacheClient use mutexes to make sure only one request is sent at a time
type CacheClient struct {
	infos          *expirable.LRU[RemotePath, fs.FileInfo]
	infoLock       sync.Mutex
	dirContent     *expirable.LRU[RemotePath, []string]
	dirContentLock sync.Mutex
	fCache         *lru.Cache[RemotePath, *cache.CachedFile]
	fCacheLock     sync.Mutex
	client         Client
}

func NewCacheClient(client Client) *CacheClient {
	dirContent := expirable.NewLRU[RemotePath](PATH_CACHE_SIZE,
		func(key RemotePath, value []string) {}, PATH_TTL)
	infos := expirable.NewLRU[RemotePath, fs.FileInfo](PATH_CACHE_SIZE,
		func(key RemotePath, info fs.FileInfo) {}, PATH_TTL)
	fCache, _ := lru.New[RemotePath, *cache.CachedFile](PATH_CACHE_SIZE)
	return &CacheClient{client: client, dirContent: dirContent, infos: infos, fCache: fCache}
}

func (c *CacheClient) PutOrGet(rpath RemotePath, cf *cache.CachedFile) *cache.CachedFile {
	c.fCacheLock.Lock()
	defer c.fCacheLock.Unlock()
	if existing, ok := c.fCache.Get(rpath); ok {
		return existing
	}
	c.fCache.Add(rpath, cf)
	return cf
}

func (c *CacheClient) Read(path RemotePath, off int64, dest []byte) ([]byte, error) {
	cacheEntry, ok := c.fCache.Get(path)
	if ok {
		dest, err := cacheEntry.Read(off, dest)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to read %s", path)
			return nil, syscall.EIO
		}
		return dest, nil
	}
	info, err := c.client.FileInfo(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", path)
		return nil, syscall.EIO
	}
	cf := cache.NewCachedFile(info.Size(), func(offset, length int64) ([]byte, error) {
		return c.client.Read(path, offset, dest)
	})
	cf = c.PutOrGet(path, cf)
	buf, err := cf.Read(off, dest)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", path)
		return nil, syscall.EIO
	}
	return buf, nil
}

func (c *CacheClient) ReadDir(path RemotePath) ([]fs.FileInfo, error) {
	if files, ok := c.dirContent.Get(path); ok {
		infos := make([]fs.FileInfo, len(files))
		for _, file := range files {
			info, err := c.FileInfo(path.Append(file))
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to read file info for %s", path)
				return nil, syscall.EIO
			}
			infos = append(infos, info)
		}
		return infos, nil
	}
	c.dirContentLock.Lock()
	defer c.dirContentLock.Unlock()
	infos, err := c.client.ReadDir(path)
	if err != nil {
		return nil, err
	}
	files := make([]string, len(infos))
	for i, info := range infos {
		files[i] = info.Name()
		c.infos.Add(path.Append(info.Name()), info)
	}
	c.dirContent.Add(path, files)
	return infos, nil
}

func (c *CacheClient) FileInfo(path RemotePath) (fs.FileInfo, error) {
	if info, ok := c.infos.Get(path); ok {
		return info, nil
	}
	c.infoLock.Lock()
	defer c.infoLock.Unlock()
	info, err := c.client.FileInfo(path)
	if err != nil {
		return nil, err
	}
	c.infos.Add(path, info)
	return info, nil
}
