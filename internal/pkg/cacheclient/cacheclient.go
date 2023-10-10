package cacheclient

import (
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog/log"
	"io/fs"
	"readnetfs/internal/pkg/fsclient"
	"sync"
	"syscall"
	"time"
)

var MAX_CONCURRENT_REQUESTS = 2
var PATH_CACHE_SIZE = 5000
var PATH_TTL = 15 * time.Minute

// CacheClient use mutexes to make sure only one request is sent at a time
type CacheClient struct {
	infos          *expirable.LRU[fsclient.RemotePath, fs.FileInfo]
	failed         *expirable.LRU[fsclient.RemotePath, error]
	infoLock       sync.Mutex
	dirContent     *expirable.LRU[fsclient.RemotePath, []string]
	dirContentLock sync.Mutex
	fCache         *lru.Cache[fsclient.RemotePath, *CachedFile]
	fCacheLock     sync.Mutex
	client         fsclient.Client
}

func NewCacheClient(client fsclient.Client) *CacheClient {
	dirContent := expirable.NewLRU[fsclient.RemotePath, []string](PATH_CACHE_SIZE,
		func(key fsclient.RemotePath, value []string) {}, PATH_TTL)
	infos := expirable.NewLRU[fsclient.RemotePath, fs.FileInfo](PATH_CACHE_SIZE,
		func(key fsclient.RemotePath, info fs.FileInfo) {}, PATH_TTL)
	failedPaths := expirable.NewLRU[fsclient.RemotePath, error](PATH_CACHE_SIZE, func(key fsclient.RemotePath, value error) {}, PATH_TTL)
	fCache, _ := lru.New[fsclient.RemotePath, *CachedFile](MEM_TOTAL_CACHE_B / MEM_PER_FILE_CACHE_B)
	return &CacheClient{client: client, dirContent: dirContent, infos: infos, fCache: fCache, failed: failedPaths}
}

func (c *CacheClient) Purge() {
	c.dirContent.Purge()
	c.infos.Purge()
	c.fCache.Purge()
	c.client.Purge()
}

func (c *CacheClient) PutOrGet(rpath fsclient.RemotePath, cf *CachedFile) *CachedFile {
	c.fCacheLock.Lock()
	defer c.fCacheLock.Unlock()
	if existing, ok := c.fCache.Get(rpath); ok {
		return existing
	}
	c.fCache.Add(rpath, cf)
	return cf
}

func (c *CacheClient) Read(path fsclient.RemotePath, off int64, dest []byte) ([]byte, error) {
	if reason, ok := c.failed.Get(path); ok {
		return nil, reason
	}
	cacheEntry, ok := c.fCache.Get(path)
	if ok {
		dest, err := cacheEntry.Read(off, dest)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to read %s", path)
			return nil, syscall.EIO
		}
		return dest, nil
	}
	cf, err := NewCachedFile(path, c.client)
	if err != nil {
		return nil, err
	}
	cf = c.PutOrGet(path, cf)
	buf, err := cf.Read(off, dest)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", path)
		return nil, syscall.EIO
	}
	return buf, nil
}

func (c *CacheClient) ReadDir(path fsclient.RemotePath) ([]fs.FileInfo, error) {
	if reason, ok := c.failed.Get(path); ok {
		return nil, reason
	}
	if files, ok := c.dirContent.Get(path); ok {
		infos := make([]fs.FileInfo, len(files))
		for i, file := range files {
			info, ok := c.infos.Get(path.Append(file))
			if !ok {
				break
			}
			infos[i] = info
		}
		if len(infos) == len(files) {
			return infos, nil
		}
	}
	c.dirContentLock.Lock()
	defer c.dirContentLock.Unlock()
	infos, err := c.client.ReadDir(path)
	if err != nil {
		c.failed.Add(path, err)
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

func (c *CacheClient) FileInfo(path fsclient.RemotePath) (fs.FileInfo, error) {
	if reason, ok := c.failed.Get(path); ok {
		return nil, reason
	}
	if info, ok := c.infos.Get(path); ok {
		return info, nil
	}
	log.Warn().Msgf("reading single file info from %s", path)
	c.infoLock.Lock()
	defer c.infoLock.Unlock()
	info, err := c.client.FileInfo(path)
	if err != nil {
		c.failed.Add(path, err)
		return nil, err
	}
	c.infos.Add(path, info)
	return info, nil
}
