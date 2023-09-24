package cacheclient

import (
	"errors"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"readnetfs/internal/pkg/fsclient"
	"sync"
)

const MEM_PER_FILE_CACHE_B = 1024 * 1024 * 100   // 100MB
const MEM_TOTAL_CACHE_B = 1024 * 1024 * 1024 * 1 //1GB
const BLOCKSIZE = 1024 * 1024 * 1                //1MB and a few

type cacheBlock struct {
	data []byte
	lock sync.Mutex
}

// CachedFile is optimal for contiguous reads
type CachedFile struct {
	lru      *lru.Cache[int64, *cacheBlock]
	fileSize int64
	mu       sync.Mutex
	client   fsclient.Client
	path     fsclient.RemotePath
}

func NewCachedFile(path fsclient.RemotePath, client fsclient.Client) (*CachedFile, error) {
	blockLru, _ := lru.New[int64, *cacheBlock](MEM_PER_FILE_CACHE_B / BLOCKSIZE)
	info, err := client.FileInfo(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", path)
		return nil, err
	}
	cf := &CachedFile{
		client:   client,
		path:     path,
		fileSize: info.Size(),
		lru:      blockLru,
	}
	return cf, nil
}

func (cf *CachedFile) fillLruBlock(blockNumber int64, block *cacheBlock) error {
	buf, err := cf.client.Read(cf.path, blockNumber*BLOCKSIZE, make([]byte, BLOCKSIZE))
	if err != nil {
		log.Warn().Msgf("killing block %d for file %s", blockNumber, cf.path)
		cf.lru.Remove(blockNumber)
		return errors.New("failed to fill block")
	}
	block.data = buf
	return nil

}

func (cf *CachedFile) Read(offset int64, dest []byte) ([]byte, error) {
	if offset > cf.fileSize {
		return dest[:0], nil
	}
	lruBlock := offset / BLOCKSIZE
	blockOffset := offset % BLOCKSIZE
	cf.mu.Lock()
	blck, ok := cf.lru.Get(lruBlock)
	if !ok {
		newBlock := cacheBlock{data: []byte{}}
		newBlock.lock.Lock()
		cf.lru.Add(lruBlock, &newBlock)
		cf.mu.Unlock()
		err := cf.fillLruBlock(lruBlock, &newBlock)
		newBlock.lock.Unlock()
		if err != nil {
			return dest[:0], err
		}
		blck = &newBlock
	} else {
		cf.mu.Unlock()
	}
	blck.lock.Lock()
	defer blck.lock.Unlock()
	for i := int64(0); i < 3; i++ {
		go cf.readNewData(lruBlock + i)
	}
	end := blockOffset + int64(len(dest))
	if end > int64(len(blck.data)) {
		end = int64(len(blck.data))
	}
	ret := blck.data[blockOffset:end]
	if len(ret) < len(dest) && offset+int64(len(ret)) < cf.fileSize {
		nb, err := cf.Read((lruBlock+1)*(BLOCKSIZE), dest[len(ret):])
		if err == nil {
			ret = append(ret, nb...)
		}
	}
	return ret, nil
}

func (cf *CachedFile) readNewData(lrublock int64) {
	if cf.lru.Contains(lrublock) {
		return
	}
	newBlock := cacheBlock{data: []byte{}}
	newBlock.lock.Lock()
	cf.lru.Add(lrublock, &newBlock)
	err := cf.fillLruBlock(lrublock, &newBlock)
	newBlock.lock.Unlock()
	if err != nil {
		return
	}
}
