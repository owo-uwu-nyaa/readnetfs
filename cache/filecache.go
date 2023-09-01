package cache

import (
	"errors"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"sync"
)

const MEM_PER_FILE_CACHE_B = 1024 * 1024 * 100   // 100MB
const MEM_TOTAL_CACHE_B = 1024 * 1024 * 1024 * 1 //1GB
const BLOCKSIZE = 1024 * 1024 * 1                //1MB and a few

type CacheBlock struct {
	data []byte
	lock sync.Mutex
}

// CachedFile is optimal for contiguous reads
type CachedFile struct {
	lru                 *lru.Cache[int64, *CacheBlock]
	dataRequestCallback func(offset, length int64) ([]byte, error)
	fileSize            int64
	mu                  sync.Mutex
}

func NewCachedFile(fSize int64, dataRequestCallback func(offset int64, length int64) ([]byte, error)) *CachedFile {
	blockLru, _ := lru.New[int64, *CacheBlock](MEM_PER_FILE_CACHE_B / BLOCKSIZE)
	cf := &CachedFile{
		dataRequestCallback: dataRequestCallback,
		fileSize:            fSize,
		lru:                 blockLru,
	}
	return cf
}

func (cf *CachedFile) fillLruBlock(blockNumber int64, block *CacheBlock) error {
	for i := 0; i < 5; i++ {
		buf, err := cf.dataRequestCallback(blockNumber*BLOCKSIZE, BLOCKSIZE)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to acquire new data for the cache")
			continue
		}
		block.data = buf
		return nil
	}
	log.Warn().Msg("Killing Block")
	cf.lru.Remove(blockNumber)
	return errors.New("Failed to fill block")
}

func (cf *CachedFile) Read(offset int64, dest []byte) ([]byte, error) {
	return cf.read(offset, dest)
}

func (cf *CachedFile) read(offset int64, dest []byte) ([]byte, error) {
	if offset > cf.fileSize {
		return dest[:0], nil
	}
	lruBlock := offset / BLOCKSIZE
	blockOffset := offset % BLOCKSIZE
	cf.mu.Lock()
	blck, ok := cf.lru.Get(lruBlock)
	if !ok {
		newBlock := CacheBlock{data: []byte{}}
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
		go cf.ReadNewData(lruBlock + i)
	}
	return blck.data[blockOffset:], nil
}

func (cf *CachedFile) ReadNewData(lrublock int64) {
	if cf.lru.Contains(lrublock) {
		return
	}
	newBlock := CacheBlock{data: []byte{}}
	newBlock.lock.Lock()
	cf.lru.Add(lrublock, &newBlock)
	err := cf.fillLruBlock(lrublock, &newBlock)
	newBlock.lock.Unlock()
	if err != nil {
		return
	}
}
