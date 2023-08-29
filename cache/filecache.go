package cache

import (
	"errors"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

const MEM_PER_FILE_CACHE_B = 1024 * 1024 * 100   // 100MB
const MEM_TOTAL_CACHE_B = 1024 * 1024 * 1024 * 1 //1GB
const BLOCKSIZE = 1024 * 1024 * 1                //1MB and a few

type CacheBlock struct {
	data []byte
	lock sync.Mutex
}

// CachedFile supports contiguous reads via cache
type CachedFile struct {
	lru                 *lru.Cache[int, *CacheBlock]
	dataRequestCallback func(offset, length int) ([]byte, error)
	fileSize            int
}

func NewCachedFile(fSize int, dataRequestCallback func(offset int, length int) ([]byte, error)) *CachedFile {
	blockLru, _ := lru.New[int, *CacheBlock](MEM_PER_FILE_CACHE_B / BLOCKSIZE)
	cf := &CachedFile{
		dataRequestCallback: dataRequestCallback,
		fileSize:            fSize,
		lru:                 blockLru,
	}
	return cf
}

func (CF *CachedFile) fillLruBlock(blockNumber int, block *CacheBlock) error {
	for i := 0; i < 5; i++ {
		buf, err := CF.dataRequestCallback(blockNumber*BLOCKSIZE, BLOCKSIZE)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to acquire new data for the cache")
			continue
		}
		block.data = buf
		return nil
	}
	log.Warn().Msg("Killing Block")
	CF.lru.Remove(blockNumber)
	return errors.New("Failed to fill block")
}

func (CF *CachedFile) Read(offset, length int) ([]byte, error) {
	if offset > CF.fileSize {
		return []byte{}, nil
	}
	lruBlock := offset / BLOCKSIZE
	blockOffset := offset % BLOCKSIZE
	newBlock := CacheBlock{data: []byte{}}
	newBlock.lock.Lock()
	//TODO this is subject to races in extreme edge cases
	peekaboo, ok, _ := CF.lru.PeekOrAdd(lruBlock, &newBlock)
	CF.lru.Get(lruBlock)
	if !ok {
		peekaboo = &newBlock
		err := CF.fillLruBlock(lruBlock, peekaboo)
		if err != nil {
			newBlock.lock.Unlock()
			return nil, err
		}
	}
	newBlock.lock.Unlock()
	peekaboo.lock.Lock()
	defer peekaboo.lock.Unlock()
	for i := 0; i < 10; i++ {
		go CF.ReadNewData(lruBlock + i)
		time.Sleep(10 * time.Nanosecond)
	}
	if len(peekaboo.data) < blockOffset {
		return []byte{}, nil
	}
	if len(peekaboo.data) < blockOffset+length {
		length = len(peekaboo.data) - blockOffset
	}
	return peekaboo.data[blockOffset : blockOffset+length], nil
}

func (CF *CachedFile) ReadNewData(lrublock int) {
	if CF.lru.Contains(lrublock) {
		return
	}
	newBlock := CacheBlock{data: []byte{}}
	newBlock.lock.Lock()
	CF.lru.Add(lrublock, &newBlock)
	err := CF.fillLruBlock(lrublock, &newBlock)
	newBlock.lock.Unlock()
	if err != nil {
		return
	}
}
