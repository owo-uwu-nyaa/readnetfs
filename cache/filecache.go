package cache

import (
	"github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"sync"
)

const MEM_PER_FILE_CACHE_B = 1024 * 1024 * 100   // 100MB
const MEM_TOTAL_CACHE_B = 1024 * 1024 * 1024 * 1 //1GB
const BLOCKSIZE = 1024 * 1024 * 1                //10MB

type CacheBlock struct {
	data []byte
	lock sync.Mutex
}

// CachedFile supports contiguous reads via cache
type CachedFile struct {
	lru                 *lru.Cache[int, *CacheBlock]
	dataRequestCallback func(offset, length int) ([]byte, error)
	fileSize            int
	dead                bool
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

func (CF *CachedFile) Kill() {
	CF.dead = true
}

func (CF *CachedFile) fillLruBlock(blockNumber int, block *CacheBlock) {
	buf, err := CF.dataRequestCallback(blockNumber*BLOCKSIZE, BLOCKSIZE)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to acquire new data for the cache")
	}
	block.data = buf
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
		CF.fillLruBlock(lruBlock, peekaboo)
	}
	newBlock.lock.Unlock()
	peekaboo.lock.Lock()
	defer peekaboo.lock.Unlock()
	for i := 0; i < CF.lru.Len()/10; i++ {
		go CF.ReadNewData(lruBlock + i)
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
	CF.fillLruBlock(lrublock, &newBlock)
	newBlock.lock.Unlock()
}
