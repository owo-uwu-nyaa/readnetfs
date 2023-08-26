package cache

import "C"
import (
	"errors"
	"github.com/rs/zerolog/log"
	"readnetfs/tdef"
	"sync"
)

var MEM_PER_FILE_CACHE_B = 1024 * 1024 * 50    // 50MB
var MEM_TOTAL_CACHE_B = 1024 * 1024 * 1024 * 1 //1GB
var MEM_READ_SIZE = 1024 * 1024 * 10           //10MB

// CachedFile supports contiguous reads via cache
type CachedFile struct {
	path                string
	offset              int
	content             []byte
	readUntil           int
	lock                sync.Mutex
	dataRequestCallback func(offset, length int) (*tdef.Finfo, []byte, error)
	fileSize            int
	dead                bool
}

func NewCachedFile(path string, initialOffset int64, dataRequestCallback func(offset int, length int) (*tdef.Finfo, []byte, error)) *CachedFile {
	cf := &CachedFile{path: path, dataRequestCallback: dataRequestCallback, dead: false, offset: int(initialOffset), readUntil: int(initialOffset)}
	finfo := cf.ReadNewData()
	cf.fileSize = int(finfo.Size)
	return cf
}

func (CF *CachedFile) Kill() {
	CF.dead = true
}

func (CF *CachedFile) LenBytes() int {
	return len(CF.content)
}

func (CF *CachedFile) End() int {
	end := CF.offset + len(CF.content)
	return end
}

func (CF *CachedFile) Read(offset, length int) ([]byte, error) {
	if offset > CF.fileSize {
		return []byte{}, nil
	}
	if length+offset > CF.fileSize {
		length = CF.fileSize - offset
	}
	if CF.readUntil != offset {
		//non-contiguous read - the application needs to kill this
		//this object cannot be re-used as goroutines may still try to refill the cache if it isn't marked as dead
		log.Info().Msg("Non-contiguous read, dumping cache and retrying")
		return nil, errors.New("non-contiguous read")
	}
	if CF.End() < offset+length {
		CF.ReadNewData()
	}
	CF.lock.Lock()
	defer CF.lock.Unlock()
	CF.readUntil += length
	var buffer = make([]byte, length)
	copy(buffer, CF.content[offset-CF.offset:offset-CF.offset+length])
	CF.content = CF.content[length:]
	CF.offset += length
	go CF.ReadNewData()
	go CF.ReadNewData()
	go CF.ReadNewData()
	return buffer, nil
}

func (CF *CachedFile) ReadNewData() *tdef.Finfo {
	if CF.dead {
		return nil
	}
	log.Trace().Msg("Reading new data for the cache")
	CF.lock.Lock()
	defer CF.lock.Unlock()
	if CF.LenBytes()+MEM_READ_SIZE > MEM_TOTAL_CACHE_B || (CF.End() >= CF.fileSize && (CF.fileSize > 0)) {
		return nil
	}
	finfo, bytes, err := CF.dataRequestCallback(CF.End(), MEM_READ_SIZE)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to acquire new data for the cache")
	}
	CF.content = append(CF.content, bytes...)
	log.Trace().Msg("Successfully acquired new data for the cache")
	return finfo
}
