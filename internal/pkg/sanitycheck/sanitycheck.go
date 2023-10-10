package sanitycheck

import (
	"github.com/rs/zerolog/log"
	"io/fs"
	"readnetfs/internal/pkg/fsclient"
)

type SanityCheck struct {
	client fsclient.Client
}

func NewSanityCheck(client fsclient.Client) *SanityCheck {
	return &SanityCheck{client: client}
}

func (f *SanityCheck) Purge() {
	f.client.Purge()
}

func (f *SanityCheck) Read(path fsclient.RemotePath, offset int64, dest []byte) (buf []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Err(err).Msgf("panic in Read for %s", path)
			f.Purge()
			buf = nil
			err = fs.ErrInvalid
			return
		}
	}()
	buf, err = f.client.Read(path, offset, dest)
	if err == nil {
		info, err := f.client.FileInfo(path)
		if err != nil {
			return nil, err
		}
		if len(buf) == 0 && (info.Size() > offset) || info.Size() < offset+int64(len(buf)) {
			return nil, fs.ErrInvalid
		}
	}
	if buf == nil {
		return nil, fs.ErrNotExist
	}
	return
}

func (f *SanityCheck) ReadDir(path fsclient.RemotePath) (infos []fs.FileInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Err(err).Msgf("panic in ReadDir for %s", path)
			f.Purge()
			infos = nil
			err = fs.ErrInvalid
			return
		}
	}()
	infos, err = f.client.ReadDir(path)
	return
}

func (f SanityCheck) FileInfo(path fsclient.RemotePath) (info fs.FileInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Err(err).Msgf("panic in FileInfo for %s", path)
			f.Purge()
			info = nil
			err = fs.ErrInvalid
			return
		}
	}()
	info, err = f.client.FileInfo(path)
	return
}
