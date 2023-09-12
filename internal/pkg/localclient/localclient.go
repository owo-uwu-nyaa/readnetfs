package localclient

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"io/fs"
	"os"
	"readnetfs/internal/pkg/common"
	"readnetfs/internal/pkg/fsclient"
)

type LocalClient struct {
	srcDir string
}

func NewLocalclient(srcDir string) *LocalClient {
	return &LocalClient{srcDir: srcDir}
}

func (l *LocalClient) re2lo(remote fsclient.RemotePath) common.LocalPath {
	if len(remote) == 0 {
		return common.LocalPath(l.srcDir)
	}
	if remote[0] == '/' {
		remote = remote[1:]
	}
	return common.LocalPath(fmt.Sprintf("%s/%s", l.srcDir, string(remote)))
}

func (l *LocalClient) Read(remotePath fsclient.RemotePath, off int64, dest []byte) ([]byte, error) {
	localPath := l.re2lo(remotePath)
	file, err := os.Open(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("failed to open file %s", localPath)
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		log.Warn().Err(err).Msgf("failed to stat file %s", localPath)
		return nil, err
	}
	if off >= info.Size() {
		return []byte{}, nil
	}
	if off+int64(len(dest)) > info.Size() {
		dest = dest[:info.Size()-off]
	}
	seek, err := file.Seek(off, io.SeekStart)
	if err != nil || seek != off {
		return nil, errors.Join(err, errors.New("failed to seek to correct offset"))
	}
	read, err := io.ReadAtLeast(file, dest, len(dest))
	if err != nil {
		return nil, err
	}
	if read != len(dest) {
		log.Debug().Err(err).Msgf("failed to read enough bytes from %s", localPath)
	}
	return dest, nil
}

func (l *LocalClient) ReadDir(path fsclient.RemotePath) ([]os.FileInfo, error) {
	localPath := l.re2lo(path)
	log.Trace().Msgf("read dir at %s", path)
	dir, err := os.ReadDir(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("failed to Read dir %s", path)
		return nil, err
	}
	r := make([]os.FileInfo, len(dir))
	for i, file := range dir {
		info, err := file.Info()
		if err != nil {
			log.Debug().Err(err).Msgf("failed to acquire file info for %s", path.Append(file.Name()))
			continue
		}
		r[i] = info
	}
	return r, nil
}

func (l *LocalClient) FileInfo(path fsclient.RemotePath) (fs.FileInfo, error) {
	file, err := os.Open(l.re2lo(path).String())
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return info, nil
}
