package fileretriever

import (
	"github.com/rs/zerolog/log"
	"os"
)

type localClient struct {
	srcDir string
}

func NewLocalclient(srcDir string) *localClient {
	return &localClient{srcDir: srcDir}
}

func (l *localClient) re2lo(remote RemotePath) LocalPath {
	return LocalPath(l.srcDir + "/" + string(remote))
}

func (l *localClient) read(remotePath RemotePath, off int64, dest []byte) ([]byte, error) {
	localPath := l.re2lo(remotePath)
	file, err := os.Open(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to open file %s", localPath)
		return nil, err
	}
	finfo, err := file.Stat()
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to stat file %s", localPath)
		return nil, err
	}
	if off >= finfo.Size() {
		return []byte{}, nil
	}
	seek, err := file.Seek(off, 0)
	if err != nil || seek != off {
		log.Warn().Err(err).Msgf("Failed to seek to %d in file %s", off, localPath)
		return nil, err
	}
	read, err := file.Read(dest)
	if err != nil {
		return nil, err
	}
	return dest[:read], nil
}

func (l *localClient) readDir(path RemotePath) ([]os.FileInfo, error) {
	localPath := l.re2lo(path)
	log.Trace().Msgf("doing read dir at %s", path)
	dir, err := os.ReadDir(localPath.String())
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read dir %s", path)
		return nil, err
	}
	r := make([]os.FileInfo, len(dir))
	for i, file := range dir {
		info, err := file.Info()
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to acquire file info for %s", path.Append(file.Name()))
			continue
		}
		r[i] = info
	}
	return r, nil
}

func (l *localClient) fileInfo(path RemotePath) (os.FileInfo, error) {
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
