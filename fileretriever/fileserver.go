package fileretriever

import (
	"context"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"io/fs"
	"math"
	"net"
	"os"
	"readnetfs/cache"
	"strings"
	"time"
)

const (
	FILE_INFO byte = iota
	READ_CONTENT
	READDIR_CONTENT
)

// TODO use remote path type and custom packer
type FileRequest struct {
	Offset     int64
	Length     int64
	PathLength int64 `struc:"int16,sizeof=Path"`
	Path       string
}

type FileResponse struct {
	Length   int64 `struc:"int64,sizeof=Content"`
	FileSize int64
	Content  []byte
}

type DirResponse struct {
	DirLength  int64 `struc:"int32,sizeof=Dirs"`
	Dirs       []byte
	FileLength int64 `struc:"int32,sizeof=Files"`
	Files      []byte
}

type FileServer struct {
	srcDir  string
	bind    string
	limiter *rate.Limiter
	fclient *FileClient
}

func NewFileServer(srcDir string, bind string, fclient *FileClient, rateLimit int) *FileServer {
	maxPacketsPerSecond := (float64(rateLimit) * math.Pow(float64(10), float64(6))) / float64(cache.BLOCKSIZE*8)
	log.Trace().Msgf("Setting rate limit to %d data packets per second", maxPacketsPerSecond)
	return &FileServer{srcDir: srcDir, bind: bind, fclient: fclient, limiter: rate.NewLimiter(rate.Limit(maxPacketsPerSecond), 2)}
}

func (f *FileServer) handleDirRequest(conn net.Conn, request *FileRequest) {
	path := f.srcDir + "/" + request.Path
	root := os.DirFS(path)
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		return
	}
	files := make([]string, 0)
	dirs := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		} else {
			files = append(files, e.Name())
		}
	}
	dirResponse := DirResponse{
		Dirs:  []byte(strings.Join(dirs, "\x00")),
		Files: []byte(strings.Join(files, "\x00")),
	}
	err = struc.Pack(conn, &dirResponse)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to write response")
		return
	}
}

func (f *FileServer) handleFileRequest(conn net.Conn, request *FileRequest) {
	log.Printf("Trying to read %d bytes at %d from file %s", request.Length, request.Offset, request.Path)
	start := time.Now()
	err := f.limiter.Wait(context.Background())
	stop := time.Now()
	log.Trace().Msgf("Waited %d millis for rate limiter", stop.Sub(start).Milliseconds())
	if err != nil {
		return
	}
	buf, err := f.fclient.localRead(RemotePath(request.Path), request.Offset, request.Length)
	if err != nil {
		return
	}
	fileResponse := &FileResponse{
		Content: buf,
	}
	err = struc.Pack(conn, fileResponse)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to write response")
		return
	}
}

func (f *FileServer) handleGetFileInfo(conn net.Conn, request *FileRequest) {
	fInfo, err := f.fclient.localFileInfo(RemotePath(request.Path))
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read local file info for %s", request.Path)
		return
	}
	struc.Pack(conn, fInfo)
}

func (f *FileServer) handleConn(conn net.Conn) {
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Warn().Msg("Failed to set deadline")
		return
	}
	request := &FileRequest{}
	messageType := make([]byte, 1)
	n, err := conn.Read(messageType)
	if err != nil || n != 1 {
		log.Warn().Err(err).Msg("Failed to read message type")
		return
	}
	switch messageType[0] {
	case FILE_INFO:
		err = struc.Unpack(conn, request)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to unpack request")
			return
		}
		f.handleGetFileInfo(conn, request)
	case READ_CONTENT:
		err = struc.Unpack(conn, request)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to unpack request")
			return
		}
		f.handleFileRequest(conn, request)
	case READDIR_CONTENT:
		err = struc.Unpack(conn, request)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to unpack request")
			return
		}
		f.handleDirRequest(conn, request)
	}
}

func (f *FileServer) Serve() {
	ln, err := net.Listen("tcp", f.bind)
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Info().Err(err).Msg("Failed to accept")
			continue
		}
		go f.handleConn(conn)
	}
}
