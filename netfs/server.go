package netfs

import (
	"context"
	"fmt"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"io/fs"
	"math"
	"net"
	"os"
	"readnetfs/cache"
	"readnetfs/common"
	"readnetfs/netfs"
	"strings"
	"time"
)

type MessageType byte

const (
	FILE_INFO MessageType = iota
	READ_CONTENT
	READDIR_CONTENT
	READ_DIR_FINFO
)

// TODO use remote path type and custom packer
type FsRequest struct {
	Type       byte
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
	fclient *netfs.localClient
}

func NewFileServer(srcDir string, bind string, fclient *netfs.Client, rateLimit int) *FileServer {
	maxPacketsPerSecond := (float64(rateLimit) * math.Pow(float64(10), float64(6))) / float64(cache.BLOCKSIZE*8)
	log.Trace().Msgf("Setting rate limit to %d data packets per second", maxPacketsPerSecond)
	return &FileServer{srcDir: srcDir, bind: bind, fclient: fclient, limiter: rate.NewLimiter(rate.Limit(maxPacketsPerSecond), 2)}
}

func (f *FileServer) handleDirRequest(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.fclient.statsdSocket, "requests.incoming.readdir_content:1|c\n")
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

func (f *FileServer) handleFileRequest(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.fclient.statsdSocket, "requests.incoming.read_content:1|c\n")
	log.Printf("Trying to Read %d bytes at %d from file %s", request.Length, request.Offset, request.Path)
	start := time.Now()
	err := f.limiter.Wait(context.Background())
	stop := time.Now()
	log.Trace().Msgf("Waited %d millis for rate limiter", stop.Sub(start).Milliseconds())
	if err != nil {
		return
	}
	buf, err := f.fclient.Read(netfs.RemotePath(request.Path), request.Offset, request.Length)
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

func (f *FileServer) handleGetFileInfo(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.fclient.statsdSocket, "requests.incoming.file_info:1|c\n")
	fInfo, err := f.fclient.FileInfo(netfs.RemotePath(request.Path))
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to Read local file info for %s", request.Path)
		return
	}
	err = struc.Pack(conn, fInfo)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to write file info for %s", request.Path)
		return
	}
}

func (f *FileServer) handleDirFInfo(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.fclient.statsdSocket, "requests.incoming.read_dir_finfo:1|c\n")
	path := f.fclient.Re2Lo(netfs.RemotePath(request.Path))
	root := os.DirFS(path.String())
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to Read dir for %s", request.Path)
		return
	}
	fInfos := netfs.DirFInfo{FInfos: make([]netfs.netInfo, 0)}
	for _, e := range entries {
		fInfo, err := e.Info()
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to Read file info for %s", e.Name())
			continue
		}
		fInfos.FInfos = append(fInfos.FInfos, netfs.netInfo{
			Name:    fInfo.Name(),
			Size:    fInfo.Size(),
			IsDir:   fInfo.IsDir(),
			ModTime: fInfo.ModTime().Unix(),
		})
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to write file info for %s", e.Name())
			return
		}
	}
	//TODO use custom packer
	write, err := conn.Write([]byte{byte(len(fInfos.FInfos))})
	if err != nil || write != 1 {
		log.Debug().Err(err).Msgf("Failed to write num of file infos for dir %s", request.Path)
	}
	for _, fInfo := range fInfos.FInfos {
		err = struc.Pack(conn, &fInfo)
		if err != nil {
			log.Debug().Err(err).Msgf("Failed to write file info for dir %s", request.Path)
		}
	}
}

func (f *FileServer) handleConn(conn net.Conn) {
	conn = common.WrapStatsdConn(conn, f.fclient.statsdSocket)
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Warn().Msg("Failed to set deadline")
		return
	}
	request := &FsRequest{}
	messageType := make([]byte, 1)
	n, err := conn.Read(messageType)
	if err != nil || n != 1 {
		log.Warn().Err(err).Msg("Failed to Read message type")
		return
	}
	log.Debug().Msgf("Got message type %d", messageType[0])
	switch MessageType(messageType[0]) {
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
	case READ_DIR_FINFO:
		err = struc.Unpack(conn, request)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to unpack request")
		}
		f.handleDirFInfo(conn, request)
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
