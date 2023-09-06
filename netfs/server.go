package netfs

import (
	"context"
	"fmt"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"math"
	"net"
	"readnetfs/cache"
	"readnetfs/common"
	"time"
)

type MessageType byte

type Server struct {
	srcDir       string
	bind         string
	limiter      *rate.Limiter
	client       Client
	statsdSocket net.Conn
}

func NewFileServer(srcDir string, bind string, client *localClient, rateLimit int, statsdAddrPort string) *Server {
	maxPacketsPerSecond := (float64(rateLimit) * math.Pow(float64(10), float64(6))) / float64(cache.BLOCKSIZE*8)
	log.Trace().Msgf("Setting rate limit to %d data packets per second", maxPacketsPerSecond)
	statsdSocket := common.NewStatsdConn(statsdAddrPort)
	return &Server{srcDir: srcDir, bind: bind, client: client, limiter: rate.NewLimiter(rate.Limit(maxPacketsPerSecond), 2), statsdSocket: statsdSocket}
}

func (f *Server) handleDir(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.readdir_content:1|c\n")
	infos, err := f.client.ReadDir(RemotePath(request.Path))
	if err != nil {
		return
	}
	dirResp := NewDirInfo(infos)
	err = dirResp.Marshal(conn)
	if err != nil {
		return
	}
	if err != nil {
		log.Warn().Err(err).Msg("Failed to write response")
		return
	}
}

func (f *Server) handleRead(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.read_content:1|c\n")
	log.Printf("Trying to Read %d bytes at %d from file %s", request.Length, request.Offset, request.Path)
	start := time.Now()
	err := f.limiter.Wait(context.Background())
	stop := time.Now()
	log.Trace().Msgf("Waited %d millis for rate limiter", stop.Sub(start).Milliseconds())
	if err != nil {
		return
	}
	buf, err := f.client.Read(RemotePath(request.Path), request.Offset, make([]byte, request.Length))
	if err != nil {
		return
	}
	fileResponse := FileResponse{
		Content: buf,
	}
	err = struc.Pack(conn, &fileResponse)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to write response")
		return
	}
}

func (f *Server) handleInfo(conn net.Conn, request *FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.file_info:1|c\n")
	info, err := f.client.FileInfo(RemotePath(request.Path))
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to Read local file info for %s", request.Path)
		return
	}
	err = struc.Pack(conn, NewNetInfo(info))
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to write file info for %s", request.Path)
		return
	}
}

func (f *Server) handleConn(conn net.Conn) {
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(DEADLINE))
	if err != nil {
		log.Warn().Msg("Failed to set deadline")
		return
	}
	request := &FsRequest{}
	log.Debug().Msgf("Got message type %d", request.Type)
	err = struc.Unpack(conn, request)
	switch MessageType(request.Type) {
	case FILE_INFO:
		f.handleInfo(conn, request)
	case READ_CONTENT:
		f.handleRead(conn, request)
	case READDIR_INFO:
		f.handleDir(conn, request)
	}
}

func (f *Server) Serve() {
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
