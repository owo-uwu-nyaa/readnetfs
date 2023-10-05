package fileserver

import (
	"context"
	"fmt"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"math"
	"net"
	"readnetfs/internal/pkg/cacheclient"
	"readnetfs/internal/pkg/common"
	"readnetfs/internal/pkg/fsclient"
	"readnetfs/internal/pkg/netclient"
	"time"
)

type Server struct {
	srcDir       string
	bind         string
	limiter      *rate.Limiter
	client       fsclient.Client
	statsdSocket net.Conn
}

func NewFileServer(srcDir string, bind string, client fsclient.Client, rateLimit int, statsdAddrPort string) *Server {
	maxPacketsPerSecond := (float64(rateLimit) * math.Pow(float64(10), float64(6))) / float64(cacheclient.BLOCKSIZE*8)
	log.Trace().Msgf("setting rate limit to %d data packets per second", maxPacketsPerSecond)
	statsdSocket := common.NewStatsdConn(statsdAddrPort)
	return &Server{srcDir: srcDir, bind: bind, client: client, limiter: rate.NewLimiter(rate.Limit(maxPacketsPerSecond), 2), statsdSocket: statsdSocket}
}

func (f *Server) handleDir(conn net.Conn, request *common.FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.readdir_content:1|c\n")
	infos, err := f.client.ReadDir(fsclient.RemotePath(request.Path))
	if err != nil {
		return
	}
	dirResp := common.NewDirInfo(infos)
	err = dirResp.Marshal(conn)
	if err != nil {
		return
	}
	if err != nil {
		log.Warn().Err(err).Msg("failed to write response")
		return
	}
}

func (f *Server) handleRead(conn net.Conn, request *common.FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.read_content:1|c\n")
	log.Printf("trying to read %d bytes at %d from file %s", request.Length, request.Offset, request.Path)
	start := time.Now()
	err := f.limiter.Wait(context.Background())
	stop := time.Now()
	log.Trace().Msgf("Waited %d millis for rate limiter", stop.Sub(start).Milliseconds())
	if err != nil {
		return
	}
	buf, err := f.client.Read(fsclient.RemotePath(request.Path), request.Offset, make([]byte, request.Length))
	if err != nil {
		return
	}
	fileResponse := common.FileResponse{
		Content: buf,
	}
	log.Debug().Msgf("read %d bytes from file %s", len(buf), request.Path)
	err = struc.Pack(conn, &fileResponse)
	if err != nil {
		log.Warn().Err(err).Msg("failed to write response")
		return
	}
}

func (f *Server) handleInfo(conn net.Conn, request *common.FsRequest) {
	_, _ = fmt.Fprintf(f.statsdSocket, "requests.incoming.file_info:1|c\n")
	info, err := f.client.FileInfo(fsclient.RemotePath(request.Path))
	if err != nil {
		log.Debug().Err(err).Msgf("failed to Read local file info for %s", request.Path)
		return
	}
	err = struc.Pack(conn, common.NewNetInfo(info))
	if err != nil {
		log.Debug().Err(err).Msgf("failed to write file info for %s", request.Path)
		return
	}
}

func (f *Server) handleConn(conn net.Conn) {
	conn = common.WrapStatsdConn(conn, f.statsdSocket)
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to close statsd conn")
		}
	}(conn)
	err := conn.SetDeadline(time.Now().Add(netclient.DEADLINE))
	if err != nil {
		log.Warn().Msg("failed to set deadline")
		return
	}
	request := &common.FsRequest{}
	err = struc.Unpack(conn, request)
	log.Debug().Msgf("got message type %d", request.Type)
	switch common.MessageType(request.Type) {
	case common.FILE_INFO:
		f.handleInfo(conn, request)
	case common.READ_CONTENT:
		f.handleRead(conn, request)
	case common.READDIR_INFO:
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
			log.Info().Err(err).Msg("failed to accept")
			continue
		}
		go f.handleConn(conn)
	}
}
