package main

import (
	"context"
	"errors"
	"flag"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/lunixbochs/struc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io/fs"
	"net"
	"os"
	"readnetfs/cache"
	"readnetfs/tdef"
	"strings"
	"sync"
	"syscall"
)

type peerNode []string

func (i *peerNode) String() string {
	return "peer"
}

func (i *peerNode) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var peerNodes peerNode

type NetNode struct {
	fusefs.Inode
	path string
}

var ic uint64 = 0
var iMap = make(map[string]uint64)

var fcache = make(map[string]*cache.CachedFile)

type NetNodeFH struct {
	path string
}

type FileRequest struct {
	IsDir      bool
	Offset     int
	Length     int
	PathLength int `struc:"int16,sizeof=Path"`
	Path       string
}

type FileResponse struct {
	Length   int `struc:"int32,sizeof=Content"`
	FileSize int
	Content  []byte
}

type DirResponse struct {
	Length  int `struc:"int32,sizeof=Content"`
	Content []byte
}

func netRead(file string, offset int, length int) (*tdef.Finfo, []byte, syscall.Errno) {
	log.Trace().Msgf("doing net read at %d for len %d", offset, length)
	conn, err := net.Dial("tcp", peerNodes[0])
	if err != nil {
		return nil, nil, syscall.EIO
	}
	defer conn.Close()
	request := &FileRequest{
		IsDir:  false,
		Offset: offset,
		Length: length,
		Path:   file,
	}
	err = struc.Pack(conn, request)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to pack request")
		return nil, nil, syscall.EIO
	}
	var fInfo tdef.Finfo
	var response FileResponse
	struc.Unpack(conn, &fInfo)
	struc.Unpack(conn, &response)
	return &fInfo, response.Content, 0
}

func ReadFile(path string, off, length int) (*tdef.Finfo, []byte, error) {
	prefixlessPath := path[len(srcDir)+1:]
	file, err := os.Open(path)
	if err != nil {
		wg := sync.WaitGroup{}
		wg.Add(1)
		var p1 []byte
		var e1 syscall.Errno
		var f1 *tdef.Finfo
		log.Debug().Msgf("Reading from net %s", prefixlessPath)
		go func() {
			f1, p1, e1 = netRead(prefixlessPath, off, length)
			wg.Done()
		}()
		wg.Wait()
		if e1 != 0 {
			log.Debug().Err(e1).Msg("Failed to read from net")
			return nil, nil, errors.New("failed to read from net")
		}
		return f1, p1, nil
	}
	defer file.Close()
	f1, err := file.Stat()
	finf := &tdef.Finfo{
		Name: f1.Name(),
		Size: f1.Size(),
	}
	if off >= int(f1.Size()) {
		return finf, []byte{}, nil
	}
	file.Seek(int64(off), 0)
	buf := make([]byte, length)
	read, err := file.Read(buf)
	if err != nil {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, err
	}
	return finf, buf[:read], nil
}

func (n *NetNodeFH) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Trace().Msgf("Reading at %d from %s", off, n.path)
	prefixlessPath := n.path[len(srcDir)+1:]
	cacheEntry, ok := fcache[prefixlessPath]
	if ok {
		buf, err := cacheEntry.Read(int(off), len(dest))
		if err != nil {
			cacheEntry.Kill()
			delete(fcache, prefixlessPath)
			return n.Read(ctx, dest, off)
		}
		return fuse.ReadResultData(buf), 0
	}
	cf := cache.NewCachedFile(prefixlessPath, off, func(offset, length int) (*tdef.Finfo, []byte, error) {
		return ReadFile(n.path, offset, length)
	})
	buf, err := cf.Read(int(off), len(dest))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", prefixlessPath)
		return nil, syscall.EIO
	}
	fcache[prefixlessPath] = cf
	return fuse.ReadResultData(buf), 0
}

func pathToMode(path string) uint32 {
	file, err := os.Open(path)
	if err != nil {
		return uint32(fuse.S_IFREG)
	}
	defer file.Close()
	fInfo, _ := file.Stat()
	mode := fuse.S_IFREG
	if fInfo.IsDir() {
		mode = fuse.S_IFDIR
	}
	return uint32(mode)
}

func (n *NetNode) Open(ctx context.Context, openFlags uint32) (fh fusefs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// disallow writes
	if fuseFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}
	fh = &NetNodeFH{
		path: n.path,
	}
	// Return FOPEN_DIRECT_IO so content is not cached.
	return fh, fuse.FOPEN_DIRECT_IO, 0
}

func pathToFuseDirEntry(path, name string) fuse.DirEntry {
	return fuse.DirEntry{
		Mode: pathToMode(path + "/" + name),
		Ino:  pathToInode(path + "/" + name),
		Name: name,
	}
}

func netReadDir(path string) (fusefs.DirStream, syscall.Errno) {
	conn, err := net.Dial("tcp", peerNodes[0])
	if err != nil {
		return nil, syscall.EIO
	}
	defer conn.Close()
	request := &FileRequest{
		IsDir:  true,
		Offset: 0,
		Length: 0,
		Path:   path,
	}
	struc.Pack(conn, request)
	var dirResponse DirResponse
	struc.Unpack(conn, &dirResponse)
	paths := strings.Split(string(dirResponse.Content), "\x00")
	r := make([]fuse.DirEntry, len(paths))
	for i, file := range paths {
		r[i] = pathToFuseDirEntry(path, file)
	}
	return fusefs.NewListDirStream(r), 0
}

func (n *NetNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	root := os.DirFS(n.path)
	entries, err := fs.ReadDir(root, ".")
	if err != nil || len(entries) == 0 {
		return netReadDir(n.path)
	}
	log.Debug().Msg("Read local dir" + n.path)
	log.Debug().Fields(entries)
	r := make([]fuse.DirEntry, len(entries))
	for i, e := range entries {
		r[i] = pathToFuseDirEntry(n.path, e.Name())
	}
	return fusefs.NewListDirStream(r), 0
}

func pathToInode(path string) uint64 {
	if val, ok := iMap[path]; ok {
		return val
	}
	ic++
	iMap[path] = ic
	return ic
}

func (n *NetNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	path := n.path + "/" + name
	mode := fuse.S_IFREG
	stable := fusefs.StableAttr{
		Mode: uint32(mode),
		Ino:  pathToInode(path),
	}
	cNode := &NetNode{
		path: path,
	}

	child := n.NewInode(ctx, cNode, stable)

	return child, 0
}

func handleDirRequest(conn net.Conn, srcDir string, request *FileRequest) {
	path := srcDir + "/" + request.Path
	root := os.DirFS(path)
	entries, err := fs.ReadDir(root, ".")
	if err != nil {
		return
	}
	r := make([]string, 0, len(entries))
	for i, e := range entries {
		r[i] = e.Name()
	}
	dirResponse := DirResponse{
		Content: []byte(strings.Join(r, "\x00")),
	}
	struc.Pack(conn, &dirResponse)
}

func handleFileRequest(conn net.Conn, srcDir string, request *FileRequest) {
	file, err := os.Open(srcDir + "/" + request.Path)
	if err != nil {
		return
	}
	log.Printf("Trying to read %d bytes at %d from file %s", request.Length, request.Offset, request.Path)
	file.Seek(int64(request.Offset), 0)
	defer file.Close()
	buffer := make([]byte, request.Length)
	read, err := file.Read(buffer)
	buffer = buffer[:read]
	if err != nil {
		return
	}
	log.Printf("Read %d bytes", read)
	fStat, _ := file.Stat()
	fInfo := &tdef.Finfo{
		Name: fStat.Name(),
		Size: fStat.Size(),
	}
	struc.Pack(conn, fInfo)
	fileResponse := &FileResponse{
		Content: buffer,
	}
	struc.Pack(conn, fileResponse)
}

func handleConn(conn net.Conn, srcDir string) {
	defer conn.Close()
	request := &FileRequest{}
	err := struc.Unpack(conn, request)
	if err != nil {
		return
	}
	if request.IsDir {
		handleDirRequest(conn, srcDir, request)
	} else {
		handleFileRequest(conn, srcDir, request)
	}
}

func serveDir(srcDir, bindAddrPort string) {
	ln, err := net.Listen("tcp", bindAddrPort)
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Info().Err(err).Msg("Failed to accept")
			conn.Close()
		}
		go handleConn(conn, srcDir)
	}
}

var srcDir string

func main() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	log.Debug().Msg(strings.Join(os.Args, ","))
	bindAddrPort := flag.String("bind", "", "bind address and port")
	mntDir := flag.String("mnt", "", "mnt")
	flag.StringVar(&srcDir, "src", "", "src")
	flag.Var(&peerNodes, "peer", "peer address and port")
	flag.Parse()
	log.Debug().Msg("peers: " + strings.Join(peerNodes, ", "))
	log.Debug().Msg("bind: " + *bindAddrPort)

	go serveDir(srcDir, *bindAddrPort)

	os.Mkdir(*mntDir, 0755)
	root := &NetNode{
		Inode: fusefs.Inode{},
		path:  srcDir,
	}
	server, err := fusefs.Mount(*mntDir, root, &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug: false,
		},
	})
	if err != nil {
		log.Debug().Err(err).Msg("Failed to mount")
	}

	log.Printf("Mounted on %s", mntDir)
	log.Printf("Unmount by calling 'fusermount -u %s'", mntDir)

	// Wait until unmount before exiting
	server.Wait()
}
