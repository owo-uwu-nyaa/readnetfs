package main

import (
	"context"
	"flag"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io/fs"
	"os"
	"readnetfs/cache"
	"readnetfs/fileretriever"
	"strings"
	"syscall"
)

type NetNode struct {
	fusefs.Inode
	path string
}

var fcache = make(map[string]*cache.CachedFile)

type NetNodeFH struct {
	path string
}

func (n *NetNodeFH) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Trace().Msgf("Reading at %d from %s", off, n.path)
	prefixlessPath := n.path[len(fclient.SrcDir()):]
	if prefixlessPath != "" && prefixlessPath[0] == '/' {
		prefixlessPath = prefixlessPath[1:]
	}
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
	fInfo, err := fclient.FileInfo(prefixlessPath)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", n.path)
		return nil, syscall.EIO
	}
	cf := cache.NewCachedFile(prefixlessPath, int(fInfo.Size), func(offset, length int) ([]byte, error) {
		return fclient.Read(prefixlessPath, offset, length)
	})
	buf, err := cf.Read(int(off), len(dest))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", prefixlessPath)
		return nil, syscall.EIO
	}
	fcache[prefixlessPath] = cf
	return fuse.ReadResultData(buf), 0
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

func (n *NetNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	path := n.path + "/" + name
	finfo, err := fclient.FileInfo(path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", path)
		return nil, syscall.EIO
	}
	var mode fs.FileMode
	if finfo.IsDir {
		mode = fuse.S_IFDIR
	} else {
		mode = fuse.S_IFREG
	}
	stable := fusefs.StableAttr{
		Mode: uint32(mode),
		Ino:  fclient.ThisFsToInode(path),
	}
	cNode := &NetNode{
		path: path,
	}
	child := n.NewInode(ctx, cNode, stable)
	return child, 0
}
func (n *NetNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	log.Trace().Msgf("Reading dir %s", n.path)
	prefixlessPath := n.path[len(fclient.SrcDir()):]
	if prefixlessPath != "" && prefixlessPath[0] == '/' {
		prefixlessPath = prefixlessPath[1:]
	}
	entries, err := fclient.ReadDir(prefixlessPath)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read dir %s", n.path)
		return nil, syscall.EIO
	}
	return fusefs.NewListDirStream(entries), 0
}

var PeerNodes PeerNode

type PeerNode []string

func (i *PeerNode) String() string {
	return "peer"
}

func (i *PeerNode) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var fclient *fileretriever.FileClient

func main() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	log.Debug().Msg(strings.Join(os.Args, ","))
	bindAddrPort := flag.String("bind", "", "bind address and port")
	mntDir := flag.String("mnt", "", "mnt")
	srcDir := flag.String("src", "", "src")
	flag.Var(&PeerNodes, "peer", "peer address and port")
	flag.Parse()
	log.Debug().Msg("peers: " + strings.Join(PeerNodes, ", "))
	log.Debug().Msg("bind: " + *bindAddrPort)

	fclient = fileretriever.NewFileClient(*srcDir, PeerNodes)

	fserver := fileretriever.NewFileServer(*srcDir, *bindAddrPort, fclient)
	go fserver.Serve()

	os.Mkdir(*mntDir, 0755)
	root := &NetNode{
		Inode: fusefs.Inode{},
		path:  *srcDir,
	}
	server, err := fusefs.Mount(*mntDir, root, &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      false,
			AllowOther: true,
		},
	})
	if err != nil {
		log.Debug().Err(err).Msg("Failed to mount")
	}

	log.Printf("Mounted on %s", *mntDir)
	log.Printf("Unmount by calling 'fusermount -u %s'", *mntDir)

	// Wait until unmount before exiting
	server.Wait()
}
