package main

import (
	"context"
	"flag"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"readnetfs/cache"
	"readnetfs/fileretriever"
	"strings"
	"sync"
	"syscall"
)

type VirtNode struct {
	fusefs.Inode
	path fileretriever.RemotePath
	mu   sync.Mutex
	fc   *fileretriever.FileClient
}

func (f *VirtNode) Open(ctx context.Context, openFlags uint32) (fh fusefs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, 0, 0
}

func (n *VirtNode) Read(ctx context.Context, fh fusefs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Trace().Msgf("Reading at %d from %s", off, n.path)
	cacheEntry := n.fc.GetCachedFile(n.path)
	if cacheEntry != nil {
		buf, err := cacheEntry.Read(int(off), len(dest))
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to read %s", n.path)
			return nil, syscall.EIO
		}
		return fuse.ReadResultData(buf), 0
	}
	fInfo, err := n.fc.FileInfo(n.path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", n.path)
		return nil, syscall.EIO
	}
	cf := cache.NewCachedFile(int(fInfo.Size), func(offset, length int) ([]byte, error) {
		return n.fc.Read(n.path, offset, length)
	})
	cf = n.fc.PutOrGet(n.path, cf)
	buf, err := cf.Read(int(off), len(dest))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to read %s", n.path)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf), 0
}

func (n *VirtNode) Write(ctx context.Context, fh fusefs.FileHandle, buf []byte, off int64) (uint32, syscall.Errno) {
	return 0, 0
}

func (n *VirtNode) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fInfo, err := n.fc.FileInfo(n.path)
	if err != nil {
		return syscall.EIO
	}
	out.Size = uint64(fInfo.Size)
	out.Mtime = uint64(fInfo.ModTime)
	return 0
}

func (n *VirtNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	log.Debug().Msgf("Looking up %s in %s", name, n.path)
	childpath := n.path.Append(name)
	fInfo, err := n.fc.FileInfo(childpath)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", childpath)
		return nil, syscall.EIO
	}
	stable := fusefs.StableAttr{
		Ino: n.fc.ThisFsToInode(childpath),
	}
	if fInfo.IsDir {
		stable.Mode = uint32(fuse.S_IFDIR)
	} else {
		stable.Mode = uint32(fuse.S_IFREG)
	}
	cNode := &VirtNode{
		path: childpath,
		fc:   n.fc,
	}
	child := n.NewInode(ctx, cNode, stable)
	return child, 0
}

func (n *VirtNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	log.Trace().Msgf("Reading dir %s", n.path)
	entries, err := n.fc.ReadDir(n.path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read dir %s", n.path)
		return nil, syscall.EIO
	}
	return fusefs.NewListDirStream(entries), 0
}

var PeerNodes PeerAddress

type PeerAddress []string

func (i *PeerAddress) String() string {
	return "peer"
}

func (i *PeerAddress) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	log.Debug().Msg(strings.Join(os.Args, ","))
	bindAddrPort := flag.String("bind", "", "bind address and port")
	mntDir := flag.String("mnt", "", "mnt")
	srcDir := flag.String("src", "", "src")
	flag.Var(&PeerNodes, "peer", "peer address and port")
	send := flag.Bool("send", false, "send")
	receive := flag.Bool("receive", false, "receive")
	rateLimit := flag.Int("rate", 1000, "rate limit in Mbit/s")

	flag.Parse()
	log.Debug().Msg("peers: " + strings.Join(PeerNodes, ", "))
	log.Debug().Msg("bind: " + *bindAddrPort)

	fclient := fileretriever.NewFileClient(*srcDir, PeerNodes)

	if !*send && !*receive {
		log.Fatal().Msg("Must specify either send or receive or both")
	}

	if *send {
		fserver := fileretriever.NewFileServer(*srcDir, *bindAddrPort, fclient, *rateLimit)
		go fserver.Serve()
	}

	if *receive {
		os.Mkdir(*mntDir, 0755)
		root := &VirtNode{
			Inode: fusefs.Inode{},
			path:  "",
			fc:    fclient,
		}
		server, err := fusefs.Mount(*mntDir, root, &fusefs.Options{
			MountOptions: fuse.MountOptions{
				Debug:      false,
				AllowOther: true,
				FsName:     "chrislfs",
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
}
