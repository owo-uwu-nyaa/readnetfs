package main

import (
	"context"
	"flag"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
	"golang.org/x/sync/semaphore"
	"os"
	"readnetfs/internal/pkg/cacheclient"
	"readnetfs/internal/pkg/failcache"
	"readnetfs/internal/pkg/fileserver"
	"readnetfs/internal/pkg/fsclient"
	"readnetfs/internal/pkg/localclient"
	"readnetfs/internal/pkg/netclient"
	"strings"
	"syscall"
)

var MAX_CONCURRENCY int64 = 10

type VirtNode struct {
	fusefs.Inode
	path fsclient.RemotePath
	sem  *semaphore.Weighted
	fc   *fsclient.FileClient
}

func (n *VirtNode) Open(ctx context.Context, openFlags uint32) (fh fusefs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	return nil, 0, 0
}

func (n *VirtNode) Read(ctx context.Context, fh fusefs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	log.Trace().Msgf("Reading at %d from %s", off, n.path)
	buf, err := n.fc.Read(n.path, off, dest)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read %s", n.path)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf), 0
}

func (n *VirtNode) Write(ctx context.Context, fh fusefs.FileHandle, buf []byte, off int64) (uint32, syscall.Errno) {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	return 0, 0
}

func (n *VirtNode) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	info, err := n.fc.FileInfo(n.path)
	if err != nil {
		return syscall.EIO
	}
	out.Size = uint64(info.Size())
	out.Mtime = uint64(info.ModTime().Unix())
	return 0
}

func (n *VirtNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	log.Debug().Msgf("Looking up %s in %s", name, n.path)
	childPath := n.path.Append(name)
	fInfo, err := n.fc.FileInfo(childPath)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read file info for %s", childPath)
		return nil, syscall.EIO
	}
	stable := fusefs.StableAttr{
		Ino: n.fc.PathToInode(childPath),
	}
	if fInfo.IsDir() {
		stable.Mode = uint32(fuse.S_IFDIR)
	} else {
		stable.Mode = uint32(fuse.S_IFREG)
	}
	cNode := &VirtNode{
		sem:  semaphore.NewWeighted(MAX_CONCURRENCY),
		path: childPath,
		fc:   n.fc,
	}
	child := n.NewInode(ctx, cNode, stable)
	return child, 0
}

func (n *VirtNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	_ = n.sem.Acquire(ctx, 1)
	defer n.sem.Release(1)
	log.Trace().Msgf("Reading dir %s", n.path)
	infos, err := n.fc.ReadDir(n.path)
	if err != nil {
		log.Debug().Err(err).Msgf("Failed to read dir %s", n.path)
		return nil, syscall.EIO
	}
	entries := make([]fuse.DirEntry, 0)
	for _, info := range infos {
		stable := fusefs.StableAttr{
			Ino: n.fc.PathToInode(n.path.Append(info.Name())),
		}
		if info.IsDir() {
			stable.Mode = uint32(fuse.S_IFDIR)
		} else {
			stable.Mode = uint32(fuse.S_IFREG)
		}
		entries = append(entries, fuse.DirEntry{
			Mode: stable.Mode,
			Name: info.Name(),
			Ino:  stable.Ino,
		})
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
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Debug().Msg(strings.Join(os.Args, ","))
	bindAddrPort := flag.String("bind", "", "Bind address and port in x.x.x.x:port format")
	mntDir := flag.String("mnt", "", "Directory to mount the net filesystem on")
	srcDir := flag.String("src", "", "Directory to serve files from")
	flag.Var(&PeerNodes, "peer", "Peer addresses and ports in x.x.x.x:port format, has to specified for each peer like so: -peer x.x.x.x:port -peer x.x.x.x:port ...")
	send := flag.Bool("send", false, "Serve files from the src directory")
	receive := flag.Bool("receive", false, "Receive files and mount the net filesystem on the mnt directory")
	rateLimit := flag.Int("rate", 1000, "Rate limit in Mbit/s")
	allowOther := flag.Bool("allow-other", true, "Allow other users to access the mount")
	statsdAddrPort := flag.String("statsd", "", "Statsd fileserver address and port in x.x.x.x:port format")
	flag.Parse()

	log.Debug().Msg("peers: " + strings.Join(PeerNodes, ", "))
	log.Debug().Msg("bind: " + *bindAddrPort)

	if !*send && !*receive {
		log.Fatal().Msg("Must specify either send or receive or both")
	}
	localClient := failcache.NewFailCache(localclient.NewLocalclient(*srcDir))
	netClient := cacheclient.NewCacheClient(netclient.NewNetClient(*statsdAddrPort, PeerNodes))
	client := fsclient.NewFileClient(fsclient.Client(localClient), fsclient.Client(netClient))
	if *send {
		fserver := fileserver.NewFileServer(*srcDir, *bindAddrPort, localClient, *rateLimit, *statsdAddrPort)
		go fserver.Serve()
	}
	if *receive {
		go func() {
			err := os.Mkdir(*mntDir, 0755)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create mount directory")
			}
			root := &VirtNode{
				Inode: fusefs.Inode{},
				sem:   semaphore.NewWeighted(MAX_CONCURRENCY),
				path:  "",
				fc:    client,
			}
			server, err := fusefs.Mount(*mntDir, root, &fusefs.Options{
				MountOptions: fuse.MountOptions{
					Debug:      false,
					AllowOther: *allowOther,
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
		}()
	}
	//block forever
	select {}
}
