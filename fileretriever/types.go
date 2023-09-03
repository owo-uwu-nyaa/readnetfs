package fileretriever

import (
	"io/fs"
	"time"
)

type netInfo struct {
	nameLength int64 `struc:"int16,sizeof=Name"`
	name       string
	size       int64
	isDir      bool
	modTime    int64
}

func (n *netInfo) Name() string {
	return n.name
}

func (n *netInfo) Size() int64 {
	return n.size
}

func (n *netInfo) Mode() fs.FileMode {
	return 0
}

func (n *netInfo) ModTime() time.Time {
	return time.Unix(n.modTime, 0)
}

func (n *netInfo) IsDir() bool {
	return n.isDir
}

func (n *netInfo) Sys() any {
	return nil
}
