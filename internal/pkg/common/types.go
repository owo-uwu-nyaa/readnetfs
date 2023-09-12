package common

import (
	"encoding/binary"
	"github.com/lunixbochs/struc"
	"io"
	"io/fs"
	"time"
)

type MessageType byte

const (
	FILE_INFO MessageType = iota
	READ_CONTENT
	READDIR_INFO
)

type NetInfo struct {
	NameLength int64 `struc:"int16,sizeof=NName"`
	NName      string
	NSize      int64
	NIsDir     bool
	NModTime   int64
}

func (n NetInfo) Name() string {
	return n.NName
}

func (n NetInfo) Size() int64 {
	return n.NSize
}

func (n NetInfo) Mode() fs.FileMode {
	return 0
}

func (n NetInfo) ModTime() time.Time {
	return time.Unix(n.NModTime, 0)
}

func (n NetInfo) IsDir() bool {
	return n.NIsDir
}

func (n NetInfo) Sys() any {
	return nil
}

func NewNetInfo(info fs.FileInfo) *NetInfo {
	return &NetInfo{
		NName:    info.Name(),
		NSize:    info.Size(),
		NIsDir:   info.IsDir(),
		NModTime: info.ModTime().Unix(),
	}
}

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

func NewDirInfo(infos []fs.FileInfo) *DirInfo {
	resp := &DirInfo{Infos: make([]fs.FileInfo, len(infos))}
	for i, info := range infos {
		resp.Infos[i] = NewNetInfo(info)
	}
	return resp
}

type DirInfo struct {
	Infos []fs.FileInfo
}

type sliceRW struct {
	buf []byte
}

func (s sliceRW) Write(p []byte) (n int, err error) {
	copy(s.buf, p)
	s.buf = s.buf[len(p):]
	return len(p), nil
}

func (s sliceRW) Read(p []byte) (n int, err error) {
	copy(p, s.buf)
	s.buf = s.buf[len(p):]
	return len(p), nil
}

func (d *DirInfo) Marshal(writer io.Writer) error {
	err := binary.Write(writer, binary.LittleEndian, int32(len(d.Infos)))
	if err != nil {
		return err
	}
	for _, info := range d.Infos {
		netInfo := NewNetInfo(info)
		err := struc.Pack(writer, &netInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DirInfo) Unmarshal(reader io.Reader) error {
	var infoLen int32
	err := binary.Read(reader, binary.LittleEndian, &infoLen)
	if err != nil {
		return err
	}
	d.Infos = make([]fs.FileInfo, infoLen)
	for i := int32(0); i < infoLen; i++ {
		var info NetInfo
		err := struc.Unpack(reader, &info)
		if err != nil {
			return err
		}
		d.Infos[i] = info
	}
	return nil
}

type LocalPath string

func (l LocalPath) Append(name string) LocalPath {
	return LocalPath(string(l) + "/" + name)
}

func (l LocalPath) String() string {
	return string(l)
}
