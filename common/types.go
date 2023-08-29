package common

type Finfo struct {
	NameLength int64 `struc:"int16,sizeof=Name"`
	Name       string
	Size       int64
	IsDir      bool
	ModTime    int64
}
