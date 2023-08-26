package tdef

type Finfo struct {
	NameLength int `struc:"int16,sizeof=Name"`
	Name       string
	Size       int64
}
