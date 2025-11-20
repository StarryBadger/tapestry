package util

type IntSet map[int]struct{}

type BackPointerTable struct {
	Set IntSet
}

func NewBackPointerTable() *BackPointerTable {
	return &BackPointerTable{
		Set: make(IntSet),
	}
}