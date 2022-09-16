package index

import "errors"

var ErrNotFound = errors.New("not found")

type Index interface {
	PutOffset(key []byte, off int64) error
	GetOffset(key []byte) (int64, error)
}
