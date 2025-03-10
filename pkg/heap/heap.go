package heap

/*
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../lib -lzomdb_darwin_arm64
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../lib -lzomdb_linux_arm64
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../../lib -lzomdb_linux_amd64
#cgo CFLAGS: -I${SRCDIR}/../../include
#include "zomdb.h"
*/
import "C"
import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"syscall"
	"unsafe"
)

// Heap is an append-only log of key-value pairs.
//
// A Heap has the following limitations around key/ value choices:
//   - Keys and values must be at least 1 byte in size
//   - Keys must be at most 256 bytes in size
//   - Values must be at most 1024 bytes in size
//   - Keys and values must not contain null bytes (this is a current
//     limitation based on the fact, that the C API does not pass around byte
//     array lengths)
type Heap struct {
	heap *C.struct_Heap
}

func New(fileName string) (*Heap, error) {
	cs := C.CString(fileName)
	defer C.free(unsafe.Pointer(cs))

	heap, errno := C.create_heap(cs)
	if err := goErr(errno); err != nil {
		return nil, err
	}

	return &Heap{heap: heap}, nil
}

func (h *Heap) Close() {
	C.destroy_heap(h.heap)
}

func (h *Heap) Get(key []byte) ([]byte, error) {
	if bytes.Contains(key, []byte{0}) {
		return nil, errors.New("key contains null byte")
	}

	ck := C.CString(string(key))
	defer C.free(unsafe.Pointer(ck))

	cv, errno := C.heap_get(h.heap, ck)
	if err := goErr(errno); err != nil {
		return nil, err
	}

	return []byte(C.GoString(cv)), nil
}

func (h *Heap) Set(key, value []byte) error {
	switch {
	case bytes.Contains(key, []byte{0}):
		return errors.New("key contains null byte")
	case bytes.Contains(value, []byte{0}):
		return errors.New("value contains null byte")
	}

	ck := C.CString(string(key))
	cv := C.CString(string(value))
	defer C.free(unsafe.Pointer(ck))
	defer C.free(unsafe.Pointer(cv))

	_, errno := C.heap_set(h.heap, ck, cv)
	if err := goErr(errno); err != nil {
		return err
	}

	return nil
}

// All returns an iterator over all values of the heap.
//
// Yielded values are ordered in reverse insertion order.
func (h *Heap) All() iter.Seq2[[]byte, []byte] {
	return func(yield func(k, v []byte) bool) {
		iter := C.heap_iter(h.heap)
		defer C.heap_iter_destroy(iter)

		for {
			tuple, errno := C.heap_iter_next(iter)
			if err := goErr(errno); err != nil {
				panic(err)
			}

			if tuple == nil {
				// No more values.
				return
			}

			goKey := []byte(C.GoString(tuple.key))
			goValue := []byte(C.GoString(tuple.value))

			if !yield(goKey, goValue) {
				return
			}
		}
	}
}

func goErr(err error) error {
	if err == nil {
		return nil
	}

	errno, ok := err.(syscall.Errno)
	if !ok {
		panic(fmt.Sprintf("goErr called with non-cgo err: %v", err))
	}

	if errno == 0 { // no error
		return nil
	}

	if int(errno) >= len(errnos) {
		return fmt.Errorf("unexpected errno: %d", errno)
	}

	if err := errnos[errno]; err != nil {
		return err
	}

	// We expect the errnos array to be exhaustive.
	return fmt.Errorf("unexpected errno: %d", errno)
}

var errnos = [...]error{
	1:  errors.New("zomdb: not found"),
	10: errors.New("zomdb: io error"),
	30: errors.New("zomdb: not utf8-encoded"),
	31: errors.New("zomdb: invalid key size"),
	32: errors.New("zomdb: invalid value size"),
	50: errors.New("zomdb: corrupt data"),
}
