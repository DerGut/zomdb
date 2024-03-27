package heap

/*
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../lib -lzomdb_darwin_arm64
#cgo CFLAGS: -I${SRCDIR}/../../include
#include "zomdb.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

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

func (h *Heap) Get(key string) (string, error) {
	ck := C.CString(key)
	defer C.free(unsafe.Pointer(ck))

	cv, errno := C.heap_get(h.heap, ck)
	if err := goErr(errno); err != nil {
		return "", err
	}

	return C.GoString(cv), nil
}

func (h *Heap) Set(key, value string) error {
	ck := C.CString(key)
	cv := C.CString(value)
	defer C.free(unsafe.Pointer(ck))
	defer C.free(unsafe.Pointer(cv))

	_, errno := C.heap_set(h.heap, ck, cv)
	if err := goErr(errno); err != nil {
		return err
	}

	return nil
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
	10: errors.New("zomdb: io error"),
	30: errors.New("zomdb: not utf8-encoded"),
}
