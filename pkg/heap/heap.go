package heap

/*
#cgo LDFLAGS: -L${SRCDIR}/../../target/debug -lzomdb
#include "../../target/zomdb.h"
*/
import "C"
import (
	"unsafe"
)

type Heap struct {
	heap *C.struct_Heap
}

func New(fileName string) *Heap {
	cs := C.CString(fileName)
	heap := C.create_heap(cs)
	C.free(unsafe.Pointer(cs))

	return &Heap{heap: heap}
}

func (h *Heap) Close() {
	C.destroy_heap(h.heap)
}

func (h *Heap) Get(key string) string {
	ck := C.CString(key)
	cv := C.heap_get(h.heap, ck)
	C.free(unsafe.Pointer(ck))
	return C.GoString(cv)
}

func (h *Heap) Set(key, value string) {
	ck := C.CString(key)
	cv := C.CString(value)

	C.heap_set(h.heap, ck, cv)

	C.free(unsafe.Pointer(ck))
	C.free(unsafe.Pointer(cv))
}
