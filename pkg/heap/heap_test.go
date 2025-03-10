package heap_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/DerGut/zomdb/pkg/heap"
)

func TestHeap(t *testing.T) {
	h := newTestHeap(t)

	if err := h.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("set: expected no error, got %v", err)
	}

	value, err := h.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get: expected no error, got %v", err)
	}

	if !bytes.Equal(value, []byte("value")) {
		t.Errorf("expected value to be \"value\", got %q", value)
	}
}

func TestHeapSetAndGetMultiple(t *testing.T) {
	h := newTestHeap(t)

	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key_%d", i+1))
		value := []byte(fmt.Sprintf("value_%d", i+1))
		if err := h.Set(key, value); err != nil {
			t.Fatalf("set %d: expected no error, got %v", i+1, err)
		}
	}

	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("key_%d", i+1))
		value := []byte(fmt.Sprintf("value_%d", i+1))

		got, err := h.Get(key)
		if err != nil {
			t.Fatalf("get %d: expected no error, got %v", i+1, err)
		}

		if !bytes.Equal(got, value) {
			t.Errorf("expected value to be %q, got %q", value, got)
		}
	}
}

func TestHeapSetOverwrite(t *testing.T) {
	h := newTestHeap(t)

	if err := h.Set([]byte("color"), []byte("red")); err != nil {
		t.Fatalf("set color=red: %v", err)
	}

	if err := h.Set([]byte("color"), []byte("green")); err != nil {
		t.Fatalf("set color=green: %v", err)
	}

	value, err := h.Get([]byte("color"))
	if err != nil {
		t.Fatalf("get color: %v", err)
	}

	if !bytes.Equal(value, []byte("green")) {
		t.Errorf("want color=green, got: %s", value)
	}
}

func TestNullByte(t *testing.T) {
	h := newTestHeap(t)

	t.Run("setKey", func(t *testing.T) {
		if err := h.Set([]byte("key\x00"), []byte("value")); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("setValue", func(t *testing.T) {
		if err := h.Set([]byte("key"), []byte("value\x00")); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("get", func(t *testing.T) {
		if _, err := h.Get([]byte("key\x00")); err == nil {
			t.Error("expected error")
		}
	})
}

func TestHeapAll(t *testing.T) {
	h := newTestHeap(t)

	values := map[string]string{"1": "one", "2": "two", "3": "three"}
	for key, value := range values {
		h.Set([]byte(key), []byte(value))
	}

	for key, value := range h.All() {
		want := []byte(values[string(key)])
		if !bytes.Equal(value, want) {
			t.Errorf("expected %q, got %q", want, value)
		}
	}
}

func FuzzHeapSet(f *testing.F) {
	h := newTestHeap(f)

	f.Add([]byte("key"), []byte("value"))
	f.Fuzz(func(t *testing.T, a []byte, b []byte) {
		if err := h.Set(a, b); err != nil {
			return
		}
	})
}

func newTestHeap(t testing.TB) *heap.Heap {
	name := filepath.Join(t.TempDir(), "test.zomdb")

	h, err := heap.New(name)
	if err != nil {
		t.Fatalf("new heap: %v", err)
	}

	t.Cleanup(h.Close)

	return h
}
