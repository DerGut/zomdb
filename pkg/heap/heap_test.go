package heap_test

import (
	"path/filepath"
	"testing"

	"github.com/DerGut/zomdb/pkg/heap"
)

func TestHeap(t *testing.T) {
	name := filepath.Join(t.TempDir(), "test.zomdb")

	h, err := heap.New(name)
	if err != nil {
		t.Fatalf("new: expected no error, got %v", err)
	}
	defer h.Close()

	if err := h.Set("key", "value"); err != nil {
		t.Fatalf("set: expected no error, got %v", err)
	}

	value, err := h.Get("key")
	if err != nil {
		t.Fatalf("get: expected no error, got %v", err)
	}

	if value != "value" {
		t.Errorf("expected value to be \"value\", got %q", value)
	}
}

func FuzzHeapSet(f *testing.F) {
	name := filepath.Join(f.TempDir(), "test.zomdb")

	h, err := heap.New(name)
	if err != nil {
		f.Fatalf("new: expected no error, got %v", err)
	}
	defer h.Close()

	f.Add("key", "value")
	f.Fuzz(func(t *testing.T, a string, b string) {
		if err := h.Set(a, b); err != nil {
			return
		}
	})
}
