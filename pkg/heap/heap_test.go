package heap_test

import (
	"fmt"
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

func TestHeapSetAndGetMultiple(t *testing.T) {
	name := filepath.Join(t.TempDir(), "test.zomdb")

	h, err := heap.New(name)
	if err != nil {
		t.Fatalf("new: expected no error, got: %v", err)
	}
	defer h.Close()

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key_%d", i+1)
		value := fmt.Sprintf("value_%d", i+1)
		if err := h.Set(key, value); err != nil {
			t.Fatalf("set %d: expected no error, got %v", i+1, err)
		}
	}

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("key_%d", i+1)
		value := fmt.Sprintf("value_%d", i+1)

		got, err := h.Get(key)
		if err != nil {
			t.Fatalf("get %d: expected no error, got %v", i+1, err)
		}

		if got != value {
			t.Errorf("expected value to be %q, got %q", value, got)
		}
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
