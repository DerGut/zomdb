package heap_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/DerGut/zomdb/pkg/heap"
)

func TestHeap(t *testing.T) {
	h := newTestHeap(t)

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
	h := newTestHeap(t)

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

func TestHeapSetOverwrite(t *testing.T) {
	h := newTestHeap(t)

	if err := h.Set("color", "red"); err != nil {
		t.Fatalf("set color=red: %v", err)
	}

	if err := h.Set("color", "green"); err != nil {
		t.Fatalf("set color=green: %v", err)
	}

	value, err := h.Get("color")
	if err != nil {
		t.Fatalf("get color: %v", err)
	}

	if value != "green" {
		t.Errorf("want color=green, got: %s", value)
	}
}

func FuzzHeapSet(f *testing.F) {
	h := newTestHeap(f)

	f.Add("key", "value")
	f.Fuzz(func(t *testing.T, a string, b string) {
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
