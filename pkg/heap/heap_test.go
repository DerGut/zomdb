package heap_test

import (
	"path/filepath"
	"testing"

	"github.com/DerGut/zomdb/pkg/heap"
)

func TestHeap(t *testing.T) {
	name := filepath.Join(t.TempDir(), "test.zomdb")

	h := heap.New(name)
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
