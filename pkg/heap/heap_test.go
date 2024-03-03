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

	h.Set("key", "value")
	value := h.Get("key")

	if value != "value" {
		t.Errorf("expected value to be \"value\", got %q", value)
	}
}
