package heap_test

import (
	"testing"

	"github.com/DerGut/zomdb/pkg/heap"
)

func TestHeap(t *testing.T) {
	h := heap.New("test")
	defer h.Close()

	if err := h.Set("key", "value"); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	value := h.Get("key")
	if value != "value" {
		t.Errorf("expected value to be \"value\", got %q", value)
	}
}
