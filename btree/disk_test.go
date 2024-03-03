package btree

import (
	"context"
	"testing"
)

func TestGetPageSize(t *testing.T) {
	pageSize, err := getPageSize(context.Background())
	if err != nil {
		t.Fatalf("got error: %v", err)
	}

	if pageSize == 0 {
		t.Fatal("got 0 pageSize, want > 0")
	}
}
