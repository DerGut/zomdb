package zomdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/DerGut/zomdb/pkg/heap"
)

type DB struct {
	heap *heap.Heap
}

func New() (*DB, error) {
	name := filepath.Join(os.TempDir(), "heap.zomdb")

	h, err := heap.New(name)
	if err != nil {
		return nil, fmt.Errorf("creating heap: %w", err)
	}

	return &DB{heap: h}, nil
}

func (d *DB) Close() error {
	d.heap.Close()
	return nil
}

func (d *DB) Get(_ context.Context, key []byte) ([]byte, error) {
	return d.heap.Get(key)
}

func (d *DB) Set(_ context.Context, key []byte, value []byte) error {
	return d.heap.Set(key, value)
}
