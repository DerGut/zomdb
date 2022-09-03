package table

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spf13/afero"
)

func TestTable(t *testing.T) {
	var (
		put = "put"
		get = "get"
	)
	tc := []struct {
		name string
		ops  []struct {
			op  string
			key []byte
			val []byte
			err error
		}
	}{
		{
			name: "write -> read",
			ops: []struct {
				op  string
				key []byte
				val []byte
				err error
			}{
				{
					op:  put,
					key: []byte("key1"),
					val: []byte("hallo"),
				},
				{
					op:  get,
					key: []byte("key1"),
					val: []byte("hallo"),
				},
			},
		},
		{
			name: "write1 -> write2 -> write3 -> read2 -> read1",
			ops: []struct {
				op  string
				key []byte
				val []byte
				err error
			}{
				{
					op:  put,
					key: []byte("key1"),
					val: []byte("test1"),
				},
				{
					op:  put,
					key: []byte("key2"),
					val: []byte("test2"),
				},
				{
					op:  put,
					key: []byte("key3"),
					val: []byte("test3"),
				},
				{
					op:  get,
					key: []byte("key2"),
					val: []byte("test2"),
				},
				{
					op:  get,
					key: []byte("key1"),
					val: []byte("test1"),
				},
			},
		},
		{
			name: "put k1 v1 -> put k1 v2 -> get k1",
			ops: []struct {
				op  string
				key []byte
				val []byte
				err error
			}{
				{
					op:  put,
					key: []byte("key1"),
					val: []byte("hallo"),
				},
				{
					op:  put,
					key: []byte("key1"),
					val: []byte("overwritten"),
				},
				{
					op:  get,
					key: []byte("key1"),
					val: []byte("overwritten"),
				},
			},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			tbl, err := New(afero.NewMemMapFs())
			if err != nil {
				t.Fatal(err)
			}

			for _, op := range tt.ops {
				switch op.op {
				case "put":
					err := tbl.Put(op.key, op.val)
					if !errors.Is(err, op.err) {
						t.Fatalf("Expected %s, got %s\n", op.err, err)
					}
				case "get":
					b, err := tbl.Get(op.key)
					if !errors.Is(err, op.err) {
						t.Fatalf("Expected %s, got %s\n", op.err, err)
					}
					if bytes.Compare(op.val, b) != 0 {
						t.Fatalf("Expected \"%s\" got \"%s\"\n", op.val, b)
					}
				default:
					t.Fatalf("Unknown op: %s\n", op.op)
				}
			}
		})
	}
}
