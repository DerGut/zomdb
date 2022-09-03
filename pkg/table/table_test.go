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
			row Row
			err error
		}
	}{
		{
			name: "write -> read",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("hallo"),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("hallo"),
					},
				},
			},
		},
		{
			name: "write1 -> write2 -> write3 -> read2 -> read1",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("test1"),
					},
				},
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key2"),
						Data:       []byte("test2"),
					},
				},
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key3"),
						Data:       []byte("test3"),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: []byte("key2"),
						Data:       []byte("test2"),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("test1"),
					},
				},
			},
		},
		{
			name: "put k1 v1 -> put k1 v2 -> get k1",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("hallo"),
					},
				},
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("overwritten"),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: []byte("key1"),
						Data:       []byte("overwritten"),
					},
				},
			},
		},
		{
			name: "get k1 -> not found",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op:  get,
					row: Row{PrimaryKey: []byte("not exist")},
					err: ErrNotFound,
				},
			},
		},
		{
			name: "keySize > buffer",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: make([]byte, 3000),
						Data:       []byte("hallo"),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: make([]byte, 3000),
						Data:       []byte("hallo"),
					},
				},
			},
		},
		{
			name: "valSize > buffer",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: []byte("key"),
						Data:       make([]byte, 3000),
					},
				},
				{
					op: get,
					row: Row{
						PrimaryKey: []byte("key"),
						Data:       make([]byte, 3000),
					},
				},
			},
		},
		{
			name: "key > maxSize",
			ops: []struct {
				op  string
				row Row
				err error
			}{
				{
					op: put,
					row: Row{
						PrimaryKey: make([]byte, MaxPrimaryKeySize+1),
						Data:       []byte("hallo"),
					},
					err: ErrPrimaryKeyTooLarge,
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
					err := tbl.Put(&op.row)
					if !errors.Is(err, op.err) {
						t.Fatalf("Expected err \"%v\", got \"%v\"\n", op.err, err)
					}
				case "get":
					row, err := tbl.Get(op.row.PrimaryKey)
					if !errors.Is(err, op.err) {
						t.Fatalf("Expected err \"%v\", got \"%v\"\n", op.err, err)
					}
					if row != nil && !bytes.Equal(op.row.Data, row.Data) {
						t.Fatalf("Expected \"%s\" got \"%s\"\n", op.row.Data, row.Data)
					}
				default:
					t.Fatalf("Unknown op: %s\n", op.op)
				}
			}
		})
	}
}

func FuzzTable(f *testing.F) {
	tbl, err := New(afero.NewMemMapFs())
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, key []byte, data []byte) {
		row := Row{
			PrimaryKey: key,
			Data:       data,
		}

		if err := tbl.Put(&row); err != nil {
			t.Fatal(err)
		}

		result, err := tbl.Get(key)
		if err != nil {
			t.Fatal(err)
		}

		if bytes.Compare(data, result.Data) != 0 {
			t.Fatalf("Expected \"%s\" got \"%s\"\n", data, result.Data)
		}

		if bytes.Compare(key, result.PrimaryKey) != 0 {
			t.Fatalf("Expected \"%s\" got \"%s\"\n", key, result.PrimaryKey)
		}
	})
}
