package sstable

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	"github.com/spf13/afero"
)

func BenchmarkCompactInMem(b *testing.B) {
	const tableSize = 10000
	const keySize = 50
	const valSize = 1024

	tmpDir := b.TempDir()

	// TODO: This uses the global fs var
	fs = afero.NewBasePathFs(afero.NewOsFs(), tmpDir)

	rnd := rand.New(rand.NewSource(10))

	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		filename := fmt.Sprintf("sstable-%d", i)
		err := prepareFile(filename, rnd, keySize, valSize, tableSize)
		if err != nil {
			b.Fatal(err)
		}

		f, err := fs.Open(filename)
		if err != nil {
			b.Fatal(err)
		}

		sst := SSTable{file: f}

		b.StartTimer()

		result, err := sst.Compact()
		if err != nil {
			b.Fatal(err)
		}

		if err := result.file.Sync(); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()

		f.Close()
		result.file.Close()
	}
}

func prepareFile(name string, rnd *rand.Rand, keySize, valSize, tableSize int) error {
	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0655)
	if err != nil {
		return err
	}
	defer f.Close()

	e := entry{
		key:   make([]byte, keySize),
		value: make([]byte, valSize),
	}

	if _, err := rnd.Read(e.key); err != nil {
		return err
	}

	if _, err := rnd.Read(e.value); err != nil {
		return err
	}

	data, err := e.MarshalBinary()
	if err != nil {
		return err
	}

	for i := 0; i < tableSize; i++ {
		if _, err := f.Write(data); err != nil {
			return err
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

func FuzzEntry(f *testing.F) {
	f.Fuzz(func(t *testing.T, key, value []byte) {
		e := entry{key, value}

		data, err := e.MarshalBinary()
		if err != nil {
			if data != nil {
				t.Fatal()
			}

			return
		}

		b := entry{}
		if err := b.UnmarshalBinary(data); err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(key, b.key) {
			t.Fatal()
		}

		if !bytes.Equal(value, b.value) {
			t.Fatal()
		}
	})
}

func TestParseEntries(t *testing.T) {
	tc := []struct {
		name    string
		entries []entry
	}{
		{
			name: "single entry",
			entries: []entry{
				{key: []byte("hallo"), value: []byte("lalala")},
			},
		},
		{
			name: "three entries",
			entries: []entry{
				{key: []byte("ha"), value: []byte("aso")},
				{key: []byte("scha"), value: []byte("dara")},
				{key: []byte("fananana"), value: []byte("1234")},
			},
		},
		{
			name: "unicode",
			entries: []entry{
				{key: []byte("😄"), value: []byte("🍗3🚀")},
			},
		},
		{
			name: "empty key",
			entries: []entry{
				{value: []byte("hallo")},
			},
		},
		{
			name: "empty value",
			entries: []entry{
				{key: []byte("byeeee")},
			},
		},
		{
			name: "more entries than buffer",
			entries: []entry{
				{
					key:   make([]byte, 1000),
					value: make([]byte, 1000),
				},
				{
					key:   make([]byte, 1000),
					value: make([]byte, 1000),
				},
				{
					key:   make([]byte, 1000),
					value: make([]byte, 1000),
				},
			},
		},
		{
			name: "key is longer than buffer",
			entries: []entry{
				{
					key:   make([]byte, 5000),
					value: []byte("yep, I'm the value"),
				},
			},
		},
		{
			name: "value is longer than buffer",
			entries: []entry{
				{
					key:   []byte("Pustekuchen"),
					value: make([]byte, 5000),
				},
			},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			var buf1, buf2 bytes.Buffer

			w := io.MultiWriter(&buf1, &buf2)

			for _, e := range tt.entries {
				b, err := e.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}

				if _, err := w.Write(b); err != nil {
					t.Fatal(err)
				}
			}

			t.Run("parseEntries", func(t *testing.T) {
				entries, err := parseEntries(&buf1)
				if err != nil {
					t.Fatal(err)
				}

				compareEntries(t, tt.entries, entries)
			})

			t.Run("parseBuffered", func(t *testing.T) {
				entries, err := parseBuffered(&buf2)
				if err != nil {
					t.Fatal(err)
				}

				compareEntries(t, tt.entries, entries)
			})
		})
	}

	t.Fail()
}

func compareEntries(t *testing.T, expected, actual []entry) {
	if len(expected) != len(actual) {
		t.Fatalf("len(expected) != len(actual): %d != %d\n", len(expected), len(actual))
	}

	for i := range expected {
		if !bytes.Equal(expected[i].key, actual[i].key) {
			t.Fatalf("entries[%d]: expected.key != actual.key: %s != %s\n", i, expected[i].key, actual[i].key)
		}

		if !bytes.Equal(expected[i].value, actual[i].value) {
			t.Fatalf("entries[%d]: expected.value != actual.value: %s != %s\n", i, expected[i].value, actual[i].value)
		}
	}
}
