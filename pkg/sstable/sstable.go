package sstable

import (
	"bufio"
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"time"

	"github.com/DerGut/zomdb/pkg/memtable"
	"github.com/spf13/afero"
)

var (
	MaxKeySize = math.MaxUint16
	MaxValSize = math.MaxUint32
)

var (
	fs      = afero.NewOsFs()
	timeSrc = func() time.Time { return time.Now() }
)

// SSTable is an immutable structure of string sorted data
type SSTable struct {
	file afero.File
}

// TODO: this needs some fs/ timeSrc
func FromMemtable(mem *memtable.MemTable) (*SSTable, error) {
	return nil, nil
}

// Compact creates a new immutable SSTable, and writes the result
// of the compaction job there
func (t *SSTable) Compact() (*SSTable, error) {
	return compactFromReader(t.file)
}

func Merge(a, b *SSTable) (*SSTable, error) {
	r := io.MultiReader(a.file, b.file)

	return compactFromReader(r)
}

func compactFromReader(r io.Reader) (*SSTable, error) {
	res, err := compact(r)
	if err != nil {
		return nil, fmt.Errorf("compact: %w", err)
	}

	t, err := newFromReader(res)
	if err != nil {
		return nil, fmt.Errorf("new from reader: %w", err)
	}

	return t, nil
}

func compact(r io.Reader) (*bytes.Buffer, error) {
	entries, err := parseBuffered(r)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	compacted := compactEntries(entries)

	var out bytes.Buffer
	for i := range compacted {
		data, err := compacted[i].MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}

		if _, err := out.Write(data); err != nil {
			return nil, fmt.Errorf("write: %w", err)
		}
	}

	return &out, nil
}

func parseEntries(r io.Reader) ([]entry, error) {
	sizeBuf := make([]byte, 6)

	// TODO: use statistics to estimate entries size
	var entries []entry
	for {
		n, err := r.Read(sizeBuf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read: %w", err)
		}

		if n == 0 {
			break
		}

		if n < 6 {
			return nil, errors.New("n < 6")
		}

		keySize := binary.BigEndian.Uint16(sizeBuf[:2])
		valSize := binary.BigEndian.Uint32(sizeBuf[2:])

		// TODO: Preallocate buffer
		key := make([]byte, keySize)
		val := make([]byte, valSize)

		if _, err := r.Read(key); err != nil {
			return nil, fmt.Errorf("read key: %w", err)
		}

		if _, err := r.Read(val); err != nil {
			return nil, fmt.Errorf("read val: %w", err)
		}

		entries = append(entries, entry{
			key:   key,
			value: val,
		})
	}

	return entries, nil
}

func parseBuffered(r io.Reader) ([]entry, error) {
	br := bufio.NewReader(r)

	buf := make([]byte, br.Size())

	var entries []entry
	var overflow []byte

	// Loop over entire file, filling buffer
	for {
		n, err := br.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read: %w", err)
		}

		if n == 0 && len(overflow) == 0 {
			break
		}

		if n == 0 && len(overflow) > 0 {
			return nil, errors.New("file is corrupt")
		}

		// Merge overflow from last iteration and buffer
		// TODO: buffer size changes in size -> we read different window sizes
		if len(overflow) > 0 {
			buf = append(overflow, buf[:n]...)
		} else {
			buf = buf[:n]
		}

		// Loop over entire buffer, parsing entries
		var e entry
		for off := 0; off < len(buf); {
			if err := e.UnmarshalBinary(buf[off:]); err != nil {
				// Not enough bytes to unmarshal, add to overflow
				// and read next buffer window
				overflow = append(overflow, buf[off:]...)
				break
			}

			off += 6 + len(e.key) + len(e.value)
			entries = append(entries, e)
			overflow = nil
		}
	}

	return entries, nil
}

func compactEntries(in []entry) []entry {
	sort.Slice(in, func(i, j int) bool {
		return bytes.Compare(in[i].key, in[j].key) < 0
	})

	var out []entry
	var previousKey []byte
	for i := range in {
		if bytes.Equal(in[i].key, previousKey) {
			continue
		}

		previousKey = in[i].key
		out = append(out, in[i])
	}

	return out
}

func compactBuffered(r io.Reader) (*bytes.Buffer, error) {
	br := bufio.NewReader(r)

	buf := make([]byte, br.Size())

	// TODO: use statistics to estimate map size
	m := make(map[string][]byte)
	var keys [][]byte

	var overflow []byte
	for {
		n, err := br.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read: %w", err)
		}

		if n == 0 {
			break
		}

		if len(overflow) > 0 {
			// TODO parse key/ valSize from there?
			// keySize := binary.BigEndian.Uint16(overflow[:2])
			// valSize := binary.BigEndian.Uint32(overflow[2:6])
			// // entrySize := 6 + uint64(keySize) + uint64(valSize)
		}

		for off := uint64(0); off < uint64(n); {
			windowSize := uint64(n) - off
			if windowSize < 6 {
				// TODO we should be able to handle this
				return nil, errors.New("n < 6")
			}

			keySize := binary.BigEndian.Uint16(buf[off : off+2])
			valSize := binary.BigEndian.Uint32(buf[off+2 : off+6])
			entrySize := 6 + uint64(keySize) + uint64(valSize)

			if windowSize < entrySize {
				// TODO: save vars and wait for next buffer window
				overflow = append(overflow, buf[off:]...)
				break
			}

			e := entry{
				key:   make([]byte, keySize),
				value: make([]byte, valSize),
			}

			copy(e.key, buf[6:6+keySize])
			copy(e.value, buf[6+keySize:entrySize])

			m[string(e.key)] = e.value
			keys = append(keys, e.key)

			off += entrySize
			overflow = nil
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	var out bytes.Buffer
	var e entry
	for _, key := range keys {
		e = entry{key: key, value: m[string(key)]}
		data, err := e.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}

		if _, err := out.Write(data); err != nil {
			return nil, fmt.Errorf("write: %w", err)
		}
	}

	return &out, nil
}

func newFromReader(r io.Reader) (*SSTable, error) {
	name := newFilename()

	f, err := newFile(name)
	if err != nil {
		return nil, fmt.Errorf("new file: %w", err)
	}

	if err := writeFile(f, r); err != nil {
		return nil, fmt.Errorf("write file: %w", err)
	}

	return &SSTable{
		file: f,
	}, nil
}

func newFilename() string {
	now := timeSrc()
	return now.Format(time.RFC3339)
}

func newFile(name string) (afero.File, error) {
	f, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0655)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return f, nil
}

func writeFile(f afero.File, r io.Reader) error {
	if err := writeBuffered(f, r); err != nil {
		return fmt.Errorf("write buffered: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	return nil
}

func writeBuffered(w io.Writer, r io.Reader) error {
	buf := bufio.NewWriter(w)

	if err := writeAll(buf, r, buf.Size()); err != nil {
		return fmt.Errorf("writeAll: %w", err)
	}

	if err := buf.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

func writeAll(w io.Writer, r io.Reader, bufSize int) error {
	buf := make([]byte, bufSize)

	for {
		n, err := r.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read: %w", err)
		}

		if n == 0 {
			break
		}

		if _, err := w.Write(buf[:n]); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}

	return nil
}

type entry struct {
	key, value []byte
}

var _ encoding.BinaryMarshaler = &entry{}
var _ encoding.BinaryUnmarshaler = &entry{}

func (e *entry) MarshalBinary() (data []byte, err error) {
	if len(e.key) > MaxKeySize {
		return nil, errors.New("len(key) > MaxKeySize")
	}

	if len(e.value) > MaxValSize {
		return nil, errors.New("len(value) > MaxValSize")
	}

	data = make([]byte, 6+len(e.key)+len(e.value))

	binary.BigEndian.PutUint16(data[:2], uint16(len(e.key)))
	binary.BigEndian.PutUint32(data[2:6], uint32(len(e.value)))
	copy(data[6:6+len(e.key)], e.key)
	copy(data[6+len(e.key):], e.value)

	return data, nil
}

func (e *entry) UnmarshalBinary(data []byte) error {
	if len(data) < 6 {
		return errors.New("len(data) < 6")
	}

	keySize := binary.BigEndian.Uint16(data[:2])
	valSize := binary.BigEndian.Uint32(data[2:6])

	if uint64(len(data)) < 6+uint64(keySize)+uint64(valSize) {
		return fmt.Errorf("len(data) < len(entry): %d < %d", len(data), 6+uint64(keySize)+uint64(valSize))
	}

	e.key = data[6 : 6+uint32(keySize)]
	e.value = data[6+keySize : 6+uint64(keySize)+uint64(valSize)]

	return nil
}
