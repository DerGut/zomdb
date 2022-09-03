package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/DerGut/zomdb/pkg/log"
	"github.com/spf13/afero"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrCorruptData = errors.New("data is corruped")
)

type Table struct {
	log *log.Log
}

func New(fs afero.Fs) (*Table, error) {
	l, err := log.New(fs)
	if err != nil {
		return nil, fmt.Errorf("new log: %w", err)
	}

	return &Table{
		log: l,
	}, nil
}

// Data layout:
// uint32 keySize
// uint32 valSize
// varbyte key
// avrbyte val
func (t *Table) Put(key, value []byte) error {
	data := make([]byte, len(key)+len(value)+8)

	// Encode key value
	binary.BigEndian.PutUint32(data[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(data[4:8], uint32(len(value)))
	copy(data[8:8+len(key)], key)
	copy(data[8+len(key):], value)

	if err := binary.Write(t.log, binary.BigEndian, data); err != nil {
		return fmt.Errorf("write to log: %w", err)
	}

	return nil
}

const bufSize = 1024

func (t *Table) Get(key []byte) ([]byte, error) {
	buf := make([]byte, bufSize)

	// Run a sequential scan
	var off int64
	for {
		n, err := t.log.ReadAt(buf, int64(off))
		if err != nil {
			// We try to read a fixed-size buffer, so an EOF can also
			// occur after the value we want to retrieve
			if !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("read at %d: %w", off, err)
			}
		}

		// At minimum, we encode two uint32 size values for each key-value pair
		if n < 8 {
			return nil, ErrCorruptData
		}

		keySize := binary.BigEndian.Uint32(buf[:4])
		valSize := binary.BigEndian.Uint32(buf[4:8])

		if int(keySize) != len(key) {
			// If the key size doesn't match, we already know that this
			// isn't the right value
			off += int64(keySize) + int64(valSize) + 8
			continue
		}

		if n < int(8+keySize) {
			// We reached EOF before reading the entire key
			return nil, ErrCorruptData
		}

		// key doesn't fit into current buffer
		if keySize > bufSize-8 {
			// new buffer includes the full length of the key and val
			missingBufSize := (keySize + valSize) - bufSize + 8
			extdBuf := make([]byte, missingBufSize)

			// Start at current offset + len of first buffer
			newOff := off + bufSize
			if _, err := t.log.ReadAt(extdBuf, newOff); err != nil {
				if errors.Is(err, io.EOF) {
					// This time we know that the key+value should fit
					return nil, ErrCorruptData
				}

				return nil, fmt.Errorf("read extended buffer: %w", err)
			}

			buf = append(buf, extdBuf...)
		}

		curKey := buf[8 : 8+keySize]

		if bytes.Compare(key, curKey) != 0 {
			// Key mismatch
			off += int64(keySize) + int64(valSize) + 8
			continue
		}

		// Key match
		value := buf[8+keySize : 8+keySize+valSize]

		return value, nil
	}
}
