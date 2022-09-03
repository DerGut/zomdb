package table

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/DerGut/zomdb/pkg/log"
	"github.com/spf13/afero"
)

var (
	MaxPrimaryKeySize = 2 ^ 32 - 1
	MaxDataSize       = 2 ^ 32 - 1
)

var (
	ErrNotFound    = errors.New("not found")
	ErrCorruptData = errors.New("data is corruped")

	ErrPrimaryKeyTooLarge = errors.New("primary key too large")
	ErrDataTooLarge       = errors.New("data too large")
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

func (t *Table) Put(row *Row) error {
	data, err := row.MarshalBinary()
	if err != nil {
		return fmt.Errorf("encode row: %w", err)
	}

	if _, err := t.log.Append(data); err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	return nil
}

func (t *Table) Get(primaryKey []byte) (*Row, error) {
	const bufSize = 1024

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

		if int(keySize) != len(primaryKey) {
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

		foundKey := buf[8 : 8+keySize]

		if bytes.Compare(primaryKey, foundKey) != 0 {
			// Key mismatch
			off += int64(keySize) + int64(valSize) + 8
			continue
		}

		return &Row{
			PrimaryKey: primaryKey,
			Data:       buf[8+keySize : 8+keySize+valSize],
		}, nil
	}
}

type Row struct {
	PrimaryKey []byte
	Data       []byte
}

var _ encoding.BinaryMarshaler = &Row{}

func (r *Row) MarshalBinary() ([]byte, error) {
	if err := ValidateRow(r); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	keySize := len(r.PrimaryKey)
	valSize := len(r.Data)

	data := make([]byte, keySize+valSize+8)

	// Encode row
	binary.BigEndian.PutUint32(data[:4], uint32(keySize))
	binary.BigEndian.PutUint32(data[4:8], uint32(valSize))
	copy(data[8:8+keySize], r.PrimaryKey)
	copy(data[8+keySize:], r.Data)

	return data, nil
}

func ValidateRow(row *Row) error {
	if row == nil {
		return errors.New("nil row")
	}

	if len(row.PrimaryKey) > MaxPrimaryKeySize {
		return ErrPrimaryKeyTooLarge
	}

	if len(row.Data) > MaxDataSize {
		return ErrDataTooLarge
	}

	return nil
}
