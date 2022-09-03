package table

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/DerGut/zomdb/pkg/log"
	"github.com/spf13/afero"
)

var (
	MaxPrimaryKeySize = math.MaxUint32
	MaxDataSize       = math.MaxUint32
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
		n, err := t.log.ReadAt(buf, off)
		if err != nil {
			// We try to read a fixed-size buffer, so an EOF can also
			// occur after the bytes we are interested in
			// If it occurs right away, we hit the end of the log
			if errors.Is(err, io.EOF) && n == 0 {
				return nil, ErrNotFound
			}

			return nil, fmt.Errorf("read at %d: %w", off, err)
		}

		// At minimum, we encode two uint32 size values for each key-value pair
		if n < 8 {
			return nil, fmt.Errorf("row doesn't fit sizes: %d: %w", off, ErrCorruptData)
		}

		keySize := binary.BigEndian.Uint32(buf[:4])
		valSize := binary.BigEndian.Uint32(buf[4:8])

		if int(keySize) != len(primaryKey) {
			// If the key size doesn't match, we already know that this
			// isn't the right value
			off += 8 + int64(keySize) + int64(valSize)
			continue
		}

		if n < int(8+keySize) {
			// We reached EOF before reading the entire key
			return nil, fmt.Errorf("row doesn't fit key: %d: %w", off, ErrCorruptData)
		}

		// If row didn't fit into the buffer
		// TODO: track this to optimize default buffer size
		rowSize := 8 + keySize + valSize
		if rowSize > bufSize {
			missingBufSize := rowSize - bufSize
			extdBuf := make([]byte, missingBufSize)

			// Start at current offset - len of first buffer
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
			off += 8 + int64(keySize) + int64(valSize)
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
