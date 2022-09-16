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

	ErrInvalidKey  = errors.New("invalid key")
	ErrInvalidData = errors.New("invalid data")
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

	if err := validateKey(primaryKey); err != nil {
		return nil, fmt.Errorf("validate key: %w", err)
	}

	buf := make([]byte, bufSize)

	var match *Row
	var off int64

	// Run a sequential scan
	for {
		n, err := t.log.ReadAt(buf, off)
		if err != nil {
			// We try to read a fixed-size buffer, so an EOF can
			// occur even though all the data we want has been read
			// successfully
			if errors.Is(err, io.EOF) && n == 0 {
				// If it occurs right away, we hit the end of the log
				break
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

		// If row didn't fit into the buffer
		// TODO: track this to optimize default buffer size
		rowSize := 8 + keySize + valSize
		if rowSize > bufSize {
			missingBufSize := rowSize - bufSize
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

		if !bytes.Equal(primaryKey, foundKey) {
			// Key mismatch
			off += 8 + int64(keySize) + int64(valSize)
			continue
		}

		// Save the match but continue through the rest of the log (most recent wins)
		row := Row{
			PrimaryKey: make([]byte, len(primaryKey)),
			Data:       make([]byte, valSize),
		}
		copy(row.PrimaryKey, primaryKey)
		copy(row.Data, buf[8+keySize:8+keySize+valSize])

		off += int64(rowSize)
		match = &row
	}

	if match == nil {
		return nil, ErrNotFound
	}

	return match, nil
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

	if err := validateKey(row.PrimaryKey); err != nil {
		return err
	}

	if len(row.Data) == 0 {
		return ErrInvalidData
	}

	if len(row.Data) > MaxDataSize {
		return fmt.Errorf("len(data) > MaxDataSize: %w", ErrInvalidData)
	}

	return nil
}

func validateKey(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}

	if len(key) > MaxPrimaryKeySize {
		return fmt.Errorf("key too large: %w", ErrInvalidKey)
	}

	return nil
}
