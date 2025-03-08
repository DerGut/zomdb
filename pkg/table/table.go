package table

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/DerGut/zomdb/pkg/heap"
)

type Table struct {
	heap *heap.Heap

	columns []Column
	pkIdxs  []int
}

func New(spec Spec) (*Table, error) {
	var primaryKeys []int
	for i, col := range spec.Columns {
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, i)
		}
	}

	if len(primaryKeys) == 0 {
		return nil, errors.New("no primary key defined")
	}

	h, err := heap.New(spec.Name)
	if err != nil {
		return nil, fmt.Errorf("new heap: %w", err)
	}

	return &Table{
		heap:    h,
		columns: spec.Columns,
		pkIdxs:  primaryKeys,
	}, nil
}

type Spec struct {
	Name    string
	Columns []Column
}

type Column struct {
	Name       string
	Type       ColumnType
	PrimaryKey bool
}

func (c Column) String() string {
	return c.Name
}

type ColumnType int

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeInt64
)

func (t *Table) Insert(values []any) error {
	if len(values) != len(t.columns) {
		// We don't yet support nullable values.
		return fmt.Errorf("must pass no. of values equal to no. of columns, passed: %d", len(values))
	}

	for i := range values {
		if err := validateColumnType(values[i], t.columns[i].Type); err != nil {
			return fmt.Errorf("column %s: %w", t.columns[i], err)
		}
	}

	key, err := t.buildKey(values)
	if err != nil {
		return fmt.Errorf("build key: %w", err)
	}

	value, err := encode(values)
	if err != nil {
		return fmt.Errorf("build value: %w", err)
	}

	return t.heap.Set(key, value)
}

// Select retrieves a single row from the table.
//
// TODO: Don't assume predicates are ANDed.
func (t *Table) Select(where []Predicate) ([]any, error) {
	if pks, ok := t.primaryKeysFromPredicates(where); ok {
		row, err := t.indexScan(pks)
		if err != nil {
			return nil, fmt.Errorf("index scan: %w", err)
		}

		return row, nil
	}

	row, err := t.sequentialScan(where)
	if err != nil {
		return nil, fmt.Errorf("sequential scan: %w", err)
	}

	return row, nil
}

func (t *Table) primaryKeysFromPredicates(predicates []Predicate) ([]any, bool) {
	pks := make([]any, 0, len(t.pkIdxs))
	for _, idx := range t.pkIdxs {
		for _, predicate := range predicates {
			if t.columns[idx].Name == predicate.ColumnName {
				pks = append(pks, predicate.Value)
			}
		}
	}

	if len(pks) != len(t.pkIdxs) {
		// We don't have predicates for all primary keys.
		return nil, false
	}

	return pks, true
}

func (t *Table) indexScan(pks []any) ([]any, error) {
	key, err := encode(pks)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}

	b, err := t.heap.Get(key)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}

	row, err := decode(b)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (t *Table) sequentialScan(where []Predicate) ([]any, error) {
	for key, value := range t.heap.All() {
		_, _ = key, value
	}
	return nil, errors.New("not implemented")
}

// Predicate matches which row's column value equals the given value.
//
// TODO: Support more operators to be able to return multiple results from
// select.
type Predicate struct {
	ColumnName string
	Value      any
}

func (t *Table) buildKey(values []any) ([]byte, error) {
	key := make([]any, len(t.pkIdxs))
	for i, idx := range t.pkIdxs {
		key[i] = values[idx]
	}

	return encode(key)
}

func encode(a []any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(a); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func decode(p []byte) ([]any, error) {
	r := bytes.NewReader(p)

	var v []any
	if err := gob.NewDecoder(r).Decode(&v); err != nil {
		return nil, err
	}

	return v, nil
}

func validateColumnType(value any, colType ColumnType) error {
	switch colType {
	case ColumnTypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string value, received %T", value)
		}
	case ColumnTypeInt64:
		switch value.(type) {
		case int64, int:
			return nil
		default:
			return fmt.Errorf("expected int64 value, received %T", value)
		}

	default:
		return fmt.Errorf("unsupported type %T", value)
	}

	return nil
}
