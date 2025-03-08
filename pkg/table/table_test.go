package table_test

import (
	"path/filepath"
	"testing"

	"github.com/DerGut/zomdb/pkg/table"
)

func TestTable_New(t *testing.T) {
	spec := table.Spec{
		Name: filepath.Join(t.TempDir(), "test"),
		Columns: []table.Column{
			{Name: "id", Type: table.ColumnTypeString, PrimaryKey: true},
			{Name: "name", Type: table.ColumnTypeString},
			{Name: "amount", Type: table.ColumnTypeInt64},
		},
	}

	tbl, err := table.New(spec)
	if err != nil {
		t.Fatal("new table", err)
	}

	for i, row := range [][]any{
		{"id1", "foo", 3},
		{"id2", "bar", 16},
		{"id3", "baz", 39},
	} {
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("insert row %d: %v\n", i, err)
		}
	}

	row, err := tbl.Select([]table.Predicate{
		{ColumnName: "id", Value: "id2"},
	})
	if err != nil {
		t.Fatalf("Select row: %v\n", err)
	}

	if len(row) != 3 {
		t.Fatalf("Expected 3 column values, got %d", len(row))
	}

	id, ok := row[0].(string)
	if !ok {
		t.Fatalf("Expected string type, got %T", row[0])
	}

	if id != "id2" {
		t.Fatalf("Expected %q, got %q", "id2", id)
	}

	name, ok := row[1].(string)
	if !ok {
		t.Fatalf("Expected string type, got %T", row[1])
	}

	if name != "bar" {
		t.Fatalf("Expected %q, got %q", "bar", name)
	}
}
