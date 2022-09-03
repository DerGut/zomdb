package log

import (
	"testing"

	"github.com/spf13/afero"
)

func TestLog(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()

	log, err := New(fs)
	if err != nil {
		t.Fatal(err)
	}

	row1 := "hallo ballo"
	row2 := "lullu schlullu"

	offset1, err := log.Append([]byte(row1))
	if err != nil {
		t.Fatal(err)
	}

	offset2, err := log.Append([]byte(row2))
	if err != nil {
		t.Fatal(err)
	}

	buf1 := make([]byte, len([]byte(row1)))
	if _, err := log.ReadAt(buf1, int64(offset1)); err != nil {
		t.Fatal(err)
	}

	if string(buf1) != row1 {
		t.Fatalf("expected %s, got %s", row1, buf1)
	}

	buf2 := make([]byte, len([]byte(row2)))
	if _, err := log.ReadAt(buf2, int64(offset2)); err != nil {
		t.Fatal(err)
	}

	if _, err := log.ReadAt(buf2, int64(offset2)); err != nil {
		t.Fatal(err)
	}

	if string(buf2) != row2 {
		t.Fatalf("expected %s, got %s", row2, buf2)
	}
}
