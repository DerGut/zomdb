package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/afero"
)

const (
	defaultLogDir = "/etc/zomdb/logs"
)

var ErrNotFound = errors.New("not found")

var errNoNew = errors.New("use log.New() to create a Log")

// Log abstracts a log that is split into multiple files
// It provides an API to read from any point but only append
// to the latest entry.
// It implements the io.Closer, io.ReaderAt and io.Writer interfaces
// TODO: think about concurrent operations, the Log SHOULD be safe
// for concurrent use
type Log struct {
	fs afero.Fs

	size int64

	segments []segment
	lock     sync.Mutex
}

type segment struct {
	startOff int64
	file     afero.File
}

var _ io.Closer = &Log{}

var _ io.ReaderAt = &Log{}

var _ io.Writer = &Log{}

func New(fs afero.Fs) (*Log, error) {
	l := Log{
		fs: fs,
	}

	if err := l.rotate(); err != nil {
		return nil, fmt.Errorf("initial rotate: %w", err)
	}

	return &l, nil
}

func (l *Log) ReadAt(b []byte, off int64) (int, error) {
	if len(l.segments) == 0 {
		return 0, errNoNew
	}

	if off > l.size {
		return 0, fmt.Errorf("no segment with offset: %w", io.EOF)
	}

	idx, err := seekSegment(l.segments, off)
	if err != nil {
		return 0, fmt.Errorf("seek segment: %w", err)
	}

	segmentOff := off - l.segments[idx].startOff

	n, err := l.segments[idx].file.ReadAt(b, segmentOff)
	if err != nil {
		return n, fmt.Errorf("readAt latest segment: %w", err)
	}

	return n, nil
}

func (l *Log) Write(p []byte) (int, error) {
	if len(l.segments) == 0 {
		return 0, errNoNew
	}

	n, err := l.segments[0].file.Write(p)
	if err != nil {
		// Return with error without incrementing the offset
		// this way, the next write will overwrite the corrupted data
		// TODO: no it won't the disk is still seeked to this pointed
		return n, fmt.Errorf("write: %w", err)
	}

	l.size += int64(n)

	return n, nil
}

// Append appends the content to the most recent log file and
// returns the updated current offset on success.
func (l *Log) Append(content []byte) (off int64, err error) {
	n, err := l.Write(content)
	if err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}

	return l.size - int64(n), nil
}

func (l *Log) Close() error {
	var lastErr error

	for _, s := range l.segments {
		if err := s.file.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

func (l *Log) rotate() error {
	name := filename(time.Now())

	f, err := l.fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0655)
	if err != nil {
		return fmt.Errorf("open new file: %w", err)
	}

	// Prepend new segment
	l.segments = append([]segment{{
		startOff: l.size,
		file:     f,
	}}, l.segments...)

	return nil
}

func seekSegment(segments []segment, off int64) (idx int, err error) {
	idx = -1

	// Search for segment that contains the offset
	for i, s := range segments {
		if off < s.startOff {
			// We went one too far, use the last segment we saved
			break
		}

		// Save this segment and try the next one
		idx = i
	}

	if idx == -1 {
		return 0, errors.New("no segment found")
	}

	return idx, nil
}

func filename(t time.Time) string {
	file := fmt.Sprintf("%s.log", t.Format(time.RFC3339))

	return filepath.Join(defaultLogDir, file)
}
