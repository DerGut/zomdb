package zomdb

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBTree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	bt, err := NewBTree(path)
	if err != nil {
		t.Fatalf("create BTree: %v", err)
	}

	if err := bt.Insert("test", "value"); err != nil {
		t.Fatalf("insert (%q: %q): %v", "test", "value", err)
	}

	value, err := bt.Find("test")
	if err != nil {
		t.Fatalf("find %q: %v", "test", err)
	}

	if value != "value" {
		t.Errorf("want value = %q, got = %q", "value", value)
	}

	found, err := bt.Update("test", "update")
	if err != nil {
		t.Fatalf("update (%q: %q): %v", "test", "update", err)
	}

	if !found {
		t.Errorf("want value found")
	}
}

func TestBigValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	bt, err := NewBTree(path)
	if err != nil {
		t.Fatalf("create BTree: %v", err)
	}

	const halfAPage = pageSize / 2

	value1 := strings.Repeat("a", halfAPage)
	if err := bt.Insert("key1", value1); err != nil {
		t.Fatalf("insert (%q: %q): %v", "key1", value1, err)
	}

	value2 := strings.Repeat("a", halfAPage)
	if err := bt.Insert("key2", value2); err != nil {
		t.Fatalf("insert (%q: %q): %v", "key2", value2, err)
	}

	got1, err := bt.Find("key1")
	if err != nil {
		t.Fatalf("find %q: %v", "key1", err)
	}

	if got1 != value1 {
		t.Errorf("got %q, want %q", got1, value1)
	}

	got2, err := bt.Find("key2")
	if err != nil {
		t.Fatalf("find %q: %v", "key2", err)
	}

	if got2 != value2 {
		t.Errorf("got %q, want %q", got2, value2)
	}
}

func TestPage(t *testing.T) {
	var p page
	p.setUsed(18)
	if p.used() != 18 {
		t.Fail()
	}
}

func TestNode(t *testing.T) {
	type testCase struct {
		name string
		node node
	}

	run := func(t *testing.T, tc testCase) {
		p, err := tc.node.Encode()
		if err != nil {
			t.Fatalf("Encode(): %v", err)
		}

		var got node
		if err := got.Decode(p); err != nil {
			t.Fatalf("Decode(): %v", err)
		}

		if !reflect.DeepEqual(got, tc.node) {
			t.Fatalf("got = %#v, want = %#v", got, tc.node)
		}
	}

	testCases := []testCase{
		{
			name: "zero value",
		},
		{
			name: "empty root node",
			node: node{
				id: pageHeaderSize,
			},
		},
		{
			name: "root leaf node",
			node: node{
				id:   pageHeaderSize,
				used: 25,
				cells: []cell{
					{
						key:   "test",
						value: "value",
					},
				},
			},
		},
		{
			name: "root internal node",
		},
		{
			name: "leaf node",
		},
		{
			name: "internal node",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})
	}
}

func TestNode_split(t *testing.T) {
	type testCase struct {
		name         string
		node, parent node
		insert       cell

		wantLeft, wantRight, wantParent []cell
		wantErr                         bool
	}

	var (
		parentID nodeID = rootNodeID
		id       nodeID = parentID + 1
		newID    nodeID = id + 1
	)

	run := func(t *testing.T, tc testCase) {
		parentPage, err := tc.parent.Encode()
		if err != nil {
			t.Fatalf("encode parent: %v", err)
		}

		bt := BTree{
			nodes: &memFile{
				pages: map[pagePtr]page{
					tc.parent.id.toPagePtr(): parentPage,
				},
			},
		}
		bt.waterMarkID.Store(int64(id))

		left, right, parent, err := bt.split(&tc.node, tc.insert)
		if !tc.wantErr && err != nil {
			t.Errorf("bt.split(): %v", err)
		}
		if tc.wantErr && err == nil {
			t.Errorf("bt.split(): expected error")
		}

		if !reflect.DeepEqual(tc.wantLeft, left) {
			t.Errorf("want left cells =\n\t%v,\ngot =\n\t%v", tc.wantLeft, left)
		}

		if !reflect.DeepEqual(tc.wantRight, right) {
			t.Errorf("want right cells =\n\t%v,\ngot =\n\t%v", tc.wantRight, right)
		}

		if !reflect.DeepEqual(tc.wantParent, parent) {
			t.Errorf("want parent cells =\n\t%v,\ngot =\n\t%v", tc.wantParent, parent)
		}
	}

	testCases := []testCase{
		{
			name:    "zero node",
			wantErr: true,
		},
		{
			name: "leaf node with one cell, parent node with one sep, insert after greatest key",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere
				},
			},
			node: node{
				id:     id,
				parent: parentID,
				cells: []cell{
					{key: "5", value: "value"},
				},
			},
			insert: cell{key: "8", value: "123"},
			wantLeft: []cell{
				{key: "5", value: "value"},
			},
			wantRight: []cell{
				{key: "8", value: "123"},
			},
			wantParent: []cell{
				{key: "8", ptr: id.toPagePtr()},
				{key: "10", ptr: newID.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "leaf node with one cell, parent node with one sep, insert before lowest key",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere
				},
			},
			node: node{
				id:     id,
				parent: parentID,
				cells: []cell{
					{key: "5", value: "value"},
				},
			},
			insert: cell{key: "3", value: "123"},
			wantLeft: []cell{
				{key: "3", value: "123"},
			},
			wantRight: []cell{
				{key: "5", value: "value"},
			},
			wantParent: []cell{
				{key: "5", ptr: newID.toPagePtr()},
				{key: "10", ptr: id.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "node with two cells, parent node with one sep, insert after greatest key",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "5", value: "123"},
					{key: "8", value: "456"},
				},
			},
			insert: cell{key: "9", value: "test"},
			wantLeft: []cell{
				{key: "5", value: "123"},
				{key: "8", value: "456"},
			},
			wantRight: []cell{
				{key: "9", value: "test"},
			},
			wantParent: []cell{
				{key: "9", ptr: id.toPagePtr()},
				{key: "10", ptr: newID.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "node with two cells, parent node with one sep, insert before lowest key",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "5", value: "123"},
					{key: "8", value: "456"},
				},
			},
			insert: cell{key: "4", value: "test"},
			wantLeft: []cell{
				{key: "4", value: "test"},
				{key: "5", value: "123"},
			},
			wantRight: []cell{
				{key: "8", value: "456"},
			},
			wantParent: []cell{
				{key: "8", ptr: id.toPagePtr()},
				{key: "10", ptr: newID.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "node with two cells, parent node with one sep, insert between keys",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "5", value: "123"},
					{key: "8", value: "456"},
				},
			},
			insert: cell{key: "6", value: "test"},
			wantLeft: []cell{
				{key: "5", value: "123"},
				{key: "6", value: "test"},
			},
			wantRight: []cell{
				{key: "8", value: "456"},
			},
			wantParent: []cell{
				{key: "8", ptr: id.toPagePtr()},
				{key: "10", ptr: newID.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "node with two cells, parent node with one sep, insert after sep",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: 1232131}, //somewhere
					{ptr: id.toPagePtr()},
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "15", value: "123"},
					{key: "18", value: "456"},
				},
			},
			insert: cell{key: "16", value: "test"},
			wantLeft: []cell{
				{key: "15", value: "123"},
				{key: "16", value: "test"},
			},
			wantRight: []cell{
				{key: "18", value: "456"},
			},
			wantParent: []cell{
				{key: "10", ptr: 1232131},
				{key: "18", ptr: id.toPagePtr()},
				{ptr: newID.toPagePtr()},
			},
		},
		{
			name: "node with two cells, parent node with two seps, insert before lowest sep",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: id.toPagePtr()},
					{key: "20", ptr: 4325432532}, // somewhere
					{ptr: 1232131},               //somewhere else
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "5", value: "123"},
					{key: "8", value: "456"},
				},
			},
			insert: cell{key: "6", value: "test"},
			wantLeft: []cell{
				{key: "5", value: "123"},
				{key: "6", value: "test"},
			},
			wantRight: []cell{
				{key: "8", value: "456"},
			},
			wantParent: []cell{
				{key: "8", ptr: id.toPagePtr()},
				{key: "10", ptr: newID.toPagePtr()},
				{key: "20", ptr: 4325432532},
				{ptr: 1232131},
			},
		},
		{
			name: "node with two cells, parent node with two seps, insert between seps",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: 4325432532}, // somewhere
					{key: "20", ptr: id.toPagePtr()},
					{ptr: 1232131}, //somewhere else
				},
			},
			node: node{
				parent: parentID,
				id:     id,
				cells: []cell{
					{key: "15", value: "123"},
					{key: "18", value: "456"},
				},
			},
			insert: cell{key: "16", value: "test"},
			wantLeft: []cell{
				{key: "15", value: "123"},
				{key: "16", value: "test"},
			},
			wantRight: []cell{
				{key: "18", value: "456"},
			},
			wantParent: []cell{
				{key: "10", ptr: 4325432532},
				{key: "18", ptr: id.toPagePtr()},
				{key: "20", ptr: newID.toPagePtr()},
				{ptr: 1232131},
			},
		},
		{
			name: "internal node with one sep errors",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "100", ptr: id.toPagePtr()},
					{ptr: 11111},
				},
			},
			node: node{
				parent:     parentID,
				id:         id,
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: 22222},
					{ptr: 33333},
				},
			},
			insert:  cell{key: "5", ptr: 44444},
			wantErr: true,
		},
		{
			name: "internal node with one three seps",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "100", ptr: id.toPagePtr()},
					{ptr: 11111},
				},
			},
			node: node{
				parent:     parentID,
				id:         id,
				isInternal: true,
				cells: []cell{
					{key: "20", ptr: 22222},
					{key: "40", ptr: 33333},
					{key: "60", ptr: 44444},
					{ptr: 55555},
				},
			},
			insert: cell{key: "80", ptr: 666666},
			wantLeft: []cell{
				{key: "20", ptr: 22222},
				{key: "40", ptr: 33333},
				{ptr: 44444},
			},
			wantRight: []cell{
				{key: "80", ptr: 66666},
				{ptr: 55555},
			},
			wantParent: []cell{
				{key: "60", ptr: id.toPagePtr()},
				{key: "100", ptr: newID.toPagePtr()},
				{ptr: 11111},
			},
		},
		{
			name: "internal node with two seps",
			parent: node{
				id:         parentID,
				isInternal: true,
				cells: []cell{
					{key: "100", ptr: id.toPagePtr()},
					{ptr: 11111},
				},
			},
			node: node{
				parent:     parentID,
				id:         id,
				isInternal: true,
				cells: []cell{
					{key: "20", ptr: 22222},
					{key: "40", ptr: 33333},
					{ptr: 44444},
				},
			},
			insert: cell{key: "80", ptr: 55555},
			wantLeft: []cell{
				{key: "20", ptr: 22222},
				{ptr: 33333},
			},
			wantRight: []cell{
				{key: "80", ptr: 55555},
				{ptr: 44444},
			},
			wantParent: []cell{
				{key: "40", ptr: id.toPagePtr()},
				{key: "100", ptr: newID.toPagePtr()},
				{ptr: 11111},
			},
		},
		{
			name: "parent with multiple children",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})
	}
}

type memFile struct {
	pages map[pagePtr]page
}

func (f *memFile) load(ptr pagePtr) (page, error) {
	p, ok := f.pages[ptr]
	if !ok {
		return page{}, fmt.Errorf("page not available: %d", ptr)
	}

	return p, nil
}

func (f *memFile) store(ptr pagePtr, p page) error {
	f.pages[ptr] = p
	return nil
}

func (f *memFile) Sync() error { return nil }

func (f *memFile) Close() error { return nil }

func TestSplitNode(t *testing.T) {
	type testCase struct {
		name string

		node   node
		insert cell

		wantLeft, wantRight *node
	}

	run := func(t *testing.T, tc testCase) {
		var bt BTree
		left, right := bt.splitNode(&tc.node, tc.insert)

		if !reflect.DeepEqual(tc.wantLeft, left) {
			t.Errorf("want left = %+v, got = %+v", tc.wantLeft, left)
		}

		if !reflect.DeepEqual(tc.wantRight, right) {
			t.Errorf("want right = %+v, got = %+v", tc.wantRight, right)
		}
	}

	testCases := []testCase{
		{
			name: "insert value at the end",
			node: node{
				cells: []cell{
					{key: "1", value: "one"},
					{key: "2", value: "two"},
				},
			},
			insert: cell{key: "3", value: "three"},
			wantLeft: &node{
				cells: []cell{
					{key: "1", value: "one"},
					{key: "2", value: "two"},
				},
				used: 2 * (4 + 4 + 1 + 3),
			},
			wantRight: &node{
				cells: []cell{
					{key: "3", value: "three"},
				},
				used: 4 + 4 + 1 + 5,
				id:   4096,
			},
		},
		{
			name: "insert value inbetween",
			node: node{
				cells: []cell{
					{key: "1", value: "one"},
					{key: "3", value: "three"},
				},
			},
			insert: cell{key: "2", value: "two"},
			wantLeft: &node{
				cells: []cell{
					{key: "1", value: "one"},
					{key: "2", value: "two"},
				},
				used: 2 * (4 + 4 + 1 + 3),
			},
			wantRight: &node{
				cells: []cell{
					{key: "3", value: "three"},
				},
				used: 4 + 4 + 1 + 5,
				id:   4096,
			},
		},
		{
			name: "split internal node",
			node: node{
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: 4096},
					{ptr: 8192},
				},
			},
			insert: cell{key: "20", ptr: 12288},
			wantLeft: &node{
				isInternal: true,
				cells: []cell{
					{key: "10", ptr: 4096},
					{ptr: 4096},
				},
				used: (4 + 2 + 4),
			},
			wantRight: &node{
				isInternal: true,
				cells: []cell{
					{key: "20", ptr: 12288},
					{ptr: 8192},
				},
				used: 4 + 2 + 4 + 4 + 0 + 4,
				id:   4096,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})
	}
}

func BenchmarkBTree_Insert(b *testing.B) {

}

func BenchmarkBTree_Find(b *testing.B) {

}

func BenchmarkBTree_Update(b *testing.B) {

}

func BenchmarkBTree_Delete(b *testing.B) {

}

func BenchmarkBTree_InsertInplace(b *testing.B) {
	// TODO: test implementation without completely deserializing
	// into in-mem data structures
}

func TestFile(t *testing.T) {
	type testCase struct {
		name string
	}

	run := func(t *testing.T, tc testCase) {
		f := file{
			file: &bufFile{},
		}
		_ = f
	}

	testCases := []testCase{
		{},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run(t, tc)
		})
	}
}

type bufFile struct {
	buf bytes.Buffer
}

func (f *bufFile) ReadAt([]byte, int64) (int, error) { return 0, nil }

func (f *bufFile) WriteAt([]byte, int64) (int, error) {
	return 0, nil
}

func (f *bufFile) Sync() error {
	return nil
}

func (f *bufFile) Close() error {
	return nil
}
