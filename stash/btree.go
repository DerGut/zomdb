package zomdb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
)

const (
	pageSize        = 4096
	pageHeaderSize  = 16
	bTreeHeaderSize = pageSize
)

const rootNodeID = bTreeHeaderSize / pageSize

type BTree struct {
	nodes       pageStructure
	waterMarkID atomic.Int64
}

// func (bt *BTree) Open(path string) error {
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return err
// 	}
// }

func NewBTree(path string) (*BTree, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Write header and root page
	header := make([]byte, bTreeHeaderSize+pageSize)
	if _, err := f.Write(header); err != nil {
		return nil, err
	}

	bt := BTree{
		nodes: file{
			file: f,
		},
	}
	bt.waterMarkID.Store(bTreeHeaderSize)

	return &bt, nil
}

func (bt *BTree) Put(ctx context.Context, key, value string) error {
	return nil
}

func (bt *BTree) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (bt *BTree) Close() error {
	return bt.nodes.Close()
}

func (bt *BTree) Find(key string) (string, error) {
	root, err := bt.loadNode(rootNodeID)
	if err != nil {
		return "", err
	}

	value, _, err := bt.find(root, key)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (bt *BTree) Update(key, value string) (found bool, err error) {
	root, err := bt.loadNode(rootNodeID)
	if err != nil {
		return false, err
	}

	_, n, err := bt.find(root, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}

		return false, err
	}

	if err := bt.update(n, key, value); err != nil {
		// TODO: false or true? 🤔
		return false, err
	}

	return true, nil
}

func (bt *BTree) Insert(key, value string) error {
	root, err := bt.loadNode(rootNodeID)
	if err != nil {
		return err
	}

	_, n, err := bt.find(root, key)
	if err != nil {
		if !errors.Is(err, ErrNotFound) { // if something else
			return err
		}
	}

	c := cell{
		key:   key,
		value: value,
	}
	if err := bt.insert(n, c); err != nil {
		return err
	}

	if err := bt.storeNodes(n); err != nil {
		// we skip on nodes if we did a split
		// TODO: what about the 'mark as dirty' mechanism?
		return err
	}

	if err := bt.nodes.Sync(); err != nil {
		return err
	}

	return nil
}

func (bt *BTree) loadNode(id nodeID) (*node, error) {
	p, err := bt.nodes.load(id.toPagePtr())
	if err != nil {
		return nil, err
	}

	var n node
	if err := n.Decode(p); err != nil {
		return nil, fmt.Errorf("decode page: %w", err)
	}

	return &n, nil
}

func (bt *BTree) storeNodes(ns ...*node) error {
	for _, n := range ns {
		p, err := n.Encode()
		if err != nil {
			return fmt.Errorf("encoding node: %w", err)
		}

		if err := bt.nodes.store(n.id.toPagePtr(), p); err != nil {
			return err
		}
	}

	return nil
}

func (bt *BTree) addPage() nodeID {
	return nodeID(bt.waterMarkID.Add(1))
}

type node struct {
	id, parent nodeID
	isInternal bool

	used uint16

	cells []cell
}

// nodeID can be used to uniquely identify and relate nodes.
// It corresponds to the file offset where the nodes page is stored.
// The nodeID 0 indicates the root node of a BTree. The nodeID 1
// has an offset of pageSize.
type nodeID int

func (n nodeID) toPagePtr() pagePtr {
	return pagePtr(n * pageSize)
}

func toNodeID(ptr pagePtr) (nodeID, error) {
	if ptr%pageSize != 0 {
		return 0, fmt.Errorf("invalid pagePtr: %v", ptr)
	}

	return nodeID(ptr / pageSize), nil
}

// pagePtr is a byte offset into a file specifying the exact location of a page.
type pagePtr int64

func (bt *BTree) find(n *node, key string) (string, *node, error) {
	if !n.isInternal {
		for _, c := range n.cells {
			if c.key == key {
				return c.value, n, nil
			}
		}

		// We still return this node because insertions utilize find()
		// to locate the node that should contain a key.
		return "", n, ErrNotFound
	}

	childPtr := n.cells[len(n.cells)-1].ptr
	for _, c := range n.cells {
		if strings.Compare(key, c.key) < 0 {
			childPtr = c.ptr
		}
	}

	childID, err := toNodeID(childPtr)
	if err != nil {
		return "", nil, fmt.Errorf("parse child id: %w", err)
	}

	child, err := bt.loadNode(childID)
	if err != nil {
		return "", nil, err
	}

	return bt.find(child, key)
}

func (bt *BTree) update(n *node, key, value string) error {
	if n.isInternal {
		return errors.New("update in internal node")
	}

	update := cell{
		key:   key,
		value: value,
	}

	for i, c := range n.cells {
		if c.key == key {
			addedSize := update.size() - c.size()

			if n.free() < int(addedSize) {
				// Remove existing cell
				n.cells = append(n.cells[:i], n.cells[i+1:]...)
				n.used -= c.size()

				// Split and insert updated cell
				left, right, parent, err := bt.split(n, update)
				if err != nil {
					return err
				}

				return bt.storeNodes(left, right, parent)
			}

			n.cells[i] = update
			n.used += addedSize

			return bt.storeNodes(n)
		}
	}

	return ErrNotFound
}

// insert inserts the cell into the node but does not write it to disk.
// Call BTree.store() afterwards.
func (bt *BTree) insert(n *node, insert cell) error {
	if insert.key == "" {
		return errors.New("key must not be \"\"")
	}

	if !n.isInternal && !n.isRoot() && insert.isPtr() {
		return fmt.Errorf("inserting ptr into leaf: %d", n.id)
	}

	if n.isInternal && insert.isData() {
		return errors.New("inserting value into internal node")
	}

	if n.free() < int(insert.size()) {
		left, right, parent, err := bt.split(n, insert)
		if err != nil {
			return err
		}

		// TODO: Can we get away with not storing the three nodes here?
		// We postpone writes for the n *node too. Should we return all three
		// if necessary?
		// Should we return a sentinel error to tell the calling func that
		// it should perform a split-insert instead?
		return bt.storeNodes(left, right, parent)
	}

	// In whichever way we insert it, the node will lose this much space
	n.used += insert.size()

	for i, c := range n.cells {
		if strings.Compare(insert.key, c.key) < 0 {
			n.cells = append(n.cells[:i+1], n.cells[i:]...)
			n.cells[i] = insert

			return nil
		}
	}

	if insert.isData() {
		// Leaf nodes have an N:N mapping of keys to values.
		// If none of the existing keys match, it means that the newly inserted
		// key is greater and should be inserted at the end, along with the value.
		n.cells = append(n.cells, insert)
		return nil
	}

	// Internal nodes have a N:N+1 mapping of keys (separators) to pointers.
	// If none of the existing keys match, it means that the key is also greater
	// and should be inserted at the end of the keys.
	// However, because we store keys and pointers in the same slice and because
	// the last slice element is guaranteed to only contain a pointer (N+1),
	// we need to insert the new pair at the second-to-last position.
	n.cells[len(n.cells)-1].key = insert.key
	n.cells = append(n.cells, cell{ptr: insert.ptr})
	// n.cells = append(n.cells[:len(n.cells)-1], insert, n.cells[len(n.cells)-1])

	return nil
}

// A page starts with a header of 16 bytes:
//
//	0:8: pointer to parent page
//	8:10: uint16, bytes used
//	10:12: uint16, flags (internal node not)
//	12:14: number of cells stored
type page [pageSize]byte

// func (p *page) insert(insert cell) error {
// 	if insert.key == "" {
// 		return errors.New("key must not be \"\"")
// 	}

// 	if p.free() < int(insert.size()) {
// 		// Make split() take the new key and
// 		// have it know about the insertion position right away.
// 		// This way, node balancing is one step ahead
// 		err := bt.split(n, insert)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func (p *page) parent() pagePtr {
	return pagePtr(binary.BigEndian.Uint64(p[:8]))
}

func (p *page) setParent(parent pagePtr) {
	binary.BigEndian.PutUint64(p[:8], uint64(parent))
}

func (p *page) used() uint16 {
	return binary.BigEndian.Uint16(p[8:10])
}

func (p *page) setUsed(used uint16) {
	binary.BigEndian.PutUint16(p[8:10], used)
}

func (p *page) free() uint16 {
	return pageSize - pageHeaderSize - p.used()
}

func (p *page) isInternal() bool {
	internal := binary.BigEndian.Uint16(p[10:12])
	return internal != 0
}

func (p *page) setInternal(val bool) {
	var internal uint16
	if val {
		internal = 1
	}
	binary.BigEndian.PutUint16(p[10:12], internal)
}

var errParsePage = errors.New("parsing page")

func (p *page) cells() ([]cell, error) {
	numCells := binary.BigEndian.Uint16(p[12:14])

	cells := make([]cell, numCells)

	offset := pageHeaderSize
	for i := range cells {
		if offset+4 > pageSize {
			return nil, fmt.Errorf("invalide cell number: %d", numCells)
		}

		keySize := int(binary.BigEndian.Uint32(p[offset : offset+4]))
		if offset+4+keySize > pageSize {
			return nil, fmt.Errorf("invalid keySize: %d", keySize)
		}

		key := p[offset+4 : offset+4+keySize]
		cells[i].key = string(key)
		offset += 4 + keySize

		if p.isInternal() {
			if offset+8 > pageSize {
				return nil, fmt.Errorf("exceeded pageSize: %w", errParsePage)
			}

			ptr := binary.BigEndian.Uint64(p[offset : offset+8])
			cells[i].ptr = pagePtr(ptr)
			offset += 8
		} else {
			if offset+4 > pageSize {
				return nil, fmt.Errorf("exceeded pageSize: %w", errParsePage)
			}

			valueSize := int(binary.BigEndian.Uint32(p[offset : offset+4]))
			if offset+4+valueSize > pageSize {
				return nil, fmt.Errorf("exceeded pageSize: %w", errParsePage)
			}

			value := p[offset+4 : offset+4+valueSize]
			cells[i].value = string(value)
			offset += 4 + valueSize
		}
	}

	return cells, nil
}

func (p *page) setCells(cells []cell) {
	offset := pageHeaderSize

	binary.BigEndian.PutUint16(p[12:14], uint16(len(cells)))
	for _, c := range cells {
		binary.BigEndian.PutUint32(p[offset:offset+4], uint32(len(c.key)))
		copy(p[offset+4:offset+4+len(c.key)], c.key)

		if c.value != "" {
			// Write value
			binary.BigEndian.PutUint32(p[offset+4+len(c.key):offset+8+len(c.key)], uint32(len(c.value)))
			copy(p[offset+8+len(c.key):offset+8+len(c.key)+len(c.value)], c.value)
		} else {
			// Write page pointer
			binary.BigEndian.PutUint64(p[offset+4+len(c.key):offset+12+len(c.key)], uint64(c.ptr))
		}

		offset += int(c.size())
	}
}

type cell struct {
	key   string
	value string
	ptr   pagePtr
}

func (c cell) isPtr() bool  { return c.ptr != 0 }
func (c cell) isData() bool { return !c.isPtr() }

// currently we are limited to cell sizes < pageSize
// once we implement something like TOAST, size() should
// return uint32 or whatever appropriate
func (c cell) size() uint16 {
	if c.value != "" {
		return 8 + uint16(len(c.key)) + uint16(len(c.value))
	}

	return 8 + uint16(len(c.key))
}

// pageOffsets start right after the bTreeHeader. This means that a parent set to 0
// indicates a root node that doesn't have a parent.
func (n *node) isRoot() bool {
	return n.id == rootNodeID
}

func (n *node) Decode(p page) error {
	parentID, err := toNodeID(p.parent())
	if err != nil {
		return fmt.Errorf("decoding parentID: %w", err)
	}
	n.parent = parentID

	n.used = p.used()

	cells, err := p.cells()
	if err != nil {
		return err
	}

	n.cells = cells

	return nil
}

func (n *node) Encode() (page, error) {
	var p page
	p.setParent(n.parent.toPagePtr())
	p.setUsed(uint16(n.used))
	if n.isInternal {
		p.setInternal(true)
	}
	p.setCells(n.cells)

	return p, nil
}

type pageStructure interface {
	load(pagePtr) (page, error)
	store(pagePtr, page) error

	Sync() error
	io.Closer
}

// A file is a pageStructure.
type file struct {
	file interface {
		io.ReaderAt
		io.WriterAt
		io.Closer
		Sync() error
	}
}

func (f file) load(ptr pagePtr) (page, error) {
	var p page
	if _, err := f.file.ReadAt(p[:], int64(ptr)); err != nil {
		return page{}, err
	}

	return p, nil
}

func (f file) store(ptr pagePtr, p page) error {
	if _, err := f.file.WriteAt(p[:], int64(ptr)); err != nil {
		return err
	}

	return nil
}

func (f file) Close() error {
	return f.file.Close()
}

func (f file) Sync() error {
	return f.file.Sync()
}

// type memFile struct {
// 	rw io.ReadWriteSeeker
// }

// func (ms *memFile) load(ptr pagePtr) (page, error) {
// 	if _, err := ms.rw.Seek(int64(ptr), io.SeekStart); err != nil {
// 		return page{}, err
// 	}

// 	var p page
// 	if _, err := ms.rw.Read(p[:]); err != nil {
// 		return page{}, err
// 	}

// 	return p, nil
// }

func (n *node) free() int {
	return pageSize - pageHeaderSize - int(n.used)
}

func (bt *BTree) split(n *node, insert cell) (left, right, parent *node, err error) {
	if len(n.cells) == 0 {
		return nil, nil, nil, errors.New("nothing to split")
	}

	if n.isRoot() {
		left, right, root := bt.splitRoot(n, insert)
		return left, right, root, nil
	}

	if n.isInternal && insert.isData() {
		panic("inserting data cell into internal node")
	}

	if !n.isInternal && insert.isPtr() {
		// We've already exited for root nodes
		panic("inserting pointer cell into leaf node")
	}

	if n.isInternal {
		last := n.cells[len(n.cells)-1]
		if last.key != "" {
			panic(fmt.Sprintf("right-most pointer cell contains a key: %q", last.key))
		}
	}

	left, right = bt.splitNode(n, insert)

	rightPtr := cell{
		key: right.cells[0].key,
		ptr: right.id.toPagePtr(),
	}

	if n.isInternal {
		right.cells = right.cells[1:]
	}

	// TODO: the parent may hold a ptr to left associated with key
	// now we basically insert key with a ptr to right
	// We need to figure this out!
	parent, err = bt.loadNode(n.parent)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("loading parent: %w", err)
	}

	if !parent.isInternal && !n.isRoot() {
		// TODO: what about roots again? can the be root and leaf at the same time?
		// or do they get promoted to be an internal node after their first child?
		panic("parent is a leaf node")
	}

	if err := bt.insert(parent, rightPtr); err != nil {
		return nil, nil, nil, fmt.Errorf("add new split node to parent: %w", err)
	}

	return left, right, parent, nil
}

func (bt *BTree) splitRoot(root *node, insert cell) (left, right, newRoot *node) {
	leftData, rightData := splitCells(root.cells, insert)

	left = &node{
		parent: root.id,
		id:     bt.addPage(),
		cells:  leftData,
		used:   used(leftData),
	}

	right = &node{
		parent: root.id,
		id:     bt.addPage(),
		cells:  rightData,
		used:   used(rightData),
	}

	newRoot = &node{
		id: root.id,
		cells: []cell{
			{
				key: right.cells[0].key,
				ptr: left.id.toPagePtr(),
			},
			{
				ptr: right.id.toPagePtr(),
			},
		},
		used: uint16(len(right.cells[0].key)) + 16,
	}

	return left, right, newRoot
}

func (bt *BTree) splitNode(n *node, insert cell) (left, right *node) {
	leftData, rightData := splitCells(n.cells, insert)

	left = &node{
		parent:     n.parent,
		id:         n.id,
		isInternal: n.isInternal,
		cells:      leftData,
		used:       used(leftData),
	}

	right = &node{
		parent:     n.parent,
		id:         bt.addPage(),
		isInternal: n.isInternal,
		cells:      rightData,
		used:       used(rightData),
	}

	return left, right
}

func splitCells(cells []cell, insert cell) (left, right []cell) {
	added := addCell(cells, insert)

	half := (len(added) - 1) / 2

	return added[:half+1], added[half+1:]
}

func addCell(cells []cell, insert cell) []cell {
	for i, c := range cells {
		if strings.Compare(insert.key, c.key) < 0 {
			result := append(cells[:i+1], cells[i:]...)
			result[i] = insert
			return result
		}
	}

	if insert.isPtr() {
		cells[len(cells)-1].key = insert.key
		return append(cells, cell{ptr: insert.ptr})
		// Insert at second-to-last position
		// return append(cells[:len(cells)-1], insert, cells[len(cells)-1])
	}

	return append(cells, insert)
}

func used(cells []cell) uint16 {
	var sum uint16
	for _, c := range cells {
		sum += c.size()
	}
	return sum
}
