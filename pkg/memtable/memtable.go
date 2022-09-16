package memtable

import "bytes"

type MemTable struct {
	root *node
}

type node struct {
	key   []byte
	value []byte

	left, right *node
}

func (mt *MemTable) Get(key []byte) (value []byte, found bool) {
	current := mt.root
	for {
		if current == nil {
			return nil, false
		}

		switch bytes.Compare(current.key, key) {
		case 0:
			return current.value, true
		case -1:
			current = current.left
		case +1:
			current = current.right
		}
	}
}

func (mt *MemTable) Put(key, value []byte) error {
	current := mt.root
	for {
		if current == nil {
			// Add new node
			current = &node{
				key:   key,
				value: value,
			}
			return nil
		}

		switch bytes.Compare(current.key, key) {
		case 0:
			// Overwrite node
			current.value = value
			return nil
		case -1:
			current = current.left
		case +1:
			current = current.right
		}
	}
}

func (mt *MemTable) Traverse() {

}
