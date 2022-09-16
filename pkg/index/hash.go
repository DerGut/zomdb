package index

// Hash is an in-memory index that stores offsets in a hash map
type Hash struct {
	m map[string]int64
}

var _ Index = &Hash{}

func (h *Hash) PutOffset(key []byte, off int64) error {
	h.m[string(key)] = off

	return nil
}

func (h *Hash) GetOffset(key []byte) (int64, error) {
	off, ok := h.m[string(key)]
	if !ok {
		return 0, ErrNotFound
	}

	return off, nil
}
