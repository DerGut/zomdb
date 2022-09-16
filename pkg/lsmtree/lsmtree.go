package lsmtree

import (
	"fmt"
	"time"

	"github.com/DerGut/zomdb/pkg/memtable"
	"github.com/DerGut/zomdb/pkg/sstable"
	"github.com/spf13/afero"
)

type LSMTree struct {
	fs      afero.Fs
	timeSrc func() time.Time
}

func (t *LSMTree) Compact(sst *sstable.SSTable) (*sstable.SSTable, error) {
	return nil, nil
}

func do() error {
	mem := memtable.MemTable{}
	sst, err := sstable.FromMemtable(&mem)
	if err != nil {
		return fmt.Errorf("from memtable: %w", err)
	}

	nsst, err := sst.Compact()
	if err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	fmt.Println(nsst)

	return nil
}
