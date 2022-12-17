package sstable

import (
	"os"
	"sort"
)

// File format:
//
// 1. sstable file format:
// magic1
// N * key -> value
//
// 2. sstable sparse index file format:
// magic2
// N * index = [key -> byte offset]

const magic1 = 0x7473732d696e696d
const magic2 = 0x7864692d696e696d
const sparcity = 8

func Create(filename string, memtable map[string]string) error {
	sst, err := os.Create(filename + ".sst")
	if err != nil {
		return err
	}
	defer sst.Close()
	sstWr := newWriter(sst)
	defer sstWr.Flush()

	idx, err := os.Create(filename + ".idx")
	if err != nil {
		return err
	}
	defer idx.Close()
	idxWr := newWriter(idx)
	defer idxWr.Flush()

	if err := sstWr.WriteUint64(magic1); err != nil {
		return err
	}
	if err := idxWr.WriteUint64(magic2); err != nil {
		return err
	}

	keys := toSortedKeys(memtable)

	for i, key := range keys {
		off := sstWr.Offset()

		if err := sstWr.WriteString(key); err != nil {
			return err
		}
		if err := sstWr.WriteString(memtable[key]); err != nil {
			return err
		}

		// write down the offset in the index every few keys
		if i%sparcity == 0 {
			if err := idxWr.WriteString(key); err != nil {
				return err
			}
			if err := idxWr.WriteUint64(off); err != nil {
				return err
			}
		}
	}

	return nil
}

func toSortedKeys(memtable map[string]string) []string {
	var keys []string
	for k := range memtable {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}
