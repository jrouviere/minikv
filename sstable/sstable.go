package sstable

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// File format:
//
// 1. sstable file format:
// magic1: uint64
// N: uint64 (nb of keys)
// N times {[key] -> [value]}
// key and value are both string stored as:
// len: uint64
// len times char: byte

const magic1 = 0x7473732d696e696d

const sparcity = 16

type SSTable struct {
	filename string
	index    []keyOff // in-memory sparse index
}

type keyOff struct {
	key    string
	offset int64
}

func WriteFile(filename string, memtable map[string]string) error {
	sst, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer sst.Close()
	sstWr := newWriter(sst)
	defer sstWr.Flush()

	if err := sstWr.WriteUint64(magic1); err != nil {
		return err
	}

	keys := toSortedKeys(memtable)

	if err := sstWr.WriteUint64(uint64(len(keys))); err != nil {
		return err
	}

	for _, key := range keys {
		if err := sstWr.WriteString(key); err != nil {
			return err
		}
		if err := sstWr.WriteString(memtable[key]); err != nil {
			return err
		}
	}

	return nil
}

func Load(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	sstRd := newReader(file)
	m1, err := sstRd.ReadUint64()
	if err != nil {
		return nil, err
	}

	if m1 != magic1 {
		return nil, fmt.Errorf("unexpected magic: %v", m1)
	}

	nKeys, err := sstRd.ReadUint64()
	if err != nil {
		return nil, err
	}

	var index []keyOff
	for i := uint64(0); i < nKeys; i++ {
		offset := sstRd.Offset()

		key, err := sstRd.ReadString()
		if err != nil {
			return nil, err
		}

		_, err = sstRd.ReadString()
		if err != nil {
			return nil, err
		}

		if i%sparcity == 0 {
			index = append(index, keyOff{
				key:    key,
				offset: offset,
			})
		}
	}

	return &SSTable{
		filename: filename,
		index:    index,
	}, nil
}

func (sst *SSTable) Get(key string) (string, bool, error) {
	file, err := os.Open(sst.filename)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	sstRd := newReader(file)

	// binary search in our sparse index
	// to find the interval where our key should be in the file
	next := sort.Search(len(sst.index), func(i int) bool {
		return key < sst.index[i].key
	})

	if next == 0 {
		return "", false, nil // not found
	}
	next--

	start := sst.index[next].offset
	if _, err := sstRd.Seek(start); err != nil {
		return "", false, err
	}

	for {
		rdKey, err := sstRd.ReadString()
		if err == io.EOF {
			return "", false, nil // not found
		}
		if err != nil {
			return "", false, err
		}

		rdValue, err := sstRd.ReadString()
		if err != nil {
			return "", false, err
		}
		if key == rdKey {
			return rdValue, true, nil // found it!
		}
		if key < rdKey {
			return "", false, nil // not found
		}
	}
}

func (sst *SSTable) Debug() string {
	var sb strings.Builder
	for _, idx := range sst.index {
		fmt.Fprintf(&sb, "0x%04X: %v\n", idx.offset, idx.key)
	}
	return sb.String()
}

func toSortedKeys(memtable map[string]string) []string {
	var keys []string
	for k := range memtable {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}
