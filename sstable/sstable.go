package sstable

import (
	"fmt"
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

const sparcity = 8

type SSTable struct {
	file  *os.File
	index []keyOff // in-memory sparse index
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
		file:  file,
		index: index,
	}, nil
}

func (sst *SSTable) Debug() string {
	var sb strings.Builder
	for _, idx := range sst.index {
		fmt.Fprintf(&sb, "0x%04X: %v\n", idx.offset, idx.key)
	}
	return sb.String()
}

func (sst *SSTable) Close() error {
	return sst.file.Close()
}

func toSortedKeys(memtable map[string]string) []string {
	var keys []string
	for k := range memtable {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}
