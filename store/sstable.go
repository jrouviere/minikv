package store

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/jrouviere/minikv/avl"
)

const magic = 0x7473732d696e696d

const sparcity = 16

/*
SSTable is an immutable file storing a list of sorted string.
Deleted keys are stored as en empty string.

File format:

magic: uint64
N times {[key] -> [value]}

key and value are both string stored as:
len: uint64
len times char: byte
*/
type SSTable struct {
	filename string
	index    []keyOff // in-memory sparse index
}

type keyOff struct {
	key    string
	offset int64
}

func WriteFile(filename string, memtable *avl.Tree) error {
	sst, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer sst.Close()
	sstWr := newWriter(sst)
	defer sstWr.Flush()

	if err := sstWr.WriteUint64(magic); err != nil {
		return err
	}

	var wrErr error
	memtable.InorderTraversal(func(n *avl.Node) {
		if err := sstWr.WriteString(n.Key); err != nil {
			wrErr = err
		}
		if err := sstWr.WriteString(n.Value); err != nil {
			wrErr = err
		}
	})
	return wrErr
}

func LoadSST(filename string) (*SSTable, error) {
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

	if m1 != magic {
		return nil, fmt.Errorf("unexpected magic: %v", m1)
	}

	var index []keyOff
	for i := uint64(0); ; i++ {
		offset := sstRd.Offset()

		key, err := sstRd.ReadString()
		if err != nil {
			if err == io.EOF {
				break
			}
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

func (sst *SSTable) Get(key string) (val string, found bool, err error) {
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
	if err := sstRd.SeekTo(start); err != nil {
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

func (sst *SSTable) Delete() error {
	return os.Remove(sst.filename)
}

// Merge two sstables together
// sst2 is more recent than sst1
// ie: sst2 overrides key from sst1
func Merge(sst1, sst2 *SSTable, destination string) error {
	f1, err := os.Open(sst1.filename)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(sst2.filename)
	if err != nil {
		return err
	}
	defer f2.Close()

	rd1, err := processHeader(f1)
	if err != nil {
		return err
	}
	rd2, err := processHeader(f2)
	if err != nil {
		return err
	}

	// we use a memtable to simplify things
	// but really the result should be written
	// in a sst file directly
	var memtable avl.Tree

	key1, value1 := nextKey(rd1)
	key2, value2 := nextKey(rd2)

	for key1 != "" || key2 != "" {
		switch {
		case key1 == key2:
			memtable.Upsert(key2, value2)
			key1, value1 = nextKey(rd1)
			key2, value2 = nextKey(rd2)

		case key1 == "" || key2 < key1:
			memtable.Upsert(key2, value2)
			key2, value2 = nextKey(rd2)

		case key2 == "" || key1 < key2:
			memtable.Upsert(key1, value1)
			key1, value1 = nextKey(rd1)

		default:
			panic("should not happen")
		}
	}

	return WriteFile(destination, &memtable)
}

func processHeader(file *os.File) (*fileReader, error) {
	rd := newReader(file)

	m1, err := rd.ReadUint64()
	if err != nil {
		return nil, err
	}

	if m1 != magic {
		return nil, fmt.Errorf("unexpected magic: %v", m1)
	}

	return rd, nil
}

func nextKey(rd *fileReader) (string, string) {
	key, err := rd.ReadString()
	if err != nil {
		return "", ""
	}
	val, err := rd.ReadString()
	if err != nil {
		return "", ""
	}
	return key, val
}

func (sst *SSTable) Debug() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "SST: %v\n", sst.filename)
	for _, idx := range sst.index {
		fmt.Fprintf(&sb, "0x%04X: %v\n", idx.offset, idx.key)
	}
	return sb.String()
}
