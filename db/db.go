package db

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/jrouviere/minikv/store"
)

type DB struct {
	dirname   string
	fileCount int32

	mu sync.RWMutex
	// from earliest to latest sstable
	store    []*store.SSTable
	memtable *store.Treap
	wal      *store.WAL
}

func New(dirname string) (*DB, error) {
	walpath := filepath.Join(dirname, "wal.dat")

	memtable, err := store.LoadWAL(walpath)
	if err != nil {
		memtable = &store.Treap{}
	}

	wal, err := store.NewWAL(walpath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dirname:  dirname,
		memtable: memtable,
		wal:      wal,
	}

	if err := db.LoadSSTables(); err != nil {
		return nil, err
	}

	if err := db.Flush(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Set(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Commit(key, value); err != nil {
		panic(err)
	}

	// store in memtable
	db.memtable.Upsert(key, value)
}

func (db *DB) Delete(key string) {
	db.Set(key, "")
}

func (db *DB) Get(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Here we could use a bloomfilter to speedup the case where
	// the key is not in the DB.
	// We could also use a cache for values that are frequently
	// accessed.

	// first check the memtable
	if val, found := db.memtable.Get(key); found {
		return val
	}

	// then check each sstable from new to old
	for i := len(db.store) - 1; i >= 0; i-- {
		val, found, err := db.store[i].Get(key)
		if err != nil {
			panic(err)
		}

		if found {
			return val
		}
	}

	return ""
}

func (db *DB) MergeAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for len(db.store) > 1 {
		sst1 := db.store[len(db.store)-2]
		sst2 := db.store[len(db.store)-1]
		merged := db.getNextFilename()
		if err := store.Merge(sst1, sst2, merged); err != nil {
			return err
		}

		sstMerged, err := store.LoadSST(merged)
		if err != nil {
			return err
		}
		db.store = append(db.store[:len(db.store)-2], sstMerged)

		sst1.Delete()
		sst2.Delete()
	}

	return nil
}

// Flush saves the memtable to disk and clear it
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	filename := db.getNextFilename()
	if err := store.WriteFile(filename, db.memtable); err != nil {
		return err
	}

	db.memtable = &store.Treap{}

	sst, err := store.LoadSST(filename)
	if err != nil {
		return err
	}

	db.store = append(db.store, sst)

	return db.wal.Reset()
}

func (db *DB) LoadSSTables() error {
	var max int32
	err := filepath.WalkDir(db.dirname, func(path string, d fs.DirEntry, err error) error {
		var num int32
		n, _ := fmt.Sscanf(d.Name(), "data_%d.sst", &num)
		if n == 1 {
			if num > max {
				max = num
			}
			sst, err := store.LoadSST(path)
			if err != nil {
				return err
			}

			db.store = append(db.store, sst)
		}
		return nil
	})

	atomic.StoreInt32(&db.fileCount, max)
	return err
}

func (db *DB) getNextFilename() string {
	cnt := atomic.AddInt32(&db.fileCount, 1)
	return filepath.Join(db.dirname, fmt.Sprintf("data_%04d.sst", cnt))
}
