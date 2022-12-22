package db

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/jrouviere/minikv/store"
)

// TODO: implement load existing file

type DB struct {
	dirname   string
	fileCount int32

	mu sync.RWMutex
	// from earliest to latest sstable
	store    []*store.SSTable
	memtable map[string]string
	wal      *store.WAL
}

func New(dirname string) (*DB, error) {
	wal, err := store.NewWAL(filepath.Join(dirname, "wal.dat"))
	if err != nil {
		return nil, err
	}

	return &DB{
		dirname:  dirname,
		memtable: make(map[string]string),
		wal:      wal,
	}, nil
}

func (db *DB) Set(key, value string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Commit(key, value); err != nil {
		panic(err)
	}

	// store in memtable
	db.memtable[key] = value
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
	if val, found := db.memtable[key]; found {
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

		sstMerged, err := store.Load(merged)
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

	for k := range db.memtable {
		delete(db.memtable, k)
	}

	sst, err := store.Load(filename)
	if err != nil {
		return err
	}

	db.store = append(db.store, sst)

	return db.wal.Reset()
}

func (db *DB) getNextFilename() string {
	cnt := atomic.AddInt32(&db.fileCount, 1)
	return filepath.Join(db.dirname, fmt.Sprintf("data_%04d.sst", cnt))
}
