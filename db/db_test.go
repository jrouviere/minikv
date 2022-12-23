package db

import (
	"os"
	"strconv"
	"testing"
)

func setup(b *testing.B) string {
	dir, err := os.MkdirTemp("", "minidb-tests")
	if err != nil {
		b.Fatalf("cannot create temp dir: %v", err)
	}
	return dir
}

func teardown(b *testing.B, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("unable to removeAll: %s: %+v", dir, err)
	}
}

func BenchmarkSet(b *testing.B) {
	tmpDir := setup(b)
	defer teardown(b, tmpDir)

	db, err := New(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Set("key_"+strconv.Itoa(i), "some test data")
	}
}

func BenchmarkFlush(b *testing.B) {
	tmpDir := setup(b)
	defer teardown(b, tmpDir)

	db, err := New(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for k := 0; k < 1000; k++ {
			db.Set("key_"+strconv.Itoa(k), "some test data")
		}
		b.StartTimer()
		db.Flush()
	}
}

func BenchmarkGet(b *testing.B) {
	tmpDir := setup(b)
	defer teardown(b, tmpDir)

	db, err := New(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	for i := 0; i < 1e6; i++ {
		db.Set("key_"+strconv.Itoa(i), "some test data")
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	db.Set("memtable", "some test data")

	b.Run("get", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = db.Get("key_1000")
		}
	})
	b.Run("memtable", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = db.Get("memtable")
		}
	})
	b.Run("notfound", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = db.Get("notfound")
		}
	})
}

func BenchmarkAll(b *testing.B) {
	tmpDir := setup(b)
	defer teardown(b, tmpDir)

	db, err := New(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			db.Set("key_"+strconv.Itoa(i)+"_"+strconv.Itoa(j), "data")
		}
		if err := db.Flush(); err != nil {
			b.Fatal(err)
		}

		db.Get("test")
	}
}
