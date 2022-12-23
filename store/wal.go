package store

import (
	"io"
	"os"
)

func LoadWAL(filename string) (*Treap, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	rd := newReader(f)

	var memtable Treap

	for {
		key, err := rd.ReadString()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		val, err := rd.ReadString()
		if err != nil {
			return nil, err
		}
		memtable.Upsert(key, val)
	}

	return &memtable, nil
}

type WAL struct {
	file *os.File
	wr   *fileWriter
}

func NewWAL(filename string) (*WAL, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		wr:   newWriter(f),
	}, nil
}

func (w *WAL) Commit(key, value string) error {
	if err := w.wr.WriteString(key); err != nil {
		return err
	}
	if err := w.wr.WriteString(value); err != nil {
		return err
	}
	return w.wr.Flush()
}

func (w *WAL) Reset() error {
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	_, err := w.file.Seek(0, io.SeekStart)
	return err
}
