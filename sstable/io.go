package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type sstReader struct {
	r io.ReadSeeker
}

func newReader(rd io.ReadSeeker) *sstReader {
	return &sstReader{
		r: rd,
	}
}

func (rd *sstReader) ReadUint64() (uint64, error) {
	var v uint64
	err := binary.Read(rd.r, binary.LittleEndian, &v)
	return v, err
}

func (rd *sstReader) ReadString() (string, error) {
	var v uint64
	if err := binary.Read(rd.r, binary.LittleEndian, &v); err != nil {
		return "", err
	}

	buf := make([]byte, v)
	n, err := rd.r.Read(buf)
	if uint64(n) < v {
		return "", fmt.Errorf("short read")
	}
	return string(buf), err
}

func (rd *sstReader) Offset() int64 {
	offset, err := rd.r.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}
	return offset
}

func (rd *sstReader) Seek(offset int64) (int64, error) {
	return rd.r.Seek(offset, io.SeekStart)
}

type sstWriter struct {
	w *bufio.Writer
}

func newWriter(w io.WriteSeeker) *sstWriter {
	return &sstWriter{
		w: bufio.NewWriter(w),
	}
}

func (ow *sstWriter) WriteUint64(v uint64) error {
	return binary.Write(ow.w, binary.LittleEndian, v)
}

func (ow *sstWriter) WriteString(s string) error {
	if err := binary.Write(ow.w, binary.LittleEndian, uint64(len(s))); err != nil {
		return err
	}

	if _, err := ow.w.WriteString(s); err != nil {
		return err
	}

	return nil
}

func (ow *sstWriter) Flush() error {
	return ow.w.Flush()
}
