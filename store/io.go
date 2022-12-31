package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type fileReader struct {
	f      io.ReadSeeker
	r      *bufio.Reader
	offset int64
}

func newReader(rs io.ReadSeeker) *fileReader {
	return &fileReader{
		f: rs,
		r: bufio.NewReader(rs),
	}
}

func (rd *fileReader) ReadUint64() (uint64, error) {
	var v uint64
	err := binary.Read(rd.r, binary.LittleEndian, &v)
	rd.offset += 8
	return v, err
}

func (rd *fileReader) ReadString() (string, error) {
	var v uint64
	if err := binary.Read(rd.r, binary.LittleEndian, &v); err != nil {
		return "", err
	}
	rd.offset += 8

	buf := make([]byte, v)
	n, err := io.ReadFull(rd.r, buf)
	rd.offset += int64(n)
	if uint64(n) < v {
		return "", fmt.Errorf("short read")
	}
	return string(buf), err
}

func (rd *fileReader) Offset() int64 {
	return rd.offset
}

func (rd *fileReader) SeekTo(offset int64) error {
	rd.offset = offset
	_, err := rd.f.Seek(offset, io.SeekStart)
	rd.r.Reset(rd.f)
	return err
}

// ---

type fileWriter struct {
	w *bufio.Writer
}

func newWriter(w io.WriteSeeker) *fileWriter {
	return &fileWriter{
		w: bufio.NewWriter(w),
	}
}

func (ow *fileWriter) WriteUint64(v uint64) error {
	return binary.Write(ow.w, binary.LittleEndian, v)
}

func (ow *fileWriter) WriteString(s string) error {
	if err := binary.Write(ow.w, binary.LittleEndian, uint64(len(s))); err != nil {
		return err
	}

	if _, err := ow.w.WriteString(s); err != nil {
		return err
	}

	return nil
}

func (ow *fileWriter) Flush() error {
	return ow.w.Flush()
}
