package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type fileReader struct {
	r io.ReadSeeker
}

func newReader(rd io.ReadSeeker) *fileReader {
	return &fileReader{
		r: rd,
	}
}

func (rd *fileReader) ReadUint64() (uint64, error) {
	var v uint64
	err := binary.Read(rd.r, binary.LittleEndian, &v)
	return v, err
}

func (rd *fileReader) ReadString() (string, error) {
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

func (rd *fileReader) Offset() int64 {
	offset, err := rd.r.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}
	return offset
}

func (rd *fileReader) SeekTo(offset int64) (int64, error) {
	return rd.r.Seek(offset, io.SeekStart)
}

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
