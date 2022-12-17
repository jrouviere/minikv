package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
)

type offsetWriter struct {
	w   *bufio.Writer
	pos uint64
}

func newWriter(w io.WriteSeeker) *offsetWriter {
	return &offsetWriter{
		w: bufio.NewWriter(w),
	}
}

func (ow *offsetWriter) Offset() uint64 {
	return ow.pos
}

func (ow *offsetWriter) Write(p []byte) (n int, err error) {
	ow.pos += uint64(len(p))
	return ow.w.Write(p)
}

func (ow *offsetWriter) WriteUint64(v uint64) error {
	return binary.Write(ow, binary.LittleEndian, v)
}

func (ow *offsetWriter) WriteString(s string) error {
	if err := binary.Write(ow, binary.LittleEndian, uint64(len(s))); err != nil {
		return err
	}

	if _, err := ow.Write([]byte(s)); err != nil {
		return err
	}

	return nil
}

func (ow *offsetWriter) Flush() error {
	return ow.w.Flush()
}
