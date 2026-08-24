package v2

import (
	"errors"
	"io"
	"testing"
)

type partialWriterAt struct {
	data      []byte
	maxWrite  int
	failAfter int
}

func (w *partialWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	if w.failAfter >= 0 && int(offset) >= w.failAfter {
		return 0, io.ErrClosedPipe
	}
	n := len(p)
	if n > w.maxWrite {
		n = w.maxWrite
	}
	end := int(offset) + n
	if end > len(w.data) {
		w.data = append(w.data, make([]byte, end-len(w.data))...)
	}
	copy(w.data[offset:], p[:n])
	return n, nil
}

func TestWriteFullAtContinuesPartialWrites(t *testing.T) {
	w := &partialWriterAt{maxWrite: 3, failAfter: -1}
	written, err := writeFullAt(w, []byte("abcdefgh"), 2)
	if err != nil || written != 8 || string(w.data[2:]) != "abcdefgh" {
		t.Fatalf("write = %d, %v, %q", written, err, w.data)
	}
}

func TestWriteFullAtReportsPartialFailure(t *testing.T) {
	w := &partialWriterAt{maxWrite: 3, failAfter: 3}
	written, err := writeFullAt(w, []byte("abcdefgh"), 0)
	if written != 3 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write = %d, %v", written, err)
	}
}
