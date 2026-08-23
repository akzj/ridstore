package v2

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type segment struct {
	file    *os.File
	path    string
	header  segmentHeader
	end     uint64
	first   VAddr
	last    VAddr
	records uint64
	hook    func(FaultPoint) error
}

func activeSegmentName(id uint32) string { return fmt.Sprintf("segment-%08d.active", id) }
func sealedSegmentName(id uint32) string { return fmt.Sprintf("segment-%08d.sealed", id) }

func createSegment(dir string, id, previous uint32, size uint64, idValue logID, hook func(FaultPoint) error) (*segment, error) {
	if id == 0 {
		return nil, ErrInvalidConfig
	}
	h := segmentHeader{LogID: idValue, SegmentID: id, PreviousSegment: previous, SegmentSize: size}
	encoded, err := encodeSegmentHeader(h)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, activeSegmentName(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*segment, error) {
		return nil, errors.Join(cause, f.Close())
	}
	if _, err := writeFullAt(f, encoded[:], 0); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := syncDir(dir); err != nil {
		return fail(err)
	}
	return &segment{file: f, path: path, header: h, end: segmentHeaderSize, hook: hook}, nil
}

func (s *segment) remaining() uint64 {
	limit := s.header.SegmentSize - segmentFooterSize
	if s.end >= limit {
		return 0
	}
	return limit - s.end
}

func (s *segment) appendEncoded(records []pendingRecord) (uint64, error) {
	if len(records) == 0 {
		return 0, nil
	}
	var total uint64
	for i := range records {
		if total > s.remaining() || records[i].addr.SegmentID() != s.header.SegmentID || uint64(records[i].addr.Offset()) != s.end+total || uint64(len(records[i].encoded)) > s.remaining()-total {
			return 0, fmt.Errorf("append plan: %w", ErrCorrupt)
		}
		total += uint64(len(records[i].encoded))
	}
	buf := make([]byte, 0, total)
	for i := range records {
		buf = append(buf, records[i].encoded...)
	}
	if err := hitFault(s.hook, FaultBeforeAppendWrite); err != nil {
		return 0, err
	}
	written, err := writeFullAt(s.file, buf, int64(s.end))
	if err != nil {
		return uint64(written), err
	}
	for i := range records {
		if s.records == 0 {
			s.first = records[i].addr
		}
		s.last = records[i].addr
		s.records++
	}
	s.end += total
	return total, nil
}

func (s *segment) sync() error {
	if err := hitFault(s.hook, FaultBeforeSync); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *segment) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *segment) seal(dir string) error {
	footer, err := encodeSegmentFooter(segmentFooter{
		SegmentID: s.header.SegmentID, DataEnd: s.end, FirstAddr: s.first, LastAddr: s.last, RecordCount: s.records,
	})
	if err != nil {
		return err
	}
	if s.end+segmentFooterSize > s.header.SegmentSize {
		return fmt.Errorf("segment footer capacity: %w", ErrCorrupt)
	}
	if err := hitFault(s.hook, FaultBeforeFooterWrite); err != nil {
		return err
	}
	if _, err := writeFullAt(s.file, footer[:], int64(s.end)); err != nil {
		return err
	}
	if err := hitFault(s.hook, FaultBeforeFooterSync); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	if err := s.close(); err != nil {
		return err
	}
	dst := filepath.Join(dir, sealedSegmentName(s.header.SegmentID))
	if err := hitFault(s.hook, FaultBeforeRename); err != nil {
		return err
	}
	if err := os.Rename(s.path, dst); err != nil {
		return err
	}
	return syncDir(dir)
}

func writeFullAt(w io.WriterAt, data []byte, offset int64) (int, error) {
	written := 0
	for written < len(data) {
		n, err := w.WriteAt(data[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
