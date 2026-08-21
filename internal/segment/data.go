package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

var (
	ErrFull     = errors.New("ridstore: active segment full")
	ErrPoisoned = errors.New("ridstore: active segment append state uncertain")
)

type ActiveData struct {
	mu             sync.Mutex
	file           *os.File
	storeUUID      base.StoreUUID
	segmentID      base.DataSegmentID
	segmentSize    uint64
	maxPayloadSize uint64
	end            uint64
	poisoned       bool
	closed         bool
}

func ActiveDataFileName(id base.DataSegmentID) string {
	return fmt.Sprintf("DATA-%08d.active", id)
}

func OpenActiveData(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64) (*ActiveData, error) {
	if uuid == (base.StoreUUID{}) || id == 0 || segmentSize <= storeformat.SegmentHeaderSize+storeformat.SegmentFooterSize || maxPayloadSize == 0 {
		return nil, fmt.Errorf("active data configuration: %w", base.ErrInvalidConfig)
	}
	path := filepath.Join(root, "data", ActiveDataFileName(id))
	fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open active data segment")
	}
	fail := func(err error) (*ActiveData, error) {
		return nil, errors.Join(err, file.Close())
	}
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Size() < storeformat.SegmentHeaderSize || uint64(info.Size()) > segmentSize-storeformat.SegmentFooterSize {
		return fail(fmt.Errorf("active data size: %w", base.ErrCorrupt))
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return fail(err)
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return fail(err)
	}
	if header.Kind != storeformat.SegmentKindData || header.StoreUUID != uuid || header.FileID != uint32(id) {
		return fail(fmt.Errorf("active data header identity: %w", base.ErrCorrupt))
	}
	active := &ActiveData{
		file: file, storeUUID: uuid, segmentID: id, segmentSize: segmentSize,
		maxPayloadSize: maxPayloadSize, end: uint64(info.Size()),
	}
	validEnd, _, err := scanDataFrames(file, uint64(info.Size()), segmentSize-storeformat.SegmentFooterSize, maxPayloadSize, base.FrameSeq(header.FirstSeq), false, nil)
	if err != nil {
		return fail(err)
	}
	if validEnd != uint64(info.Size()) {
		if err := file.Truncate(int64(validEnd)); err != nil {
			return fail(err)
		}
		if err := file.Sync(); err != nil {
			return fail(err)
		}
		active.end = validEnd
	}
	return active, nil
}

func (s *ActiveData) Append(frame storeformat.Frame) (base.VAddr, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, 0, base.ErrClosed
	}
	if s.poisoned {
		return 0, 0, ErrPoisoned
	}
	encoded, err := storeformat.EncodeFrame(frame, s.maxPayloadSize)
	if err != nil {
		return 0, 0, err
	}
	encodedSize := uint64(len(encoded))
	contentLimit := s.segmentSize - storeformat.SegmentFooterSize
	if encodedSize > contentLimit-s.end {
		return 0, 0, ErrFull
	}
	offset := s.end
	written, err := writeFullAt(s.file, encoded, int64(offset))
	s.end += uint64(written)
	if err != nil {
		s.poisoned = true
		return 0, uint64(written), fmt.Errorf("append frame at %d: %w", offset, err)
	}
	addr, err := base.NewVAddr(s.segmentID, uint32(offset))
	if err != nil {
		s.poisoned = true
		return 0, encodedSize, err
	}
	return addr, encodedSize, nil
}

func (s *ActiveData) ReadFrame(addr base.VAddr) (storeformat.Frame, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return storeformat.Frame{}, base.ErrClosed
	}
	if addr.SegmentID() != s.segmentID || addr.Offset() < storeformat.SegmentHeaderSize || uint64(addr.Offset()) >= s.end {
		s.mu.Unlock()
		return storeformat.Frame{}, fmt.Errorf("read address outside active segment: %w", base.ErrInvalidAddress)
	}
	end := s.end
	file := s.file
	s.mu.Unlock()

	offset := uint64(addr.Offset())
	headerBytes := make([]byte, storeformat.FrameHeaderSize)
	if _, err := file.ReadAt(headerBytes, int64(offset)); err != nil {
		return storeformat.Frame{}, err
	}
	limits := storeformat.FrameLimits{MaxPayloadSize: s.maxPayloadSize, RemainingSegmentSize: end - offset}
	header, err := storeformat.DecodeFrameHeader(headerBytes, limits)
	if err != nil {
		return storeformat.Frame{}, err
	}
	total, err := base.Uint64ToInt(header.TotalSize)
	if err != nil {
		return storeformat.Frame{}, fmt.Errorf("frame allocation: %w", base.ErrCorrupt)
	}
	encoded := make([]byte, total)
	if _, err := file.ReadAt(encoded, int64(offset)); err != nil {
		return storeformat.Frame{}, err
	}
	frame, consumed, err := storeformat.DecodeFrame(encoded, limits)
	if err != nil {
		return storeformat.Frame{}, err
	}
	if consumed != total {
		return storeformat.Frame{}, fmt.Errorf("frame consumed size: %w", base.ErrCorrupt)
	}
	return frame, nil
}

func (s *ActiveData) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	if s.poisoned {
		return ErrPoisoned
	}
	return s.file.Sync()
}

func (s *ActiveData) End() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end
}

func (s *ActiveData) SegmentID() base.DataSegmentID { return s.segmentID }

// Scan visits every complete frame currently present in the Active segment.
// The callback must not call methods on this ActiveData.
func (s *ActiveData) Scan(visit func(base.VAddr, storeformat.Frame) error) error {
	if visit == nil {
		return fmt.Errorf("nil scan visitor: %w", base.ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := s.file.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	_, _, err = scanDataFrames(s.file, s.end, s.segmentSize-storeformat.SegmentFooterSize, s.maxPayloadSize, base.FrameSeq(header.FirstSeq), true, func(offset uint64, frame storeformat.Frame) error {
		addr, err := base.NewVAddr(s.segmentID, uint32(offset))
		if err != nil {
			return err
		}
		return visit(addr, frame)
	})
	return err
}

func (s *ActiveData) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	s.closed = true
	return s.file.Close()
}

func writeFullAt(file *os.File, data []byte, offset int64) (int, error) {
	written := 0
	for written < len(data) {
		n, err := file.WriteAt(data[written:], offset+int64(written))
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
