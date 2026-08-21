package segment

import (
	"encoding/binary"
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

	return readFrameAt(file, uint64(addr.Offset()), end, s.maxPayloadSize)
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

func (s *ActiveData) Remaining() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.end >= s.segmentSize-storeformat.SegmentFooterSize {
		return 0
	}
	return s.segmentSize - storeformat.SegmentFooterSize - s.end
}

func (s *ActiveData) SegmentID() base.DataSegmentID { return s.segmentID }

// Seal appends the terminal SegmentSeal, persists the footer, and atomically
// renames the file to its immutable name. The caller must serialize this with
// all appends (the append sequencer does so in production).
func (s *ActiveData) Seal(nextFrameSeq base.FrameSeq) (storeformat.FileSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storeformat.FileSummary{}, base.ErrClosed
	}
	if s.poisoned {
		return storeformat.FileSummary{}, ErrPoisoned
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := s.file.ReadAt(headerBytes, 0); err != nil {
		return storeformat.FileSummary{}, err
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	var minCommit, maxCommit base.CommitSeq
	validEnd, scanned, err := scanDataFrames(s.file, s.end, s.segmentSize-storeformat.SegmentFooterSize, s.maxPayloadSize, base.FrameSeq(header.FirstSeq), true, func(_ uint64, frame storeformat.Frame) error {
		if frame.Type != storeformat.FrameTypeCommitSeal && frame.Type != storeformat.FrameTypeRelocationSeal {
			return nil
		}
		if len(frame.Payload) != storeformat.DescriptorSealSize {
			return fmt.Errorf("descriptor seal size during rotation: %w", base.ErrCorrupt)
		}
		seq := base.CommitSeq(binary.LittleEndian.Uint64(frame.Payload[:8]))
		if seq == 0 || (maxCommit != 0 && seq <= maxCommit) {
			return fmt.Errorf("descriptor commit sequence during rotation: %w", base.ErrCorrupt)
		}
		if minCommit == 0 {
			minCommit = seq
		}
		maxCommit = seq
		return nil
	})
	if err != nil || validEnd != s.end {
		return storeformat.FileSummary{}, err
	}
	firstSeq := base.FrameSeq(header.FirstSeq)
	if scanned.FrameCount != 0 {
		if nextFrameSeq <= scanned.LastFrameSeq {
			return storeformat.FileSummary{}, fmt.Errorf("segment seal frame sequence: %w", base.ErrCorrupt)
		}
		firstSeq = scanned.FirstFrameSeq
	} else if nextFrameSeq != firstSeq {
		return storeformat.FileSummary{}, fmt.Errorf("empty segment first sequence: %w", base.ErrCorrupt)
	}
	const sealFrameBytes = uint64(storeformat.FrameHeaderSize + 64)
	sealEnd, err := base.AddUint64(s.end, sealFrameBytes)
	if err != nil || sealEnd > s.segmentSize-storeformat.SegmentFooterSize {
		return storeformat.FileSummary{}, ErrFull
	}
	payload, err := storeformat.EncodeSegmentSealPayload(storeformat.SegmentSealPayload{
		SegmentID: s.segmentID, ValidDataEnd: sealEnd, FirstFrameSeq: firstSeq, LastFrameSeq: nextFrameSeq,
		FrameCount: scanned.FrameCount + 1, MinCommitSeq: minCommit, MaxCommitSeq: maxCommit,
	})
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	encoded, err := storeformat.EncodeFrame(storeformat.Frame{Type: storeformat.FrameTypeSegmentSeal, FrameSeq: nextFrameSeq, Payload: payload[:]}, s.maxPayloadSize)
	if err != nil || uint64(len(encoded)) != sealFrameBytes {
		return storeformat.FileSummary{}, errors.Join(err, base.ErrCorrupt)
	}
	written, err := writeFullAt(s.file, encoded, int64(s.end))
	s.end += uint64(written)
	if err != nil || uint64(written) != sealFrameBytes {
		s.poisoned = true
		return storeformat.FileSummary{}, errors.Join(err, io.ErrShortWrite)
	}
	if err := s.file.Sync(); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	footer, err := storeformat.EncodeDataSegmentFooter(storeformat.DataSegmentFooter{
		SegmentID: s.segmentID, ValidDataEnd: sealEnd, FirstFrameSeq: firstSeq, LastFrameSeq: nextFrameSeq,
		FrameCount: scanned.FrameCount + 1, MinCommitSeq: minCommit, MaxCommitSeq: maxCommit,
	})
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	if _, err := writeFullAt(s.file, footer[:], int64(sealEnd)); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	if err := s.file.Sync(); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	activePath := s.file.Name()
	if err := s.file.Close(); err != nil {
		s.closed = true
		return storeformat.FileSummary{}, err
	}
	s.closed = true
	sealedPath := filepath.Join(filepath.Dir(activePath), SealedDataFileName(s.segmentID))
	if err := os.Rename(activePath, sealedPath); err != nil {
		return storeformat.FileSummary{}, err
	}
	dir, err := os.Open(filepath.Dir(activePath))
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	err = errors.Join(dir.Sync(), dir.Close())
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	return storeformat.FileSummary{FileID: uint32(s.segmentID), ValidEnd: sealEnd, FirstSeq: uint64(firstSeq), LastSeq: uint64(nextFrameSeq)}, nil
}

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
