package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

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

func CreateActiveData(root string, uuid base.StoreUUID, id base.DataSegmentID, firstSeq base.FrameSeq, segmentSize, maxPayloadSize uint64) (*ActiveData, error) {
	if uuid == (base.StoreUUID{}) || id == 0 || firstSeq == 0 {
		return nil, base.ErrInvalidConfig
	}
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{
		Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: uint32(id), FirstSeq: uint64(firstSeq), CreatedUnixNano: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, "data", ActiveDataFileName(id))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := writeFullAt(file, header[:], 0); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, err
	}
	return OpenActiveData(root, uuid, id, segmentSize, maxPayloadSize)
}

// ResumeSeal completes an interrupted Active->Sealed transition. It recognizes
// a complete terminal SegmentSeal even when the footer or rename was lost.
func ResumeSeal(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, fallbackNext base.FrameSeq) (storeformat.FileSummary, error) {
	path := filepath.Join(root, "data", ActiveDataFileName(id))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return storeformat.FileSummary{}, errors.Join(err, file.Close())
	}
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return storeformat.FileSummary{}, errors.Join(err, file.Close())
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil || header.Kind != storeformat.SegmentKindData || header.StoreUUID != uuid || header.FileID != uint32(id) {
		return storeformat.FileSummary{}, errors.Join(err, file.Close(), base.ErrCorrupt)
	}
	physicalEnd := uint64(info.Size())
	contentLimit := segmentSize - storeformat.SegmentFooterSize
	offset := uint64(storeformat.SegmentHeaderSize)
	var count uint64
	var first, previous base.FrameSeq
	for offset+storeformat.FrameHeaderSize <= physicalEnd && offset < contentLimit {
		headerBytes := make([]byte, storeformat.FrameHeaderSize)
		if _, err := file.ReadAt(headerBytes, int64(offset)); err != nil {
			break
		}
		limits := storeformat.FrameLimits{MaxPayloadSize: maxPayloadSize, RemainingSegmentSize: contentLimit - offset}
		frameHeader, err := storeformat.DecodeFrameHeader(headerBytes, limits)
		if err != nil || frameHeader.TotalSize > physicalEnd-offset {
			break
		}
		total, err := base.Uint64ToInt(frameHeader.TotalSize)
		if err != nil {
			return storeformat.FileSummary{}, errors.Join(err, file.Close())
		}
		encoded := make([]byte, total)
		if _, err := file.ReadAt(encoded, int64(offset)); err != nil {
			break
		}
		frame, _, err := storeformat.DecodeFrame(encoded, limits)
		if err != nil {
			break
		}
		if count == 0 {
			first = frame.FrameSeq
			if first != base.FrameSeq(header.FirstSeq) {
				return storeformat.FileSummary{}, errors.Join(base.ErrCorrupt, file.Close())
			}
		} else if frame.FrameSeq <= previous {
			return storeformat.FileSummary{}, errors.Join(base.ErrCorrupt, file.Close())
		}
		count++
		previous = frame.FrameSeq
		offset += frameHeader.TotalSize
		if frame.Type != storeformat.FrameTypeSegmentSeal {
			continue
		}
		seal, err := storeformat.DecodeSegmentSealFrame(frame)
		if err != nil || seal.SegmentID != id || seal.ValidDataEnd != offset || seal.FirstFrameSeq != first || seal.FrameCount != count {
			return storeformat.FileSummary{}, errors.Join(err, file.Close(), base.ErrCorrupt)
		}
		footer, err := storeformat.EncodeDataSegmentFooter(storeformat.DataSegmentFooter(seal))
		if err != nil {
			return storeformat.FileSummary{}, errors.Join(err, file.Close())
		}
		if err := file.Truncate(int64(seal.ValidDataEnd)); err != nil {
			return storeformat.FileSummary{}, errors.Join(err, file.Close())
		}
		if _, err := writeFullAt(file, footer[:], int64(seal.ValidDataEnd)); err != nil {
			return storeformat.FileSummary{}, errors.Join(err, file.Close())
		}
		if err := file.Sync(); err != nil {
			return storeformat.FileSummary{}, errors.Join(err, file.Close())
		}
		if err := file.Close(); err != nil {
			return storeformat.FileSummary{}, err
		}
		if err := os.Rename(path, filepath.Join(root, "data", SealedDataFileName(id))); err != nil {
			return storeformat.FileSummary{}, err
		}
		dir, err := os.Open(filepath.Join(root, "data"))
		if err != nil {
			return storeformat.FileSummary{}, err
		}
		if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
			return storeformat.FileSummary{}, err
		}
		return storeformat.FileSummary{FileID: uint32(id), ValidEnd: seal.ValidDataEnd, FirstSeq: uint64(seal.FirstFrameSeq), LastSeq: uint64(seal.LastFrameSeq)}, nil
	}
	if err := file.Close(); err != nil {
		return storeformat.FileSummary{}, err
	}
	if previous >= fallbackNext {
		if previous == base.FrameSeq(math.MaxUint64) {
			return storeformat.FileSummary{}, base.ErrGenerationExhausted
		}
		fallbackNext = previous + 1
	}
	active, err := OpenActiveData(root, uuid, id, segmentSize, maxPayloadSize)
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	return active.Seal(fallbackNext)
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

func (s *ActiveData) ReadFrameHeader(addr base.VAddr) (storeformat.FrameHeader, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return storeformat.FrameHeader{}, base.ErrClosed
	}
	if addr.SegmentID() != s.segmentID || addr.Offset() < storeformat.SegmentHeaderSize || uint64(addr.Offset()) >= s.end {
		s.mu.Unlock()
		return storeformat.FrameHeader{}, fmt.Errorf("read address outside active segment: %w", base.ErrInvalidAddress)
	}
	end := s.end
	file := s.file
	s.mu.Unlock()
	return readFrameHeaderAt(file, uint64(addr.Offset()), end, s.maxPayloadSize)
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

// Scan visits the complete frame prefix present when the scan starts. It does
// not hold the ActiveData mutex while decoding or invoking the callback, so a
// recovery resolver may perform validated random reads from the same file.
func (s *ActiveData) Scan(visit func(base.VAddr, storeformat.Frame) error) error {
	if visit == nil {
		return fmt.Errorf("nil scan visitor: %w", base.ErrInvalidConfig)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return base.ErrClosed
	}
	file := s.file
	end := s.end
	segmentID := s.segmentID
	maxPayloadSize := s.maxPayloadSize
	segmentSize := s.segmentSize
	s.mu.Unlock()
	headerBytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := storeformat.DecodeSegmentHeader(headerBytes)
	if err != nil {
		return err
	}
	_, _, err = scanDataFrames(file, end, segmentSize-storeformat.SegmentFooterSize, maxPayloadSize, base.FrameSeq(header.FirstSeq), true, func(offset uint64, frame storeformat.Frame) error {
		addr, err := base.NewVAddr(segmentID, uint32(offset))
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
