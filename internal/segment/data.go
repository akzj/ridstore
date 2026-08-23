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
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

var (
	ErrFull     = errors.New("ridstore: active segment full")
	ErrPoisoned = errors.New("ridstore: active segment append state uncertain")
)

const (
	PointBeforeAppendWrite failpoint.Point = "segment.before-append-write"
	PointBeforeSync        failpoint.Point = "segment.before-sync"
	PointBeforeSealWrite   failpoint.Point = "segment.before-seal-write"
	PointBeforeSealSync    failpoint.Point = "segment.before-seal-sync"
	PointBeforeFooterWrite failpoint.Point = "segment.before-footer-write"
	PointBeforeFooterSync  failpoint.Point = "segment.before-footer-sync"
	PointBeforeSealRename  failpoint.Point = "segment.before-seal-rename"
	PointBeforeDataDirSync failpoint.Point = "segment.before-data-dir-sync"

	PointBeforeCreateHeaderWrite failpoint.Point = "segment.before-create-header-write"
	PointBeforeCreateFileSync    failpoint.Point = "segment.before-create-file-sync"
	PointBeforeCreateDirSync     failpoint.Point = "segment.before-create-dir-sync"
	PointBeforeTailTruncate      failpoint.Point = "segment.before-tail-truncate"
	PointBeforeTailSync          failpoint.Point = "segment.before-tail-sync"
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
	hook           failpoint.Hook
}

func ActiveDataFileName(id base.DataSegmentID) string {
	return fmt.Sprintf("DATA-%08d.active", id)
}

func CreateActiveData(root string, uuid base.StoreUUID, id base.DataSegmentID, firstSeq base.FrameSeq, segmentSize, maxPayloadSize uint64) (*ActiveData, error) {
	return CreateActiveDataWithHook(root, uuid, id, firstSeq, segmentSize, maxPayloadSize, nil)
}

func CreateActiveDataWithHook(root string, uuid base.StoreUUID, id base.DataSegmentID, firstSeq base.FrameSeq, segmentSize, maxPayloadSize uint64, hook failpoint.Hook) (*ActiveData, error) {
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
	if err := failpoint.Hit(hook, PointBeforeCreateHeaderWrite); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if _, err := writeFullAt(file, header[:], 0); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeCreateFileSync); err != nil {
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
	if err := failpoint.Hit(hook, PointBeforeCreateDirSync); err != nil {
		return nil, errors.Join(err, dir.Close())
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return nil, err
	}
	return openActiveData(root, uuid, id, segmentSize, maxPayloadSize, hook, false)
}

// ResumeSeal completes an interrupted Active->Sealed transition. It recognizes
// a complete terminal SegmentSeal even when the footer or rename was lost.
func ResumeSeal(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, fallbackNext base.FrameSeq) (storeformat.FileSummary, error) {
	return ResumeSealWithHook(root, uuid, id, segmentSize, maxPayloadSize, fallbackNext, nil)
}

func ResumeSealWithHook(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, fallbackNext base.FrameSeq, hook failpoint.Hook) (storeformat.FileSummary, error) {
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
	active, err := OpenActiveDataWithHook(root, uuid, id, segmentSize, maxPayloadSize, hook)
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	return active.Seal(fallbackNext)
}

func OpenActiveData(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64) (*ActiveData, error) {
	return OpenActiveDataWithHook(root, uuid, id, segmentSize, maxPayloadSize, nil)
}

func OpenActiveDataWithHook(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, hook failpoint.Hook) (*ActiveData, error) {
	return openActiveData(root, uuid, id, segmentSize, maxPayloadSize, hook, true)
}

// OpenUnpublishedActiveDataWithHook validates a rotation destination whose
// durability is established explicitly by EnsureCreationDurable.
func OpenUnpublishedActiveDataWithHook(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, hook failpoint.Hook) (*ActiveData, error) {
	return openActiveData(root, uuid, id, segmentSize, maxPayloadSize, hook, false)
}

func openActiveData(root string, uuid base.StoreUUID, id base.DataSegmentID, segmentSize, maxPayloadSize uint64, hook failpoint.Hook, syncOnCleanOpen bool) (*ActiveData, error) {
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
		maxPayloadSize: maxPayloadSize, end: uint64(info.Size()), hook: hook,
	}
	validEnd, _, err := scanDataFrames(file, uint64(info.Size()), segmentSize-storeformat.SegmentFooterSize, maxPayloadSize, base.FrameSeq(header.FirstSeq), false, nil)
	if err != nil {
		return fail(err)
	}
	if validEnd != uint64(info.Size()) {
		if err := failpoint.Hit(hook, PointBeforeTailTruncate); err != nil {
			return fail(err)
		}
		if err := file.Truncate(int64(validEnd)); err != nil {
			return fail(err)
		}
		active.end = validEnd
		if err := failpoint.Hit(hook, PointBeforeTailSync); err != nil {
			return fail(err)
		}
		if err := file.Sync(); err != nil {
			return fail(err)
		}
	} else if syncOnCleanOpen {
		// A previous Open may have completed truncate but failed its fsync. With
		// no durable repair marker, every later Open must resync the clean prefix
		// before accepting it as the Active append boundary.
		if err := file.Sync(); err != nil {
			return fail(err)
		}
	}
	return active, nil
}

// EnsureCreationDurable re-establishes file and directory durability for an
// already-valid, unpublished empty Active created by interrupted rotation.
func (s *ActiveData) EnsureCreationDurable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	if s.end != storeformat.SegmentHeaderSize {
		return fmt.Errorf("unpublished active is not empty: %w", base.ErrCorrupt)
	}
	if err := failpoint.Hit(s.hook, PointBeforeCreateFileSync); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.file.Name()))
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeCreateDirSync); err != nil {
		return errors.Join(err, dir.Close())
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (s *ActiveData) Append(frame storeformat.Frame) (base.VAddr, uint64, error) {
	results, _, err := s.AppendBatch([]storeformat.Frame{frame})
	if err != nil {
		return 0, 0, err
	}
	if len(results) != 1 {
		return 0, 0, base.ErrCorrupt
	}
	return results[0].Addr, results[0].Size, nil
}

type AppendResult struct {
	Addr base.VAddr
	Size uint64
}

// AppendBatch encodes complete Frames into one contiguous buffer and writes
// that buffer with one logical append. A short write poisons the Active file;
// callers must not publish any result from the failed group.
func (s *ActiveData) AppendBatch(frames []storeformat.Frame) ([]AppendResult, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, base.ErrClosed
	}
	if s.poisoned {
		return nil, 0, ErrPoisoned
	}
	if len(frames) == 0 {
		return nil, 0, base.ErrInvalidConfig
	}
	encodedSizes := make([]uint64, len(frames))
	var total uint64
	for i, frame := range frames {
		size, err := storeformat.EncodedFrameSize(frame, s.maxPayloadSize)
		if err != nil {
			return nil, 0, err
		}
		if size > ^uint64(0)-total {
			return nil, 0, base.ErrInvalidConfig
		}
		encodedSizes[i] = size
		total += size
	}
	contentLimit := s.segmentSize - storeformat.SegmentFooterSize
	if total > contentLimit-s.end {
		return nil, 0, ErrFull
	}
	offset := s.end
	totalInt, err := base.Uint64ToInt(total)
	if err != nil {
		return nil, 0, err
	}
	buffer := make([]byte, totalInt)
	nextBuffer := 0
	for i, frame := range frames {
		size, err := base.Uint64ToInt(encodedSizes[i])
		if err != nil {
			return nil, 0, err
		}
		if _, err := storeformat.EncodeFrameTo(buffer[nextBuffer:nextBuffer+size], frame, s.maxPayloadSize); err != nil {
			return nil, 0, err
		}
		nextBuffer += size
	}
	if err := failpoint.Hit(s.hook, PointBeforeAppendWrite); err != nil {
		s.poisoned = true
		return nil, 0, err
	}
	written, err := writeFullAt(s.file, buffer, int64(offset))
	s.end += uint64(written)
	if err != nil {
		s.poisoned = true
		return nil, uint64(written), fmt.Errorf("append frame batch at %d: %w", offset, err)
	}
	results := make([]AppendResult, len(encodedSizes))
	next := offset
	for i, size := range encodedSizes {
		addr, err := base.NewVAddr(s.segmentID, uint32(next))
		if err != nil {
			s.poisoned = true
			return nil, total, err
		}
		results[i] = AppendResult{Addr: addr, Size: size}
		next += size
	}
	return results, total, nil
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
	if err := failpoint.Hit(s.hook, PointBeforeSync); err != nil {
		s.poisoned = true
		return err
	}
	if err := s.file.Sync(); err != nil {
		s.poisoned = true
		return err
	}
	return nil
}

func (s *ActiveData) SetHook(hook failpoint.Hook) {
	s.mu.Lock()
	s.hook = hook
	s.mu.Unlock()
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
	if err := failpoint.Hit(s.hook, PointBeforeSealWrite); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	written, err := writeFullAt(s.file, encoded, int64(s.end))
	s.end += uint64(written)
	if err != nil || uint64(written) != sealFrameBytes {
		s.poisoned = true
		return storeformat.FileSummary{}, errors.Join(err, io.ErrShortWrite)
	}
	if err := failpoint.Hit(s.hook, PointBeforeSealSync); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
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
	if err := failpoint.Hit(s.hook, PointBeforeFooterWrite); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	if _, err := writeFullAt(s.file, footer[:], int64(sealEnd)); err != nil {
		s.poisoned = true
		return storeformat.FileSummary{}, err
	}
	if err := failpoint.Hit(s.hook, PointBeforeFooterSync); err != nil {
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
	if err := failpoint.Hit(s.hook, PointBeforeSealRename); err != nil {
		return storeformat.FileSummary{}, err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return storeformat.FileSummary{}, err
	}
	dir, err := os.Open(filepath.Dir(activePath))
	if err != nil {
		return storeformat.FileSummary{}, err
	}
	if err := failpoint.Hit(s.hook, PointBeforeDataDirSync); err != nil {
		_ = dir.Close()
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
