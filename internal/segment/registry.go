package segment

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

// SealedData is an immutable, strictly validated Data Segment.
type SealedData struct {
	mu             sync.RWMutex
	file           *os.File
	segmentID      base.DataSegmentID
	validEnd       uint64
	maxPayloadSize uint64
	closed         bool
}

func OpenSealedData(root string, uuid base.StoreUUID, summary storeformat.FileSummary, segmentSize, maxPayloadSize uint64) (*SealedData, error) {
	if err := ValidateSealedData(root, uuid, summary, segmentSize, maxPayloadSize); err != nil {
		return nil, err
	}
	path := filepath.Join(root, "data", SealedDataFileName(base.DataSegmentID(summary.FileID)))
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open sealed data segment")
	}
	return &SealedData{file: file, segmentID: base.DataSegmentID(summary.FileID), validEnd: summary.ValidEnd, maxPayloadSize: maxPayloadSize}, nil
}

func (s *SealedData) SegmentID() base.DataSegmentID { return s.segmentID }

func (s *SealedData) ReadFrame(addr base.VAddr) (storeformat.Frame, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return storeformat.Frame{}, base.ErrClosed
	}
	if addr.SegmentID() != s.segmentID || addr.Offset() < storeformat.SegmentHeaderSize || uint64(addr.Offset()) >= s.validEnd {
		return storeformat.Frame{}, fmt.Errorf("read address outside sealed segment: %w", base.ErrInvalidAddress)
	}
	return readFrameAt(s.file, uint64(addr.Offset()), s.validEnd, s.maxPayloadSize)
}

func (s *SealedData) Scan(visit func(base.VAddr, storeformat.Frame) error) error {
	if visit == nil {
		return fmt.Errorf("nil scan visitor: %w", base.ErrInvalidConfig)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	_, _, err = scanDataFrames(s.file, s.validEnd, s.validEnd, s.maxPayloadSize, base.FrameSeq(header.FirstSeq), true, func(offset uint64, frame storeformat.Frame) error {
		addr, err := base.NewVAddr(s.segmentID, uint32(offset))
		if err != nil {
			return err
		}
		return visit(addr, frame)
	})
	return err
}

func (s *SealedData) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	s.closed = true
	return s.file.Close()
}

// Registry resolves a VAddr without exposing Segment lifecycle to Mapping.
// Removal is added by the GC phase; until then sealed entries are immutable.
type Registry struct {
	mu       sync.RWMutex
	active   *ActiveData
	sealed   map[base.DataSegmentID]*SealedData
	openRefs map[base.DataSegmentID]uint64
	closed   bool
}

func NewRegistry(active *ActiveData, sealed []*SealedData) (*Registry, error) {
	if active == nil {
		return nil, base.ErrInvalidConfig
	}
	r := &Registry{active: active, sealed: make(map[base.DataSegmentID]*SealedData, len(sealed)), openRefs: make(map[base.DataSegmentID]uint64)}
	for _, item := range sealed {
		if item == nil || item.segmentID == active.SegmentID() {
			return nil, base.ErrInvalidConfig
		}
		if _, exists := r.sealed[item.segmentID]; exists {
			return nil, base.ErrInvalidConfig
		}
		r.sealed[item.segmentID] = item
	}
	return r, nil
}

func (r *Registry) PinOpenBatch(id base.DataSegmentID) error {
	if id == 0 {
		return base.ErrInvalidAddress
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return base.ErrClosed
	}
	if (r.active == nil || r.active.SegmentID() != id) && r.sealed[id] == nil {
		return base.ErrInvalidAddress
	}
	r.openRefs[id]++
	return nil
}

func (r *Registry) UnpinOpenBatch(id base.DataSegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := r.openRefs[id]
	if count == 0 {
		return fmt.Errorf("open batch segment ref underflow: %w", base.ErrCorrupt)
	}
	if count == 1 {
		delete(r.openRefs, id)
	} else {
		r.openRefs[id] = count - 1
	}
	return nil
}

func (r *Registry) OpenBatchRefs(id base.DataSegmentID) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.openRefs[id]
}

func (r *Registry) ReadFrame(addr base.VAddr) (storeformat.Frame, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return storeformat.Frame{}, base.ErrClosed
	}
	active := r.active
	sealed := r.sealed[addr.SegmentID()]
	r.mu.RUnlock()
	if active != nil && addr.SegmentID() == active.SegmentID() {
		return active.ReadFrame(addr)
	}
	if sealed == nil {
		return storeformat.Frame{}, base.ErrInvalidAddress
	}
	return sealed.ReadFrame(addr)
}

func (r *Registry) ReplaceActive(oldID base.DataSegmentID, sealed *SealedData, active *ActiveData) error {
	if sealed == nil || active == nil || sealed.segmentID != oldID || active.SegmentID() == oldID {
		return base.ErrInvalidConfig
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return base.ErrClosed
	}
	if r.active == nil || r.active.SegmentID() != oldID {
		return fmt.Errorf("active segment changed during rotation: %w", base.ErrCorrupt)
	}
	if _, exists := r.sealed[oldID]; exists {
		return fmt.Errorf("sealed segment already registered: %w", base.ErrCorrupt)
	}
	r.sealed[oldID] = sealed
	r.active = active
	return nil
}

func (r *Registry) Active() *ActiveData {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

func (r *Registry) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return base.ErrClosed
	}
	r.closed = true
	active := r.active
	sealed := make([]*SealedData, 0, len(r.sealed))
	for _, item := range r.sealed {
		sealed = append(sealed, item)
	}
	r.mu.Unlock()
	var result error
	if active != nil {
		result = errors.Join(result, active.Close())
	}
	for _, item := range sealed {
		result = errors.Join(result, item.Close())
	}
	return result
}

func readFrameAt(file *os.File, offset, end, maxPayloadSize uint64) (storeformat.Frame, error) {
	headerBytes := make([]byte, storeformat.FrameHeaderSize)
	if _, err := file.ReadAt(headerBytes, int64(offset)); err != nil {
		return storeformat.Frame{}, err
	}
	limits := storeformat.FrameLimits{MaxPayloadSize: maxPayloadSize, RemainingSegmentSize: end - offset}
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
