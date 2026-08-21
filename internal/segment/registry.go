package segment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

var (
	ErrCleaning = errors.New("ridstore: data segment cleaning")
	ErrRetired  = errors.New("ridstore: data segment retired")
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

func (s *SealedData) ReadFrameHeader(addr base.VAddr) (storeformat.FrameHeader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return storeformat.FrameHeader{}, base.ErrClosed
	}
	if addr.SegmentID() != s.segmentID || addr.Offset() < storeformat.SegmentHeaderSize || uint64(addr.Offset()) >= s.validEnd {
		return storeformat.FrameHeader{}, fmt.Errorf("read address outside sealed segment: %w", base.ErrInvalidAddress)
	}
	return readFrameHeaderAt(s.file, uint64(addr.Offset()), s.validEnd, s.maxPayloadSize)
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

// Registry resolves a VAddr and serializes reader/open-batch references with
// sealed-segment retirement. Sealed data itself remains immutable.
type Registry struct {
	mu         sync.RWMutex
	active     *ActiveData
	sealed     map[base.DataSegmentID]*SealedData
	openRefs   map[base.DataSegmentID]uint64
	readerRefs map[base.DataSegmentID]uint64
	cleaning   map[base.DataSegmentID]struct{}
	retired    map[base.DataSegmentID]struct{}
	notify     chan struct{}
	closed     bool
}

type ReadPin struct {
	mu       sync.Mutex
	registry *Registry
	id       base.DataSegmentID
	active   *ActiveData
	sealed   *SealedData
	released bool
}

func NewRegistry(active *ActiveData, sealed []*SealedData) (*Registry, error) {
	if active == nil {
		return nil, base.ErrInvalidConfig
	}
	r := &Registry{
		active: active, sealed: make(map[base.DataSegmentID]*SealedData, len(sealed)),
		openRefs: make(map[base.DataSegmentID]uint64), readerRefs: make(map[base.DataSegmentID]uint64),
		cleaning: make(map[base.DataSegmentID]struct{}), retired: make(map[base.DataSegmentID]struct{}), notify: make(chan struct{}),
	}
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
	if _, retired := r.retired[id]; retired {
		return ErrRetired
	}
	if _, cleaning := r.cleaning[id]; cleaning {
		return ErrCleaning
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
	r.signalLocked()
	return nil
}

func (r *Registry) OpenBatchRefs(id base.DataSegmentID) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.openRefs[id]
}

func (r *Registry) Acquire(id base.DataSegmentID) (*ReadPin, error) {
	if id == 0 {
		return nil, base.ErrInvalidAddress
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, base.ErrClosed
	}
	if _, retired := r.retired[id]; retired {
		return nil, ErrRetired
	}
	pin := &ReadPin{registry: r, id: id}
	if r.active != nil && r.active.SegmentID() == id {
		pin.active = r.active
	} else if sealed := r.sealed[id]; sealed != nil {
		pin.sealed = sealed
	} else {
		return nil, base.ErrInvalidAddress
	}
	r.readerRefs[id]++
	return pin, nil
}

func (p *ReadPin) ReadFrame(addr base.VAddr) (storeformat.Frame, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return storeformat.Frame{}, base.ErrClosed
	}
	if addr.SegmentID() != p.id {
		return storeformat.Frame{}, base.ErrInvalidAddress
	}
	if p.active != nil {
		return p.active.ReadFrame(addr)
	}
	return p.sealed.ReadFrame(addr)
}

func (p *ReadPin) ReadFrameHeader(addr base.VAddr) (storeformat.FrameHeader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return storeformat.FrameHeader{}, base.ErrClosed
	}
	if addr.SegmentID() != p.id {
		return storeformat.FrameHeader{}, base.ErrInvalidAddress
	}
	if p.active != nil {
		return p.active.ReadFrameHeader(addr)
	}
	return p.sealed.ReadFrameHeader(addr)
}

func (p *ReadPin) Release() error {
	if p == nil {
		return base.ErrInvalidConfig
	}
	p.mu.Lock()
	if p.released || p.registry == nil {
		p.mu.Unlock()
		return base.ErrClosed
	}
	p.released = true
	registry, id := p.registry, p.id
	p.registry, p.active, p.sealed = nil, nil, nil
	p.mu.Unlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	count := registry.readerRefs[id]
	if count == 0 {
		return fmt.Errorf("reader segment ref underflow: %w", base.ErrCorrupt)
	}
	if count == 1 {
		delete(registry.readerRefs, id)
	} else {
		registry.readerRefs[id] = count - 1
	}
	registry.signalLocked()
	return nil
}

func (r *Registry) ReaderRefs(id base.DataSegmentID) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.readerRefs[id]
}

func (r *Registry) BeginCleaning(id base.DataSegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return base.ErrClosed
	}
	if _, retired := r.retired[id]; retired {
		return ErrRetired
	}
	if _, cleaning := r.cleaning[id]; cleaning {
		return ErrCleaning
	}
	if r.sealed[id] == nil {
		return base.ErrInvalidAddress
	}
	if r.openRefs[id] != 0 {
		return fmt.Errorf("clean segment with open batch refs: %w", base.ErrInvalidConfig)
	}
	r.cleaning[id] = struct{}{}
	r.signalLocked()
	return nil
}

func (r *Registry) CancelCleaning(id base.DataSegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, cleaning := r.cleaning[id]; !cleaning {
		return base.ErrInvalidConfig
	}
	delete(r.cleaning, id)
	r.signalLocked()
	return nil
}

func (r *Registry) ScanCleaning(id base.DataSegmentID, visit func(base.VAddr, storeformat.Frame) error) error {
	if visit == nil {
		return base.ErrInvalidConfig
	}
	r.mu.RLock()
	_, cleaning := r.cleaning[id]
	sealed := r.sealed[id]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return base.ErrClosed
	}
	if !cleaning || sealed == nil {
		return base.ErrInvalidConfig
	}
	return sealed.Scan(visit)
}

func (r *Registry) RetireCleaning(id base.DataSegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return base.ErrClosed
	}
	if _, cleaning := r.cleaning[id]; !cleaning || r.sealed[id] == nil || r.openRefs[id] != 0 {
		return base.ErrInvalidConfig
	}
	delete(r.cleaning, id)
	r.retired[id] = struct{}{}
	r.signalLocked()
	return nil
}

// Retire atomically prevents new readers from acquiring a sealed segment.
// Existing ReadPins remain valid until released.
func (r *Registry) Retire(id base.DataSegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return base.ErrClosed
	}
	if r.sealed[id] == nil {
		if _, retired := r.retired[id]; retired {
			return ErrRetired
		}
		return base.ErrInvalidAddress
	}
	if _, cleaning := r.cleaning[id]; cleaning {
		return ErrCleaning
	}
	if r.openRefs[id] != 0 {
		return fmt.Errorf("retire segment with open batch refs: %w", base.ErrInvalidConfig)
	}
	r.retired[id] = struct{}{}
	r.signalLocked()
	return nil
}

func (r *Registry) WaitForNoReaders(ctx context.Context, id base.DataSegmentID) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return base.ErrClosed
		}
		if _, retired := r.retired[id]; !retired {
			r.mu.Unlock()
			return base.ErrInvalidConfig
		}
		if r.readerRefs[id] == 0 {
			r.mu.Unlock()
			return nil
		}
		notify := r.notify
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// DetachRetired removes a reader-free segment from the live Registry. The
// retired ID tombstone remains forever because DataSegmentIDs are never reused.
func (r *Registry) DetachRetired(id base.DataSegmentID) (*SealedData, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, base.ErrClosed
	}
	if _, retired := r.retired[id]; !retired {
		return nil, base.ErrInvalidConfig
	}
	if r.readerRefs[id] != 0 || r.openRefs[id] != 0 {
		return nil, base.ErrInvalidConfig
	}
	sealed := r.sealed[id]
	if sealed == nil {
		return nil, ErrRetired
	}
	delete(r.sealed, id)
	r.signalLocked()
	return sealed, nil
}

func (r *Registry) ReadFrame(addr base.VAddr) (storeformat.Frame, error) {
	pin, err := r.Acquire(addr.SegmentID())
	if err != nil {
		return storeformat.Frame{}, err
	}
	defer pin.Release()
	return pin.ReadFrame(addr)
}

func (r *Registry) ReadFrameHeader(addr base.VAddr) (storeformat.FrameHeader, error) {
	pin, err := r.Acquire(addr.SegmentID())
	if err != nil {
		return storeformat.FrameHeader{}, err
	}
	defer pin.Release()
	return pin.ReadFrameHeader(addr)
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
	r.signalLocked()
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
	r.signalLocked()
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

func (r *Registry) signalLocked() {
	close(r.notify)
	r.notify = make(chan struct{})
}

func readFrameAt(file *os.File, offset, end, maxPayloadSize uint64) (storeformat.Frame, error) {
	header, err := readFrameHeaderAt(file, offset, end, maxPayloadSize)
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
	limits := storeformat.FrameLimits{MaxPayloadSize: maxPayloadSize, RemainingSegmentSize: end - offset}
	frame, consumed, err := storeformat.DecodeFrame(encoded, limits)
	if err != nil {
		return storeformat.Frame{}, err
	}
	if consumed != total {
		return storeformat.Frame{}, fmt.Errorf("frame consumed size: %w", base.ErrCorrupt)
	}
	return frame, nil
}

func readFrameHeaderAt(file *os.File, offset, end, maxPayloadSize uint64) (storeformat.FrameHeader, error) {
	headerBytes := make([]byte, storeformat.FrameHeaderSize)
	if _, err := file.ReadAt(headerBytes, int64(offset)); err != nil {
		return storeformat.FrameHeader{}, err
	}
	limits := storeformat.FrameLimits{MaxPayloadSize: maxPayloadSize, RemainingSegmentSize: end - offset}
	header, err := storeformat.DecodeFrameHeader(headerBytes, limits)
	if err != nil {
		return storeformat.FrameHeader{}, err
	}
	return header, nil
}
