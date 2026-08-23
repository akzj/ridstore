package appendlog

import (
	"errors"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

// stageFrameLocked reserves a stable address in the active segment without
// issuing I/O. The caller must have preflighted capacity for size.
func (l *Log) stageFrameLocked(frame storeformat.Frame, size uint64) (base.VAddr, error) {
	if !l.buffered || frame.FrameSeq != l.nextFrameSeq || size == 0 {
		return 0, base.ErrCorrupt
	}
	if l.pendingBytes > math.MaxUint64-l.active.End() {
		return 0, base.ErrGenerationExhausted
	}
	offset := l.active.End() + l.pendingBytes
	if offset > math.MaxUint32 {
		return 0, base.ErrGenerationExhausted
	}
	addr, err := base.NewVAddr(l.active.SegmentID(), uint32(offset))
	if err != nil {
		return 0, err
	}
	if _, exists := l.pendingByAddr[addr]; exists {
		l.faulted = true
		return 0, fmt.Errorf("duplicate buffered address %d: %w", addr, base.ErrCorrupt)
	}
	frame.Payload = append([]byte(nil), frame.Payload...)
	l.pendingByAddr[addr] = len(l.pending)
	l.pending = append(l.pending, pendingFrame{frame: frame, addr: addr, size: size})
	l.pendingBytes += size
	l.nextFrameSeq++
	return addr, nil
}

func (l *Log) flushForBudgetLocked(required uint64) error {
	if len(l.pending) == 0 {
		return nil
	}
	if len(l.pending) < l.maxBufferFrames && l.pendingBytes < l.maxBufferBytes && required <= l.maxBufferBytes-l.pendingBytes {
		return nil
	}
	_, err := l.flushPendingLocked()
	return err
}

// ensureBufferedCapacityLocked keeps every reserved frame at its original
// address. Pending frames are therefore flushed before a segment rotation.
func (l *Log) ensureBufferedCapacityLocked(required uint64) error {
	if required > math.MaxUint64-segmentSealReserve {
		return l.capacityErrorLocked(required)
	}
	remaining := l.active.Remaining()
	if l.pendingBytes <= remaining && required+segmentSealReserve <= remaining-l.pendingBytes {
		return nil
	}
	if len(l.pending) != 0 {
		if _, err := l.flushPendingLocked(); err != nil {
			return err
		}
	}
	return l.ensureCapacityLocked(required)
}

// flushPendingLocked materializes all reserved frames in one append. A failed
// append poisons the Log because returned addresses may no longer be moved or
// reused. nextFrameSeq already advanced when the frames were staged.
func (l *Log) flushPendingLocked() (uint64, error) {
	if len(l.pending) == 0 {
		return 0, nil
	}
	frames := make([]storeformat.Frame, len(l.pending))
	for i := range l.pending {
		frames[i] = l.pending[i].frame
	}
	appended, written, err := l.active.AppendBatch(frames)
	if err != nil {
		l.faulted = true
		if errors.Is(err, segment.ErrFull) {
			err = fmt.Errorf("reserved append no longer fits active data segment: %w", base.ErrCorrupt)
		}
		return written, err
	}
	if len(appended) != len(l.pending) || written != l.pendingBytes {
		l.faulted = true
		return written, fmt.Errorf("buffered append result mismatch: %w", base.ErrCorrupt)
	}
	for i := range appended {
		if appended[i].Addr != l.pending[i].addr || appended[i].Size != l.pending[i].size {
			l.faulted = true
			return written, fmt.Errorf("buffered append address mismatch: %w", base.ErrCorrupt)
		}
	}
	clear(l.pendingByAddr)
	l.pending = l.pending[:0]
	l.pendingBytes = 0
	return written, nil
}

func (l *Log) syncPendingLocked() (uint64, error) {
	written, err := l.flushPendingLocked()
	if err != nil {
		return written, err
	}
	if err := l.active.Sync(); err != nil {
		l.faulted = true
		return written, err
	}
	if err := l.markDurableLocked(); err != nil {
		return written, err
	}
	return written, nil
}

func (l *Log) closePending() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.buffered || len(l.pending) == 0 {
		return nil
	}
	_, err := l.syncPendingLocked()
	return err
}

func (l *Log) readPendingFrame(addr base.VAddr) (storeformat.Frame, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	index, ok := l.pendingByAddr[addr]
	if !ok || index < 0 || index >= len(l.pending) {
		return storeformat.Frame{}, false
	}
	frame := l.pending[index].frame
	frame.Payload = append([]byte(nil), frame.Payload...)
	return frame, true
}
