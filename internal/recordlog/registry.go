package recordlog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type segmentState uint8

const (
	segmentActive segmentState = iota + 1
	segmentSealed
	segmentRetiring
)

type registryEntry struct {
	state  segmentState
	active *activeSegment
	sealed *sealedSegment
	pins   uint64
}

func (r *segmentRegistry) pinSnapshot() ([]*segmentPin, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	ids := make([]SegmentID, 0, len(r.entries))
	for id, entry := range r.entries {
		if entry.state == segmentRetiring {
			return nil, ErrSegmentRetiring
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	pins := make([]*segmentPin, 0, len(ids))
	for _, id := range ids {
		entry := r.entries[id]
		entry.pins++
		pins = append(pins, &segmentPin{registry: r, id: id, entry: entry})
	}
	return pins, nil
}

type segmentRegistry struct {
	mu      sync.Mutex
	entries map[SegmentID]*registryEntry
	active  SegmentID
	changed chan struct{}
	closed  bool
}

type segmentPin struct {
	mu       sync.Mutex
	registry *segmentRegistry
	id       SegmentID
	entry    *registryEntry
	released bool
}

func (p *segmentPin) scanSealed(start uint32, visit func(AppendResult, []byte) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return ErrClosed
	}
	p.registry.mu.Lock()
	state, sealed := p.entry.state, p.entry.sealed
	p.registry.mu.Unlock()
	if state != segmentSealed || sealed == nil {
		return ErrInvalidConfig
	}
	return sealed.scan(start, visit)
}

func newSegmentRegistry(active *activeSegment, sealed []*sealedSegment) (*segmentRegistry, error) {
	if active == nil || active.file == nil || active.header.SegmentID == 0 {
		return nil, ErrInvalidConfig
	}
	r := &segmentRegistry{entries: make(map[SegmentID]*registryEntry, len(sealed)+1), active: active.header.SegmentID, changed: make(chan struct{})}
	r.entries[r.active] = &registryEntry{state: segmentActive, active: active}
	for _, item := range sealed {
		if item == nil || item.file == nil || item.header.SegmentID == r.active {
			return nil, ErrInvalidConfig
		}
		id := item.header.SegmentID
		if _, exists := r.entries[id]; exists {
			return nil, ErrInvalidConfig
		}
		r.entries[id] = &registryEntry{state: segmentSealed, sealed: item}
	}
	return r, nil
}

func (r *segmentRegistry) pin(id SegmentID) (*segmentPin, error) {
	if id == 0 {
		return nil, ErrInvalidVAddr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	entry := r.entries[id]
	if entry == nil {
		return nil, ErrSegmentMissing
	}
	if entry.state == segmentRetiring {
		return nil, ErrSegmentRetiring
	}
	entry.pins++
	return &segmentPin{registry: r, id: id, entry: entry}, nil
}

// pinSealed waits for a Catalog-published rotation to reach the Registry,
// then pins the sealed segment. Callers must establish that id is sealed in
// the Catalog before calling this method; otherwise waiting on an active
// segment would have no completion condition.
func (r *segmentRegistry) pinSealed(ctx context.Context, id SegmentID) (*segmentPin, error) {
	if id == 0 {
		return nil, ErrInvalidVAddr
	}
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrClosed
		}
		entry := r.entries[id]
		if entry == nil {
			r.mu.Unlock()
			return nil, ErrSegmentMissing
		}
		switch entry.state {
		case segmentSealed:
			if entry.sealed == nil {
				r.mu.Unlock()
				return nil, ErrInvalidConfig
			}
			entry.pins++
			r.mu.Unlock()
			return &segmentPin{registry: r, id: id, entry: entry}, nil
		case segmentRetiring:
			r.mu.Unlock()
			return nil, ErrSegmentRetiring
		case segmentActive:
			changed := r.changed
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
		default:
			r.mu.Unlock()
			return nil, ErrInvalidConfig
		}
	}
}

func (p *segmentPin) read(addr VAddr) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return nil, ErrClosed
	}
	if addr.SegmentID() != p.id {
		return nil, ErrInvalidVAddr
	}
	p.registry.mu.Lock()
	active, sealed := p.entry.active, p.entry.sealed
	p.registry.mu.Unlock()
	if active != nil {
		return active.read(addr)
	}
	if sealed != nil {
		return sealed.read(addr)
	}
	return nil, ErrSegmentMissing
}

func (p *segmentPin) inspect(addr VAddr, prefixBytes uint32) (RecordHeader, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return RecordHeader{}, nil, ErrClosed
	}
	if addr.SegmentID() != p.id {
		return RecordHeader{}, nil, ErrInvalidVAddr
	}
	p.registry.mu.Lock()
	active, sealed := p.entry.active, p.entry.sealed
	p.registry.mu.Unlock()
	if active != nil {
		return active.inspect(addr, prefixBytes)
	}
	if sealed != nil {
		return sealed.inspect(addr, prefixBytes)
	}
	return RecordHeader{}, nil, ErrSegmentMissing
}

func (p *segmentPin) release() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.released || p.registry == nil {
		return ErrClosed
	}
	p.registry.mu.Lock()
	entry := p.registry.entries[p.id]
	if entry == nil || entry != p.entry || entry.pins == 0 {
		p.registry.mu.Unlock()
		return fmt.Errorf("reader pin underflow: %w", ErrCorrupt)
	}
	entry.pins--
	p.registry.signalLocked()
	p.registry.mu.Unlock()
	p.released = true
	p.registry = nil
	p.entry = nil
	return nil
}

func (r *segmentRegistry) publishRotation(oldID SegmentID, sealed *sealedSegment, active *activeSegment) error {
	if sealed == nil || active == nil || sealed.header.SegmentID != oldID || active.header.PreviousSegment != oldID || sealed.file == nil || active.file == nil {
		return ErrInvalidConfig
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	old := r.entries[oldID]
	if r.active != oldID || old == nil || old.state != segmentActive || old.active == nil {
		return ErrInvalidConfig
	}
	if _, exists := r.entries[active.header.SegmentID]; exists {
		return ErrInvalidConfig
	}
	if err := old.active.transferSealedOwnership(sealed); err != nil {
		return err
	}
	old.state = segmentSealed
	old.active = nil
	old.sealed = sealed
	r.entries[active.header.SegmentID] = &registryEntry{state: segmentActive, active: active}
	r.active = active.header.SegmentID
	r.signalLocked()
	return nil
}

func (r *segmentRegistry) publishSealed(segments []*sealedSegment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || len(segments) == 0 {
		return ErrInvalidConfig
	}
	for _, segment := range segments {
		if segment == nil || segment.file == nil || !IsCompactionSegment(segment.header.SegmentID) {
			return ErrInvalidConfig
		}
		if _, exists := r.entries[segment.header.SegmentID]; exists {
			return ErrInvalidConfig
		}
	}
	for _, segment := range segments {
		r.entries[segment.header.SegmentID] = &registryEntry{state: segmentSealed, sealed: segment}
	}
	r.signalLocked()
	return nil
}

func (r *segmentRegistry) beginRetire(id SegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	entry := r.entries[id]
	if entry == nil {
		return ErrSegmentMissing
	}
	if entry.state == segmentRetiring {
		return nil
	}
	if entry.state != segmentSealed || entry.sealed == nil {
		return ErrInvalidConfig
	}
	entry.state = segmentRetiring
	r.signalLocked()
	return nil
}

func (r *segmentRegistry) cancelRetire(id SegmentID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	entry := r.entries[id]
	if entry == nil {
		return ErrSegmentMissing
	}
	if entry.state != segmentRetiring {
		return ErrInvalidConfig
	}
	entry.state = segmentSealed
	r.signalLocked()
	return nil
}

func (r *segmentRegistry) waitNoReaders(ctx context.Context, id SegmentID) error {
	for {
		r.mu.Lock()
		entry := r.entries[id]
		if entry == nil {
			r.mu.Unlock()
			return ErrSegmentMissing
		}
		if entry.state != segmentRetiring {
			r.mu.Unlock()
			return ErrInvalidConfig
		}
		if entry.pins == 0 {
			r.mu.Unlock()
			return nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (r *segmentRegistry) detachRetired(id SegmentID) (*sealedSegment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[id]
	if entry == nil {
		return nil, ErrSegmentMissing
	}
	if entry.state != segmentRetiring || entry.sealed == nil {
		return nil, ErrInvalidConfig
	}
	if entry.pins != 0 {
		return nil, ErrReadersActive
	}
	delete(r.entries, id)
	r.signalLocked()
	return entry.sealed, nil
}

func (r *segmentRegistry) close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	r.closed = true
	for {
		readers := false
		for _, entry := range r.entries {
			if entry.pins != 0 {
				readers = true
				break
			}
		}
		if !readers {
			break
		}
		changed := r.changed
		r.mu.Unlock()
		<-changed
		r.mu.Lock()
	}
	entries := r.entries
	r.entries = nil
	r.signalLocked()
	r.mu.Unlock()
	var result error
	for _, entry := range entries {
		if entry.active != nil {
			result = errors.Join(result, entry.active.close())
		}
		if entry.sealed != nil {
			result = errors.Join(result, entry.sealed.close())
		}
	}
	return result
}

func (r *segmentRegistry) signalLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}
