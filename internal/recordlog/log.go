package recordlog

import (
	"context"
	"errors"
	"sort"
	"sync"
)

type Config struct {
	MaxQueuedBytes uint64
	QueueCapacity  int
	BufferBytes    uint32
	BufferRecords  int
}

func (c Config) validate(segmentSize, maxPayloadBytes uint32) error {
	if segmentSize <= SegmentHeaderSize+SegmentFooterSize || maxPayloadBytes == 0 || c.MaxQueuedBytes == 0 || c.QueueCapacity <= 0 || c.BufferBytes == 0 || c.BufferRecords <= 0 {
		return ErrInvalidConfig
	}
	maximum, err := PhysicalRecordSize(uint64(maxPayloadBytes))
	if err != nil || maximum > segmentSize-SegmentHeaderSize-SegmentFooterSize || uint64(maximum) > c.MaxQueuedBytes {
		return ErrInvalidConfig
	}
	return nil
}

type Watermarks struct {
	Reserved LogPos
	Written  LogPos
	Durable  LogPos
}

type Status struct {
	Watermarks     Watermarks
	PendingRecords int
	PendingBytes   uint64
	QueuedRequests int
	QueuedBytes    uint64
	WriteCalls     uint64
	SyncCalls      uint64
	Poisoned       bool
	Closed         bool
}

type appendResponse struct {
	result AppendResult
	err    error
}

type appendRequest struct {
	ctx       context.Context
	payload   []byte
	physical  uint32
	sync      bool
	budget    uint64
	response  chan appendResponse
	stop      bool
	reserved  bool
	completed bool
	result    AppendResult
}

type pendingRecord struct {
	request *appendRequest
	result  AppendResult
	size    uint32
	payload []byte
}

type scanSnapshot struct {
	written  LogPos
	reserved LogPos
	pending  []pendingSnapshot
}

type pendingSnapshot struct {
	result  AppendResult
	payload []byte
}

type rotateActive func(*activeSegment) (*activeSegment, error)

type Log struct {
	cfg             Config
	root            string
	catalog         CatalogPort
	files           fileBackend
	requests        chan *appendRequest
	done            chan struct{}
	budget          *byteBudget
	registry        *segmentRegistry
	rotate          rotateActive
	maxPayloadBytes uint32

	submitMu sync.RWMutex
	closing  bool

	stateMu  sync.RWMutex
	pending  map[VAddr][]byte
	status   Status
	terminal error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newLog(cfg Config, maxPayloadBytes uint32, active *activeSegment, registry *segmentRegistry, rotate rotateActive) (*Log, error) {
	if active == nil || registry == nil || rotate == nil || cfg.validate(active.header.SegmentSize, maxPayloadBytes) != nil {
		return nil, ErrInvalidConfig
	}
	registry.mu.Lock()
	validRegistry := !registry.closed && registry.active == active.header.SegmentID && registry.entries[registry.active] != nil && registry.entries[registry.active].active == active
	registry.mu.Unlock()
	if !validRegistry {
		return nil, ErrInvalidConfig
	}
	initial := LogPos{SegmentID: active.header.SegmentID, Offset: active.summary().ValidEnd}
	log := &Log{
		cfg: cfg, requests: make(chan *appendRequest, cfg.QueueCapacity), done: make(chan struct{}),
		budget: newByteBudget(cfg.MaxQueuedBytes), registry: registry, rotate: rotate, maxPayloadBytes: maxPayloadBytes,
		pending: make(map[VAddr][]byte), closeDone: make(chan struct{}),
	}
	log.status.Watermarks = Watermarks{Reserved: initial, Written: initial, Durable: initial}
	go log.runWriter(active)
	return log, nil
}

func (l *Log) Append(ctx context.Context, payload []byte, syncWrite bool) (AppendResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if uint64(len(payload)) > uint64(l.maxPayloadBytes) {
		return AppendResult{}, ErrPayloadTooBig
	}
	physical, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return AppendResult{}, err
	}
	if err := l.budget.acquire(ctx, uint64(physical)); err != nil {
		return AppendResult{}, l.preferTerminal(err)
	}
	request := &appendRequest{
		ctx: ctx, payload: payload, physical: physical, sync: syncWrite, budget: uint64(physical), response: make(chan appendResponse, 1),
	}
	l.submitMu.RLock()
	if l.closing {
		err := l.submitErrorLocked()
		l.submitMu.RUnlock()
		l.budget.release(request.budget)
		return AppendResult{}, err
	}
	l.stateMu.RLock()
	terminal := l.terminal
	l.stateMu.RUnlock()
	if terminal != nil {
		l.submitMu.RUnlock()
		l.budget.release(request.budget)
		return AppendResult{}, terminal
	}
	select {
	case l.requests <- request:
		l.submitMu.RUnlock()
	case <-ctx.Done():
		l.submitMu.RUnlock()
		l.budget.release(request.budget)
		return AppendResult{}, ctx.Err()
	}
	// Once accepted, writer decides whether cancellation happened before
	// reservation. Waiting here preserves caller ownership of payload until it
	// has either been rejected or copied into the writer buffer.
	response := <-request.response
	return response.result, response.err
}

func (l *Log) Read(ctx context.Context, addr VAddr) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !addr.Valid() {
		return nil, ErrInvalidVAddr
	}
	l.submitMu.RLock()
	closed := l.closing
	l.submitMu.RUnlock()
	l.stateMu.RLock()
	terminal := l.terminal
	l.stateMu.RUnlock()
	if terminal != nil {
		return nil, terminal
	}
	if closed {
		return nil, ErrClosed
	}
	l.stateMu.RLock()
	if payload, exists := l.pending[addr]; exists {
		result := append([]byte(nil), payload...)
		l.stateMu.RUnlock()
		return result, nil
	}
	l.stateMu.RUnlock()
	pin, err := l.registry.pin(addr.SegmentID())
	if err != nil {
		return nil, err
	}
	payload, readErr := pin.read(addr)
	releaseErr := pin.release()
	return payload, errors.Join(readErr, releaseErr)
}

func (l *Log) Scan(ctx context.Context, from LogPos, visit func(AppendResult, []byte) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !from.Valid() || visit == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.submitMu.RLock()
	closed := l.closing
	l.submitMu.RUnlock()
	l.stateMu.RLock()
	terminal := l.terminal
	l.stateMu.RUnlock()
	if terminal != nil {
		return terminal
	}
	if closed {
		return ErrClosed
	}
	snapshot := l.snapshotForScan()
	pins, err := l.registry.pinSnapshot()
	if err != nil {
		return err
	}
	defer releasePins(pins)
	foundStart := false
	for _, pin := range pins {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pin.id < from.SegmentID || pin.id > snapshot.written.SegmentID {
			continue
		}
		if pin.id == from.SegmentID {
			foundStart = true
		}
		start := SegmentHeaderSize
		if pin.id == from.SegmentID {
			start = from.Offset
		}
		limit := uint32(0)
		pin.registry.mu.Lock()
		active, sealed := pin.entry.active, pin.entry.sealed
		pin.registry.mu.Unlock()
		if active != nil {
			limit = active.summary().ValidEnd
			if pin.id == snapshot.written.SegmentID && limit > snapshot.written.Offset {
				limit = snapshot.written.Offset
			}
		} else if sealed != nil {
			limit = sealed.summary.ValidEnd
		}
		if start > limit {
			if active != nil && pin.id == snapshot.reserved.SegmentID && validPendingBoundary(snapshot, from) {
				continue
			}
			if pin.id == from.SegmentID && start == limit {
				continue
			}
			return ErrInvalidLogPos
		}
		if start == limit {
			continue
		}
		callback := func(result AppendResult, payload []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return visit(result, payload)
		}
		if active != nil {
			if err := active.scanTo(start, limit, callback); err != nil {
				return err
			}
		} else if err := sealed.scan(start, callback); err != nil {
			return err
		}
	}
	if !foundStart {
		return ErrInvalidLogPos
	}
	for _, record := range snapshot.pending {
		start := LogPos{SegmentID: record.result.Addr.SegmentID(), Offset: record.result.Addr.Offset()}
		if start.Compare(from) < 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(record.result, record.payload); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) snapshotForScan() scanSnapshot {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	snapshot := scanSnapshot{
		written: l.status.Watermarks.Written, reserved: l.status.Watermarks.Reserved,
		pending: make([]pendingSnapshot, 0, len(l.pending)),
	}
	for addr, payload := range l.pending {
		size, _ := PhysicalRecordSize(uint64(len(payload)))
		result, _ := NewAppendResult(addr, size)
		snapshot.pending = append(snapshot.pending, pendingSnapshot{result: result, payload: append([]byte(nil), payload...)})
	}
	sort.Slice(snapshot.pending, func(i, j int) bool { return snapshot.pending[i].result.Addr < snapshot.pending[j].result.Addr })
	return snapshot
}

func validPendingBoundary(snapshot scanSnapshot, position LogPos) bool {
	if position == snapshot.written || position == snapshot.reserved {
		return true
	}
	for _, record := range snapshot.pending {
		start := LogPos{SegmentID: record.result.Addr.SegmentID(), Offset: record.result.Addr.Offset()}
		if position == start || position == record.result.End {
			return true
		}
	}
	return false
}

func (l *Log) Status() Status {
	l.stateMu.RLock()
	status := l.status
	l.stateMu.RUnlock()
	status.QueuedRequests = len(l.requests)
	status.QueuedBytes = l.budget.usage()
	return status
}

func (l *Log) Close() error {
	l.closeOnce.Do(func() {
		l.submitMu.Lock()
		l.closing = true
		l.submitMu.Unlock()
		l.budget.close()
		request := &appendRequest{stop: true, response: make(chan appendResponse, 1)}
		l.requests <- request
		response := <-request.response
		<-l.done
		l.closeErr = errors.Join(response.err, l.registry.close())
		l.stateMu.Lock()
		l.status.Closed = true
		l.stateMu.Unlock()
		close(l.closeDone)
	})
	<-l.closeDone
	return l.closeErr
}

func (l *Log) setTerminal(cause error) error {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	if l.terminal == nil {
		l.terminal = errors.Join(ErrPoisoned, cause)
		l.status.Poisoned = true
		l.budget.close()
	}
	return l.terminal
}

func (l *Log) preferTerminal(fallback error) error {
	l.stateMu.RLock()
	terminal := l.terminal
	l.stateMu.RUnlock()
	if terminal != nil {
		return terminal
	}
	l.submitMu.RLock()
	defer l.submitMu.RUnlock()
	if l.closing {
		return ErrClosed
	}
	return fallback
}

func (l *Log) submitErrorLocked() error {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	if l.terminal != nil {
		return l.terminal
	}
	return ErrClosed
}

func releasePins(pins []*segmentPin) {
	for _, pin := range pins {
		_ = pin.release()
	}
}
