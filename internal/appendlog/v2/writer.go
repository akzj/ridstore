package v2

import (
	"errors"
	"fmt"
)

type pendingRecord struct {
	addr    VAddr
	payload []byte
	size    uint64
	budget  uint64
}

type durableWaiter struct {
	request *appendRequest
	addr    VAddr
}

type writer struct {
	log      *Log
	active   *segment
	logID    logID
	pending  []pendingRecord
	encoded  []byte
	bytes    uint64
	waiters  []durableWaiter
	needSync bool
	fault    error
	writes   uint64
	syncs    uint64
	lastAddr VAddr
	current  *appendRequest
}

func newWriter(log *Log, active *segment, idValue logID, lastAddr VAddr) *writer {
	return &writer{log: log, active: active, logID: idValue, lastAddr: lastAddr}
}

func (w *writer) run() {
	for {
		request := <-w.log.requests
		w.current = request
		if request.stop {
			w.stop(request)
			return
		}
		if request.snapshot != nil {
			w.snapshot(request)
			w.current = nil
			continue
		}
		if w.fault != nil {
			w.reject(request, w.fault)
			w.current = nil
			continue
		}
		boundary, err := w.stage(request)
		if err != nil {
			w.poison(err, request)
			w.current = nil
			continue
		}
		if boundary {
			if err := w.finishBatch(); err != nil {
				w.poison(err, nil)
			}
			continue
		}

	Drain:
		for {
			select {
			case request = <-w.log.requests:
				w.current = request
				if request.stop {
					w.stop(request)
					return
				}
				if request.snapshot != nil {
					w.snapshot(request)
					w.current = nil
					continue
				}
				boundary, err = w.stage(request)
				if err != nil {
					w.poison(err, request)
					break Drain
				}
				if boundary {
					break Drain
				}
			default:
				break Drain
			}
		}
		if w.fault == nil && (w.needSync || boundary) {
			if err := w.finishBatch(); err != nil {
				w.poison(err, nil)
			}
		}
		w.current = nil
	}
}

func (w *writer) stage(request *appendRequest) (bool, error) {
	size, err := encodedRecordSize(uint64(len(request.data)))
	if err != nil || uint64(len(request.data)) > w.log.cfg.MaxPayloadSize {
		w.reject(request, errors.Join(err, ErrPayloadTooBig))
		return false, nil
	}
	remaining := w.active.remaining()
	if w.bytes > remaining || size > remaining-w.bytes {
		if err := w.finishBatch(); err != nil {
			return false, err
		}
		if size > w.active.remaining() {
			if err := w.rotate(); err != nil {
				return false, err
			}
		}
	}
	if len(w.pending) != 0 && (len(w.pending) >= w.log.cfg.MaxBufferRecords || size > w.log.cfg.MaxBufferBytes || w.bytes > w.log.cfg.MaxBufferBytes-size) {
		if err := w.finishBatch(); err != nil {
			return false, err
		}
	}
	addr, err := makeVAddr(w.active.header.SegmentID, w.active.end+w.bytes)
	if err != nil {
		return false, err
	}
	start := len(w.encoded)
	w.growEncoded(size)
	if err := encodeRecordInto(w.encoded[start:], addr, request.data); err != nil {
		w.encoded = w.encoded[:start]
		return false, err
	}
	payloadStart := start + int(recordHeaderSize)
	payload := w.encoded[payloadStart : payloadStart+len(request.data)]
	record := pendingRecord{addr: addr, payload: payload, size: size, budget: request.bytes}
	request.data = nil
	w.log.pendingMu.Lock()
	w.log.pending[addr] = payload
	w.log.pendingMu.Unlock()
	w.pending = append(w.pending, record)
	request.staged = true
	w.bytes += size
	w.lastAddr = addr
	w.needSync = w.needSync || request.sync
	if request.sync {
		w.waiters = append(w.waiters, durableWaiter{request: request, addr: addr})
	} else {
		w.completeAppend(request, appendResult{addr: addr})
	}
	w.publishStatus()
	return len(w.pending) >= w.log.cfg.MaxBufferRecords || w.bytes >= w.log.cfg.MaxBufferBytes, nil
}

func (w *writer) growEncoded(size uint64) {
	required := uint64(len(w.encoded)) + size
	if required <= uint64(cap(w.encoded)) {
		w.encoded = w.encoded[:int(required)]
		return
	}
	capacity := w.log.cfg.MaxBufferBytes
	if required > capacity {
		capacity = required
	}
	next := make([]byte, int(required), int(capacity))
	copy(next, w.encoded)
	w.encoded = next
}

func (w *writer) finishBatch() error {
	if len(w.pending) == 0 {
		if w.needSync {
			if err := w.active.sync(); err != nil {
				return err
			}
			w.syncs++
			w.completeDurable()
		}
		return nil
	}
	written, err := w.active.appendEncoded(w.encoded, w.pending)
	if err != nil || written != w.bytes {
		return errors.Join(err, fmt.Errorf("append wrote %d of %d: %w", written, w.bytes, ErrCorrupt))
	}
	w.writes++
	w.log.pendingMu.Lock()
	for i := range w.pending {
		delete(w.log.pending, w.pending[i].addr)
		w.log.budget.release(w.pending[i].budget)
	}
	w.log.pendingMu.Unlock()
	w.pending = w.pending[:0]
	if uint64(cap(w.encoded)) > w.log.cfg.MaxBufferBytes {
		w.encoded = nil
	} else {
		w.encoded = w.encoded[:0]
	}
	w.bytes = 0
	w.updateWritten()
	if w.needSync {
		if err := w.active.sync(); err != nil {
			return err
		}
		w.syncs++
		w.completeDurable()
	}
	w.publishStatus()
	return nil
}

func (w *writer) completeDurable() {
	position := Position{SegmentID: w.active.header.SegmentID, Offset: w.active.end}
	w.log.statusMu.Lock()
	w.log.status.Watermarks.Durable = position
	w.log.statusMu.Unlock()
	for _, waiter := range w.waiters {
		w.completeAppend(waiter.request, appendResult{addr: waiter.addr})
	}
	w.waiters = w.waiters[:0]
	w.needSync = false
}

func (w *writer) rotate() error {
	if w.active.header.SegmentID == uint32(maxSegmentID) {
		return ErrInvalidConfig
	}
	oldID := w.active.header.SegmentID
	if err := w.active.seal(w.log.cfg.Dir); err != nil {
		return err
	}
	w.syncs++
	next, err := createSegment(w.log.cfg.Dir, oldID+1, oldID, w.log.cfg.SegmentSize, w.logID, w.log.cfg.FaultHook, w.log.cfg.files)
	if err != nil {
		return err
	}
	w.active = next
	position := Position{SegmentID: next.header.SegmentID, Offset: next.end}
	w.log.statusMu.Lock()
	w.log.status.Watermarks = Watermarks{Reserved: position, Written: position, Durable: position}
	w.log.statusMu.Unlock()
	return nil
}

func (w *writer) updateWritten() {
	position := Position{SegmentID: w.active.header.SegmentID, Offset: w.active.end}
	w.log.statusMu.Lock()
	w.log.status.Watermarks.Written = position
	w.log.statusMu.Unlock()
}

func (w *writer) publishStatus() {
	w.log.statusMu.Lock()
	w.log.status.PendingRecords = len(w.pending)
	w.log.status.PendingBytes = w.bytes
	w.log.status.WriteCalls = w.writes
	w.log.status.SyncCalls = w.syncs
	w.log.status.Watermarks.Reserved = Position{SegmentID: w.active.header.SegmentID, Offset: w.active.end + w.bytes}
	w.log.statusMu.Unlock()
}

func (w *writer) poison(err error, current *appendRequest) {
	w.fault = errors.Join(ErrPoisoned, err)
	w.log.setTerminal(err)
	if current != nil {
		w.reject(current, w.fault)
	}
	for _, waiter := range w.waiters {
		w.completeAppend(waiter.request, appendResult{err: w.fault})
	}
	w.waiters = w.waiters[:0]
	w.needSync = false
}

func (w *writer) reject(request *appendRequest, err error) {
	if request.snapshot != nil {
		w.completeSnapshot(request, snapshotResult{err: err})
		return
	}
	if !request.staged {
		w.log.budget.release(request.bytes)
	}
	w.completeAppend(request, appendResult{err: err})
}

func (w *writer) snapshot(request *appendRequest) {
	if w.fault != nil {
		w.completeSnapshot(request, snapshotResult{err: w.fault})
		return
	}
	snapshot := scanSnapshot{
		last:    w.lastAddr,
		written: Position{SegmentID: w.active.header.SegmentID, Offset: w.active.end},
		pending: make(map[VAddr][]byte, len(w.pending)),
	}
	for i := range w.pending {
		snapshot.pending[w.pending[i].addr] = append([]byte(nil), w.pending[i].payload...)
	}
	w.completeSnapshot(request, snapshotResult{snapshot: snapshot})
}

func (w *writer) stop(request *appendRequest) {
	err := w.fault
	if err == nil {
		err = w.finishBatch()
	}
	if err == nil {
		if syncErr := w.active.sync(); syncErr != nil {
			err = syncErr
		} else {
			w.syncs++
			w.completeDurable()
		}
	}
	err = errors.Join(err, w.active.close())
	if len(w.pending) != 0 {
		w.log.pendingMu.Lock()
		for i := range w.pending {
			delete(w.log.pending, w.pending[i].addr)
			w.log.budget.release(w.pending[i].budget)
		}
		w.log.pendingMu.Unlock()
		w.pending = nil
	}
	w.completeAppend(request, appendResult{err: err})
}

func (w *writer) completeAppend(request *appendRequest, result appendResult) {
	if request == nil || request.completed {
		return
	}
	request.completed = true
	request.result <- result
}

func (w *writer) completeSnapshot(request *appendRequest, result snapshotResult) {
	if request == nil || request.completed {
		return
	}
	request.completed = true
	request.snapshot <- result
}

func (w *writer) recoverPanic(value any) {
	err := fmt.Errorf("writer panic: %v", value)
	w.fault = errors.Join(ErrPoisoned, err)
	w.log.setTerminal(err)
	for _, waiter := range w.waiters {
		w.completeAppend(waiter.request, appendResult{err: w.fault})
	}
	w.waiters = nil
	if w.current != nil && !w.current.stop && !w.current.completed {
		w.reject(w.current, w.fault)
	}
	w.log.pendingMu.Lock()
	for i := range w.pending {
		delete(w.log.pending, w.pending[i].addr)
		w.log.budget.release(w.pending[i].budget)
	}
	w.log.pendingMu.Unlock()
	w.pending = nil
	w.encoded = nil
	w.bytes = 0
	closeErr := w.active.close()
	if w.current != nil && w.current.stop {
		w.completeAppend(w.current, appendResult{err: errors.Join(w.fault, closeErr)})
		return
	}
	for {
		request := <-w.log.requests
		if request.stop {
			w.completeAppend(request, appendResult{err: errors.Join(w.fault, closeErr)})
			return
		}
		w.reject(request, w.fault)
	}
}
