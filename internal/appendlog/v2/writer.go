package v2

import (
	"errors"
	"fmt"
)

type pendingRecord struct {
	addr    VAddr
	payload []byte
	encoded []byte
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
	bytes    uint64
	waiters  []durableWaiter
	needSync bool
	fault    error
	writes   uint64
	syncs    uint64
	lastAddr VAddr
}

func newWriter(log *Log, active *segment, idValue logID) *writer {
	return &writer{log: log, active: active, logID: idValue, lastAddr: active.last}
}

func (w *writer) run() {
	for {
		request := <-w.log.requests
		if request.stop {
			w.stop(request)
			return
		}
		if request.snapshot != nil {
			w.snapshot(request)
			continue
		}
		if w.fault != nil {
			w.reject(request, w.fault)
			continue
		}
		boundary, err := w.stage(request)
		if err != nil {
			w.poison(err, request)
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
				if request.stop {
					w.stop(request)
					return
				}
				if request.snapshot != nil {
					w.snapshot(request)
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
	encoded, err := encodeRecord(addr, request.data)
	if err != nil {
		return false, err
	}
	record := pendingRecord{addr: addr, payload: request.data, encoded: encoded, budget: request.bytes}
	w.log.pendingMu.Lock()
	w.log.pending[addr] = request.data
	w.log.pendingMu.Unlock()
	w.pending = append(w.pending, record)
	w.bytes += uint64(len(encoded))
	w.lastAddr = addr
	w.needSync = w.needSync || request.sync
	if request.sync {
		w.waiters = append(w.waiters, durableWaiter{request: request, addr: addr})
	} else {
		request.result <- appendResult{addr: addr}
	}
	w.publishStatus()
	return len(w.pending) >= w.log.cfg.MaxBufferRecords || w.bytes >= w.log.cfg.MaxBufferBytes, nil
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
	written, err := w.active.appendEncoded(w.pending)
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
		waiter.request.result <- appendResult{addr: waiter.addr}
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
	next, err := createSegment(w.log.cfg.Dir, oldID+1, oldID, w.log.cfg.SegmentSize, w.logID, w.log.cfg.FaultHook)
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
		waiter.request.result <- appendResult{err: w.fault}
	}
	w.waiters = w.waiters[:0]
	w.needSync = false
}

func (w *writer) reject(request *appendRequest, err error) {
	if request.snapshot != nil {
		request.snapshot <- snapshotResult{err: err}
		return
	}
	w.log.budget.release(request.bytes)
	request.result <- appendResult{err: err}
}

func (w *writer) snapshot(request *appendRequest) {
	if w.fault != nil {
		request.snapshot <- snapshotResult{err: w.fault}
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
	request.snapshot <- snapshotResult{snapshot: snapshot}
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
	request.result <- appendResult{err: err}
}
