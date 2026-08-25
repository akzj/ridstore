package recordlog

import (
	"errors"
	"fmt"
)

type writer struct {
	log      *Log
	active   *activeSegment
	deferred *appendRequest

	pending  []pendingRecord
	encoded  []byte
	bytes    uint32
	waiters  []pendingRecord
	needSync bool
	writes   uint64
	syncs    uint64
	fault    error
	current  *appendRequest
}

func (l *Log) runWriter(active *activeSegment) {
	w := &writer{log: l, active: active}
	defer func() {
		if value := recover(); value != nil {
			w.poison(fmt.Errorf("writer panic: %v", value))
			w.rejectOutstanding()
		}
		close(l.done)
	}()
	w.run()
}

func (w *writer) run() {
	for {
		request := w.nextRequest()
		w.current = request
		if request.stop {
			w.stop(request)
			return
		}
		if w.fault != nil {
			w.reject(request, w.fault)
			w.current = nil
			continue
		}
		if !w.stage(request) {
			w.current = nil
			continue
		}
		for len(w.pending) < w.log.cfg.BufferRecords && w.bytes < w.log.cfg.BufferBytes {
			select {
			case next := <-w.log.requests:
				if next.stop || !w.canStage(next) {
					w.deferred = next
					goto flush
				}
				w.current = next
				if !w.stage(next) && w.fault != nil {
					goto flush
				}
			default:
				goto flush
			}
		}
	flush:
		if err := w.flush(); err != nil {
			w.poison(err)
		}
		w.current = nil
	}
}

func (w *writer) nextRequest() *appendRequest {
	if w.deferred != nil {
		request := w.deferred
		w.deferred = nil
		return request
	}
	return <-w.log.requests
}

func (w *writer) canStage(request *appendRequest) bool {
	if request.stop || request.physical == 0 {
		return false
	}
	if len(w.pending) != 0 && (request.physical > w.log.cfg.BufferBytes || w.bytes > w.log.cfg.BufferBytes-request.physical) {
		return false
	}
	remaining := w.active.remaining()
	return w.bytes <= remaining && request.physical <= remaining-w.bytes
}

func (w *writer) stage(request *appendRequest) bool {
	if err := request.ctx.Err(); err != nil {
		w.reject(request, err)
		return false
	}
	if request.physical > w.active.remaining() {
		next, err := w.log.rotate(w.active)
		if err != nil {
			w.reject(request, w.poison(err))
			return false
		}
		if next == nil || next.file == nil || next.header.PreviousSegment != w.active.header.SegmentID {
			w.reject(request, w.poison(ErrInvalidConfig))
			return false
		}
		w.active = next
		position := LogPos{SegmentID: next.header.SegmentID, Offset: next.summary().ValidEnd}
		w.log.stateMu.Lock()
		w.log.status.Watermarks = Watermarks{Reserved: position, Written: position, Durable: position}
		w.log.stateMu.Unlock()
	}
	offset := w.active.summary().ValidEnd + w.bytes
	addr, err := NewVAddr(w.active.header.SegmentID, offset, request.physical)
	if err != nil {
		w.reject(request, w.poison(err))
		return false
	}
	result, err := NewAppendResult(addr, request.physical)
	if err != nil {
		w.reject(request, w.poison(err))
		return false
	}
	start := len(w.encoded)
	w.growEncoded(request.physical)
	if err := EncodeRecordTo(w.encoded[start:], addr, request.payload); err != nil {
		w.encoded = w.encoded[:start]
		w.reject(request, w.poison(err))
		return false
	}
	payloadStart := start + int(RecordHeaderSize)
	payloadCopy := w.encoded[payloadStart : payloadStart+len(request.payload)]
	request.payload = nil
	request.reserved = true
	request.result = result
	record := pendingRecord{request: request, result: result, size: request.physical, payload: payloadCopy}
	w.pending = append(w.pending, record)
	w.bytes += request.physical
	w.needSync = w.needSync || request.sync
	w.log.stateMu.Lock()
	w.log.pending[addr] = payloadCopy
	w.log.status.Watermarks.Reserved = result.End
	w.log.status.PendingRecords = len(w.pending)
	w.log.status.PendingBytes = uint64(w.bytes)
	w.log.stateMu.Unlock()
	w.log.budget.release(request.budget)
	request.budget = 0
	if request.sync {
		w.waiters = append(w.waiters, record)
	} else {
		w.complete(request, appendResponse{result: result})
	}
	return true
}

func (w *writer) growEncoded(size uint32) {
	required := len(w.encoded) + int(size)
	if required <= cap(w.encoded) {
		w.encoded = w.encoded[:required]
		return
	}
	capacity := int(w.log.cfg.BufferBytes)
	if required > capacity {
		capacity = required
	}
	next := make([]byte, required, capacity)
	copy(next, w.encoded)
	w.encoded = next
}

func (w *writer) flush() error {
	if len(w.pending) == 0 {
		return nil
	}
	extents := make([]recordExtent, len(w.pending))
	for i, record := range w.pending {
		extents[i] = recordExtent{Result: record.result, Size: record.size}
	}
	written, err := w.active.appendEncoded(w.encoded, extents)
	if err != nil || written != len(w.encoded) {
		return errors.Join(err, fmt.Errorf("append wrote %d of %d: %w", written, len(w.encoded), ErrCorrupt))
	}
	w.writes++
	w.log.stateMu.Lock()
	for _, record := range w.pending {
		delete(w.log.pending, record.result.Addr)
	}
	w.log.status.Watermarks.Written = w.pending[len(w.pending)-1].result.End
	w.log.status.PendingRecords = 0
	w.log.status.PendingBytes = 0
	w.log.status.WriteCalls = w.writes
	w.log.stateMu.Unlock()
	if w.needSync {
		if err := w.active.sync(); err != nil {
			return err
		}
		w.syncs++
		w.log.stateMu.Lock()
		w.log.status.Watermarks.Durable = w.log.status.Watermarks.Written
		w.log.status.SyncCalls = w.syncs
		w.log.stateMu.Unlock()
		for _, waiter := range w.waiters {
			w.complete(waiter.request, appendResponse{result: waiter.result})
		}
	}
	w.pending = w.pending[:0]
	w.waiters = w.waiters[:0]
	w.bytes = 0
	w.needSync = false
	if uint32(cap(w.encoded)) > w.log.cfg.BufferBytes {
		w.encoded = nil
	} else {
		w.encoded = w.encoded[:0]
	}
	return nil
}

func (w *writer) poison(cause error) error {
	if w.fault == nil {
		w.fault = w.log.setTerminal(cause)
		for _, waiter := range w.waiters {
			w.complete(waiter.request, appendResponse{err: w.fault})
		}
		w.log.stateMu.Lock()
		clear(w.log.pending)
		w.log.status.PendingRecords = 0
		w.log.status.PendingBytes = 0
		w.log.stateMu.Unlock()
		w.pending = nil
		w.waiters = nil
		w.encoded = nil
		w.bytes = 0
		w.needSync = false
	}
	return w.fault
}

func (w *writer) reject(request *appendRequest, err error) {
	if request == nil || request.completed {
		return
	}
	if !request.reserved && request.budget != 0 {
		w.log.budget.release(request.budget)
		request.budget = 0
	}
	w.complete(request, appendResponse{err: err})
}

func (w *writer) complete(request *appendRequest, response appendResponse) {
	if request == nil || request.completed {
		return
	}
	request.completed = true
	request.response <- response
}

func (w *writer) stop(request *appendRequest) {
	err := w.fault
	if err == nil {
		if syncErr := w.active.sync(); syncErr != nil {
			err = w.poison(syncErr)
		} else {
			w.syncs++
			w.log.stateMu.Lock()
			w.log.status.Watermarks.Durable = w.log.status.Watermarks.Written
			w.log.status.SyncCalls = w.syncs
			w.log.stateMu.Unlock()
		}
	}
	w.complete(request, appendResponse{err: err})
}

func (w *writer) rejectOutstanding() {
	if w.current != nil && !w.current.stop {
		w.reject(w.current, w.fault)
	}
	if w.deferred != nil && !w.deferred.stop {
		w.reject(w.deferred, w.fault)
	} else if w.deferred != nil {
		w.complete(w.deferred, appendResponse{err: w.fault})
		return
	}
	for {
		request := <-w.log.requests
		if request.stop {
			w.complete(request, appendResponse{err: w.fault})
			return
		}
		w.reject(request, w.fault)
	}
}
