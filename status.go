package ridstore

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/akzj/ridstore/internal/base"
)

// addStatusLocked keeps at most Config.StatusRetention resolved outcomes.
// CommitUnknown is deliberately not part of the eviction queue: the Store is
// faulted after an unknown outcome and the status remains queryable until the
// required reopen resolves it from durable bytes.
func (s *Store) addStatusLocked(status BatchStatus) {
	s.statusSerial++
	entry := statusEntry{status: status, serial: s.statusSerial}
	previous, existed := s.statuses[status.BatchID]
	s.statuses[status.BatchID] = entry
	if status.State != BatchStateCommitUnknown {
		s.resolvedStatusCount++
		s.statusOrder = append(s.statusOrder, statusOrderEntry{id: status.BatchID, serial: entry.serial})
	}
	if existed && previous.status.State != BatchStateCommitUnknown {
		s.resolvedStatusCount--
	}
	s.trimStatusesLocked()
}

func (s *Store) trimStatusesLocked() {
	for s.resolvedStatusCount > s.config.StatusRetention && s.statusOrderHead < len(s.statusOrder) {
		oldest := s.statusOrder[s.statusOrderHead]
		s.statusOrderHead++
		current, exists := s.statuses[oldest.id]
		if !exists || current.serial != oldest.serial || current.status.State == BatchStateCommitUnknown {
			continue
		}
		delete(s.statuses, oldest.id)
		s.resolvedStatusCount--
	}
	if s.statusOrderHead > s.config.StatusRetention && s.statusOrderHead*2 >= len(s.statusOrder) {
		copy(s.statusOrder, s.statusOrder[s.statusOrderHead:])
		s.statusOrder = s.statusOrder[:len(s.statusOrder)-s.statusOrderHead]
		s.statusOrderHead = 0
	}
}

func (s *Store) replayStatusCountLocked() uint64 {
	if s.terminalStatusTotal < s.terminalStatusBase {
		return math.MaxUint64
	}
	return s.terminalStatusTotal - s.terminalStatusBase
}

func (s *Store) statusCapacityAvailableLocked() bool {
	used := s.replayStatusCountLocked()
	reserved, err := base.AddUint64(uint64(s.openCount), uint64(s.internalStatusSlots))
	if err != nil {
		return false
	}
	used, err = base.AddUint64(used, reserved)
	return err == nil && used < uint64(s.config.StatusRetention)
}

func (s *Store) statusCheckpointNeededLocked() bool {
	limit := uint64(s.config.StatusRetention)
	threshold := limit - limit/4
	if threshold == 0 {
		threshold = 1
	}
	return s.replayStatusCountLocked() >= threshold
}

func (s *Store) recordTerminalStatusLocked(status BatchStatus) error {
	if s.terminalStatusTotal == math.MaxUint64 {
		return base.ErrOverflow
	}
	s.terminalStatusTotal++
	s.addStatusLocked(status)
	return nil
}

// reserveInternalStatusSlot protects recovery memory while Data GC creates a
// terminal relocation descriptor without occupying a public Batch slot. Data
// GC already owns checkpointMu, so it returns a retryable capacity error and
// schedules a checkpoint instead of waiting on itself.
func (s *Store) reserveInternalStatusSlot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.statusCapacityAvailableLocked() {
		return base.ErrStatusCapacity
	}
	s.internalStatusSlots++
	return nil
}

func (s *Store) releaseInternalStatusSlot(status *BatchStatus) error {
	s.mu.Lock()
	if s.internalStatusSlots <= 0 {
		s.mu.Unlock()
		return fmt.Errorf("internal status slot underflow: %w", base.ErrCorrupt)
	}
	s.internalStatusSlots--
	var err error
	if status != nil {
		err = s.recordTerminalStatusLocked(*status)
	}
	s.signalSlotLocked()
	needCheckpoint := s.statusCheckpointNeededLocked()
	s.mu.Unlock()
	if needCheckpoint {
		s.requestCheckpoint()
	}
	return err
}

func (s *Store) waitForBatchSlot(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.ops.RLock()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			s.ops.RUnlock()
			return base.ErrClosed
		}
		if s.fault != nil {
			err := errors.Join(base.ErrReadOnly, s.fault)
			s.mu.Unlock()
			s.ops.RUnlock()
			return err
		}
		capacityBlocked := !s.statusCapacityAvailableLocked()
		if s.openCount < s.config.MaxOpenBatches && !capacityBlocked {
			s.openCount++
			if s.openCount < s.config.MaxOpenBatches && s.statusCapacityAvailableLocked() {
				s.signalSlotLocked()
			}
			s.mu.Unlock()
			// Return with the ops read lease held; Begin releases it after the
			// reserved slot has become a published Batch or has been rolled back.
			return nil
		}
		notify := s.slotNotify
		s.slotWaiters++
		s.mu.Unlock()
		s.ops.RUnlock()
		if capacityBlocked {
			s.requestCheckpoint()
		}
		select {
		case <-ctx.Done():
		case <-notify:
		}
		s.mu.Lock()
		s.slotWaiters--
		s.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
