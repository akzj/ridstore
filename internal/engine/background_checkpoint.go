package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

type checkpointWaiter struct {
	generation  uint64
	force       bool
	gcAdmission bool
	result      chan error
}

func (s *Store) startCheckpointWorker() {
	s.checkpointRequests = make(chan struct{}, 1)
	s.checkpointStop = make(chan struct{})
	s.checkpointDone = make(chan struct{})
	s.checkpointRequestMu.Lock()
	s.checkpointWorkerStopped = false
	s.checkpointRequestMu.Unlock()
	go s.runCheckpointWorker()
}

func (s *Store) wakeCheckpointWorker() {
	select {
	case s.checkpointRequests <- struct{}{}:
	default:
	}
}

func (s *Store) requestBackgroundCheckpoint(generation uint64) {
	if s == nil || s.checkpointRequests == nil || generation == 0 {
		return
	}
	if generation <= s.checkpointPressureCompleted.Load() {
		return
	}
	if !advanceAtomic(&s.checkpointPressurePending, generation) {
		return
	}
	if generation <= s.checkpointPressureCompleted.Load() {
		return
	}
	s.metrics.backgroundCheckpointRequested.Add(1)
	s.wakeCheckpointWorker()
}

func (s *Store) requestCheckpoint(ctx context.Context, generation uint64, force, gcAdmission bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if generation != 0 && generation <= s.checkpointPressureCompleted.Load() {
		return nil
	}
	waiter := checkpointWaiter{
		generation: generation, force: force, gcAdmission: gcAdmission,
		result: make(chan error, 1),
	}
	s.checkpointRequestMu.Lock()
	if s.checkpointWorkerStopped {
		s.checkpointRequestMu.Unlock()
		return base.ErrClosed
	}
	if generation != 0 && generation <= s.checkpointPressureCompleted.Load() {
		s.checkpointRequestMu.Unlock()
		return nil
	}
	s.checkpointWaiters = append(s.checkpointWaiters, waiter)
	s.checkpointRequestMu.Unlock()
	s.wakeCheckpointWorker()
	select {
	case err := <-waiter.result:
		return err
	case <-ctx.Done():
		// The worker still completes shared work. The buffered result prevents a
		// cancelled caller from blocking delivery to the other waiters.
		return ctx.Err()
	}
}

func (s *Store) awaitCheckpointPressure(ctx context.Context, generation uint64, gcAdmission bool) error {
	if generation == 0 {
		return base.ErrInvalidConfig
	}
	s.requestBackgroundCheckpoint(generation)
	return s.requestCheckpoint(ctx, generation, false, gcAdmission)
}

func (s *Store) completeCheckpointPressure(generation uint64) {
	if s == nil || generation == 0 {
		return
	}
	advanceAtomic(&s.checkpointPressureCompleted, generation)
}

func advanceAtomic(value *atomic.Uint64, candidate uint64) bool {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return true
		}
	}
	return false
}

func (s *Store) takeCheckpointWaiters() []checkpointWaiter {
	s.checkpointRequestMu.Lock()
	waiters := s.checkpointWaiters
	s.checkpointWaiters = nil
	s.checkpointRequestMu.Unlock()
	return waiters
}

func (s *Store) periodicCheckpointNeeded() bool {
	charged, _, _, _ := s.mapping.DeltaUsage()
	return charged != 0
}

func (s *Store) runCheckpointCycle(periodic bool) {
	waiters := s.takeCheckpointWaiters()
	completed := s.checkpointPressureCompleted.Load()
	pending := s.checkpointPressurePending.Load()
	pressure := pending > completed
	needed := pressure || (periodic && s.periodicCheckpointNeeded())
	gcAdmission := false
	active := waiters[:0]
	for _, waiter := range waiters {
		if waiter.generation != 0 && waiter.generation <= completed && !waiter.force {
			waiter.result <- nil
			continue
		}
		needed = needed || waiter.force || waiter.generation != 0
		gcAdmission = gcAdmission || waiter.gcAdmission
		active = append(active, waiter)
	}
	if !needed {
		return
	}
	err := s.executeCheckpoint(context.Background(), gcAdmission)
	if err != nil && gcAdmission {
		// GC admission failure (most notably insufficient copy headroom) is
		// recoverable and must not prevent an ordinary pressure checkpoint.
		normal := make([]checkpointWaiter, 0, len(active))
		for _, waiter := range active {
			if waiter.gcAdmission {
				waiter.result <- err
			} else {
				normal = append(normal, waiter)
			}
		}
		active = normal
		if pressure || periodic || len(active) != 0 {
			err = s.executeCheckpoint(context.Background(), false)
		} else {
			return
		}
	}
	if pressure {
		if err == nil {
			s.metrics.backgroundCheckpointCompleted.Add(1)
		} else {
			s.metrics.backgroundCheckpointFailed.Add(1)
		}
	}
	if err != nil && (pressure || periodic) && !errors.Is(err, base.ErrClosed) {
		s.setFault(err)
	}
	for _, waiter := range active {
		waiter.result <- err
	}
}

func (s *Store) runCheckpointWorker() {
	timer := time.NewTicker(s.checkpointInterval)
	defer func() {
		timer.Stop()
		s.checkpointRequestMu.Lock()
		s.checkpointWorkerStopped = true
		waiters := s.checkpointWaiters
		s.checkpointWaiters = nil
		s.checkpointRequestMu.Unlock()
		for _, waiter := range waiters {
			waiter.result <- base.ErrClosed
		}
		close(s.checkpointDone)
	}()
	for {
		select {
		case <-s.checkpointStop:
			return
		case <-s.checkpointRequests:
			s.runCheckpointCycle(false)
		case <-timer.C:
			s.runCheckpointCycle(true)
		}
	}
}

func (s *Store) stopCheckpointWorker() {
	if s == nil || s.checkpointStop == nil {
		return
	}
	s.checkpointStopOnce.Do(func() { close(s.checkpointStop) })
	<-s.checkpointDone
}
