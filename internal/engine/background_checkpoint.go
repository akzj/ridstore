package engine

import (
	"context"
	"errors"
	"sync"
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

// checkpointRuntime owns checkpoint capture serialization, worker lifecycle,
// waiter coalescing, and Delta-pressure generations. Keeping these fields
// together makes their lock ownership explicit without changing the existing
// Store-level checkpoint protocol.
type checkpointRuntime struct {
	captureMu sync.Mutex

	requests chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	requestMu     sync.Mutex
	waiters       []checkpointWaiter
	workerStopped bool
	interval      time.Duration

	pressurePending   atomic.Uint64
	pressureCompleted atomic.Uint64
}

func (s *Store) startCheckpointWorker() {
	s.checkpoints.requests = make(chan struct{}, 1)
	s.checkpoints.stop = make(chan struct{})
	s.checkpoints.done = make(chan struct{})
	s.checkpoints.requestMu.Lock()
	s.checkpoints.workerStopped = false
	s.checkpoints.requestMu.Unlock()
	go s.runCheckpointWorker()
}

func (s *Store) wakeCheckpointWorker() {
	select {
	case s.checkpoints.requests <- struct{}{}:
	default:
	}
}

func (s *Store) requestBackgroundCheckpoint(generation uint64) {
	if s == nil || s.checkpoints.requests == nil || generation == 0 {
		return
	}
	if generation <= s.checkpoints.pressureCompleted.Load() {
		return
	}
	if !advanceAtomic(&s.checkpoints.pressurePending, generation) {
		return
	}
	if generation <= s.checkpoints.pressureCompleted.Load() {
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
	if generation != 0 && generation <= s.checkpoints.pressureCompleted.Load() {
		return nil
	}
	waiter := checkpointWaiter{
		generation: generation, force: force, gcAdmission: gcAdmission,
		result: make(chan error, 1),
	}
	s.checkpoints.requestMu.Lock()
	if s.checkpoints.workerStopped {
		s.checkpoints.requestMu.Unlock()
		return base.ErrClosed
	}
	if generation != 0 && generation <= s.checkpoints.pressureCompleted.Load() {
		s.checkpoints.requestMu.Unlock()
		return nil
	}
	s.checkpoints.waiters = append(s.checkpoints.waiters, waiter)
	s.checkpoints.requestMu.Unlock()
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
	advanceAtomic(&s.checkpoints.pressureCompleted, generation)
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
	s.checkpoints.requestMu.Lock()
	waiters := s.checkpoints.waiters
	s.checkpoints.waiters = nil
	s.checkpoints.requestMu.Unlock()
	return waiters
}

func (s *Store) periodicCheckpointNeeded() bool {
	charged, _, _, _ := s.mapping.DeltaUsage()
	return charged != 0
}

func (s *Store) runCheckpointCycle(periodic bool) {
	waiters := s.takeCheckpointWaiters()
	completed := s.checkpoints.pressureCompleted.Load()
	pending := s.checkpoints.pressurePending.Load()
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
	timer := time.NewTicker(s.checkpoints.interval)
	defer func() {
		timer.Stop()
		s.checkpoints.requestMu.Lock()
		s.checkpoints.workerStopped = true
		waiters := s.checkpoints.waiters
		s.checkpoints.waiters = nil
		s.checkpoints.requestMu.Unlock()
		for _, waiter := range waiters {
			waiter.result <- base.ErrClosed
		}
		close(s.checkpoints.done)
	}()
	for {
		select {
		case <-s.checkpoints.stop:
			return
		case <-s.checkpoints.requests:
			s.runCheckpointCycle(false)
		case <-timer.C:
			s.runCheckpointCycle(true)
		}
	}
}

func (s *Store) stopCheckpointWorker() {
	if s == nil || s.checkpoints.stop == nil {
		return
	}
	s.checkpoints.stopOnce.Do(func() { close(s.checkpoints.stop) })
	<-s.checkpoints.done
}
