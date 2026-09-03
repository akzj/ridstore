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

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	requestMu     sync.Mutex
	waiters       []checkpointWaiter
	workerStopped bool
	interval      time.Duration

	pressurePending   atomic.Uint64
	pressureCompleted atomic.Uint64
	periodicPending   atomic.Bool
}

func (s *Store) startCheckpointWorker() {
	s.checkpoints.stop = make(chan struct{})
	s.checkpoints.done = make(chan struct{})
	s.checkpoints.requestMu.Lock()
	s.checkpoints.workerStopped = false
	s.checkpoints.requestMu.Unlock()
	go s.runCheckpointTimer()
}

func (s *Store) wakeCheckpointWorker() {
	if s.maintenance.scheduler == nil {
		return
	}
	_ = s.maintenance.scheduler.submitBackground(maintenanceJobSpec{
		key: "checkpoint", priority: maintenancePriorityCheckpoint,
		resources: maintenanceMappingWriter, rerunOnActive: true,
		run: func(ctx context.Context) error {
			s.runCheckpointCycle(ctx, s.checkpoints.periodicPending.Swap(false))
			return nil
		},
	})
}

func (s *Store) requestBackgroundCheckpoint(generation uint64) {
	if s == nil || s.maintenance.scheduler == nil || generation == 0 {
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
	charged, _, _, _ := s.core.mapping.DeltaUsage()
	return charged != 0
}

func (s *Store) runCheckpointCycle(ctx context.Context, periodic bool) {
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
	err := s.executeCheckpoint(ctx, gcAdmission)
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
			err = s.executeCheckpoint(ctx, false)
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
	if err != nil && (pressure || periodic) && !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) {
		s.setFault(err)
	}
	for _, waiter := range active {
		waiter.result <- err
	}
}

func (s *Store) runCheckpointTimer() {
	timer := time.NewTicker(s.checkpoints.interval)
	var maintenanceTimer *time.Ticker
	var maintenanceC <-chan time.Time
	if s.maintenance.config.Enabled {
		maintenanceTimer = time.NewTicker(s.maintenance.config.Interval)
		maintenanceC = maintenanceTimer.C
	}
	defer func() {
		timer.Stop()
		if maintenanceTimer != nil {
			maintenanceTimer.Stop()
		}
		close(s.checkpoints.done)
	}()
	for {
		select {
		case <-s.checkpoints.stop:
			return
		case <-timer.C:
			s.checkpoints.periodicPending.Store(true)
			s.wakeCheckpointWorker()
		case <-maintenanceC:
			s.scheduleAutomaticMaintenance()
		}
	}
}

func (s *Store) stopCheckpointWorker() {
	if s == nil {
		return
	}
	if s.checkpoints.stop == nil {
		if s.maintenance.scheduler != nil {
			s.maintenance.scheduler.Close()
		}
		return
	}
	s.checkpoints.stopOnce.Do(func() { close(s.checkpoints.stop) })
	<-s.checkpoints.done
	s.checkpoints.requestMu.Lock()
	s.checkpoints.workerStopped = true
	waiters := s.checkpoints.waiters
	s.checkpoints.waiters = nil
	s.checkpoints.requestMu.Unlock()
	if s.maintenance.scheduler != nil {
		s.maintenance.scheduler.Close()
	}
	for _, waiter := range waiters {
		waiter.result <- base.ErrClosed
	}
}
