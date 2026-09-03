package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

// checkpointRuntime owns checkpoint capture serialization, worker lifecycle,
// and Delta-pressure generations. Request coalescing and lifecycle belong to
// MaintenanceScheduler. Keeping these fields
// together makes their lock ownership explicit without changing the existing
// Store-level checkpoint protocol.
type checkpointRuntime struct {
	captureMu sync.Mutex

	interval time.Duration

	pressurePending   atomic.Uint64
	pressureCompleted atomic.Uint64
}

func (s *Store) startCheckpointWorker() {
	if err := s.maintenance.scheduler.Configure(s.checkpoints.interval, s.maintenance.config); err != nil {
		s.setFault(err)
	}
}

func (s *Store) wakeCheckpointWorker() {
	if s.maintenance.scheduler == nil {
		return
	}
	_ = s.maintenance.scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceCheckpointRequest, generation: s.checkpoints.pressurePending.Load()})
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
	_, err := s.maintenance.scheduler.Submit(ctx, maintenanceRequest{kind: maintenanceCheckpointRequest, generation: generation, force: force, gcAdmission: gcAdmission})
	return err
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

func (s *Store) periodicCheckpointNeeded() bool {
	charged, _, _, _ := s.core.mapping.DeltaUsage()
	return charged != 0
}

func (s *Store) runCheckpointCycle(ctx context.Context, periodic, forced, dependencyGCAdmission bool, requestedGeneration uint64) error {
	completed := s.checkpoints.pressureCompleted.Load()
	pending := s.checkpoints.pressurePending.Load()
	if requestedGeneration > pending {
		pending = requestedGeneration
	}
	pressure := pending > completed
	needed := forced || pressure || (periodic && s.periodicCheckpointNeeded())
	gcAdmission := dependencyGCAdmission
	if !needed {
		return nil
	}
	err := s.executeCheckpoint(ctx, gcAdmission)
	var admissionErr error
	if err != nil && gcAdmission {
		// GC admission failure (most notably insufficient copy headroom) is
		// recoverable and must not prevent an ordinary pressure checkpoint.
		admissionErr = err
		if pressure || periodic {
			err = s.executeCheckpoint(ctx, false)
		} else {
			return admissionErr
		}
	}
	if pressure {
		if err == nil {
			s.metrics.backgroundCheckpointCompleted.Add(1)
		} else if !checkpointConflict(err) {
			s.metrics.backgroundCheckpointFailed.Add(1)
		}
	}
	if err != nil && (pressure || periodic) && !checkpointConflict(err) &&
		!errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) {
		s.setFault(err)
	}
	if admissionErr != nil {
		return admissionErr
	}
	return err
}

func (s *Store) stopCheckpointWorker() {
	if s == nil || s.maintenance.scheduler == nil {
		return
	}
	s.maintenance.scheduler.Close()
}
