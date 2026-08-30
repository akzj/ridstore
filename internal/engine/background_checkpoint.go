package engine

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/akzj/ridstore/internal/base"
)

func (s *Store) startBackgroundCheckpoint() {
	s.checkpointRequests = make(chan struct{}, 1)
	s.checkpointStop = make(chan struct{})
	s.checkpointDone = make(chan struct{})
	go s.runBackgroundCheckpoint()
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
	select {
	case s.checkpointRequests <- struct{}{}:
	default:
	}
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

func (s *Store) runBackgroundCheckpoint() {
	defer close(s.checkpointDone)
	for {
		select {
		case <-s.checkpointStop:
			return
		default:
		}
		select {
		case <-s.checkpointStop:
			return
		case <-s.checkpointRequests:
		}
		select {
		case <-s.checkpointStop:
			return
		default:
		}
		for s.checkpointPressurePending.Load() > s.checkpointPressureCompleted.Load() {
			select {
			case <-s.checkpointStop:
				return
			default:
			}
			if err := s.Checkpoint(context.Background()); err != nil {
				if !errors.Is(err, base.ErrClosed) {
					s.setFault(err)
				}
				s.metrics.backgroundCheckpointFailed.Add(1)
				break
			}
			s.metrics.backgroundCheckpointCompleted.Add(1)
		}
	}
}

func (s *Store) stopBackgroundCheckpoint() {
	if s == nil || s.checkpointStop == nil {
		return
	}
	s.checkpointStopOnce.Do(func() { close(s.checkpointStop) })
	<-s.checkpointDone
}
