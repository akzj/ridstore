package engine

import (
	"context"
	"errors"

	"github.com/akzj/ridstore/internal/base"
)

func (s *Store) startBackgroundCheckpoint() {
	s.checkpointRequests = make(chan struct{}, 1)
	s.checkpointStop = make(chan struct{})
	s.checkpointDone = make(chan struct{})
	go s.runBackgroundCheckpoint()
}

func (s *Store) requestBackgroundCheckpoint() {
	if s == nil || s.checkpointRequests == nil {
		return
	}
	select {
	case s.checkpointRequests <- struct{}{}:
		s.metrics.backgroundCheckpointRequested.Add(1)
	default:
	}
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
		if err := s.Checkpoint(context.Background()); err != nil {
			if !errors.Is(err, base.ErrClosed) {
				s.setFault(err)
			}
			s.metrics.backgroundCheckpointFailed.Add(1)
			continue
		}
		s.metrics.backgroundCheckpointCompleted.Add(1)
	}
}

func (s *Store) stopBackgroundCheckpoint() {
	if s == nil || s.checkpointStop == nil {
		return
	}
	s.checkpointStopOnce.Do(func() { close(s.checkpointStop) })
	<-s.checkpointDone
}
