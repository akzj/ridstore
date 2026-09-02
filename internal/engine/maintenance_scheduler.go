package engine

import (
	"context"
	"sync/atomic"
	"time"
)

// MaintenanceScheduler arbitrates low-priority maintenance builders. It does
// not gate foreground operations and does not serialize Checkpoint: a
// Checkpoint may preempt a Mapping rewrite, whose COW result is version
// checked before publication.
type MaintenanceScheduler struct {
	mappingRewrite  atomic.Uint32
	dataMaintenance atomic.Uint32
}

func (m *MaintenanceScheduler) acquireData(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if m.dataMaintenance.CompareAndSwap(0, 1) {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *MaintenanceScheduler) releaseData() { m.dataMaintenance.Store(0) }

func (m *MaintenanceScheduler) acquireMappingRewrite(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if m.mappingRewrite.CompareAndSwap(0, 1) {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *MaintenanceScheduler) releaseMappingRewrite() {
	m.mappingRewrite.Store(0)
}
