package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

func TestStoreLifecycleCancelsAndWaitsForActiveOperations(t *testing.T) {
	lifecycle := newStoreLifecycle()
	opCtx, end, err := lifecycle.begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	shutdownStarted := make(chan struct{})
	lifecycle.startClose(func() error {
		close(shutdownStarted)
		<-lifecycle.Drained()
		return nil
	})
	<-shutdownStarted
	select {
	case <-opCtx.Done():
		if !errors.Is(context.Cause(opCtx), base.ErrClosed) {
			t.Fatalf("cause=%v", context.Cause(opCtx))
		}
	case <-time.After(time.Second):
		t.Fatal("operation context was not canceled")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lifecycle.wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait err=%v", err)
	}
	if _, _, err := lifecycle.begin(context.Background()); !errors.Is(err, base.ErrClosed) {
		t.Fatalf("late begin err=%v", err)
	}

	end()
	select {
	case <-lifecycle.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after active operation ended")
	}
}

func TestMaintenanceSchedulerStopsWhenOwnerContextIsCanceled(t *testing.T) {
	owner, cancelOwner := context.WithCancel(context.Background())
	started := make(chan struct{})
	factory := fakeMaintenanceFactory{newWorker: func(maintenanceRequest) maintenanceWorker {
		return &fakeMaintenanceWorker{
			resources: func(maintenancePhase) maintenanceResource { return maintenanceHeavyIO },
			run: func(ctx context.Context, _ maintenancePhase, _ maintenanceResult) maintenanceTransition {
				close(started)
				<-ctx.Done()
				return maintenanceTransition{done: true, err: ctx.Err()}
			},
		}
	}}
	scheduler := newMaintenanceScheduler(owner, factory)
	if err := scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingSurveyRequest}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancelOwner()
	select {
	case <-scheduler.Done():
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after owner context cancellation")
	}
}
