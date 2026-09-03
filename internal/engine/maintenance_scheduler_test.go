package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

type fakeMaintenanceFactory struct {
	newWorker func(maintenanceRequest) maintenanceWorker
}

func (f fakeMaintenanceFactory) NewMaintenanceWorker(r maintenanceRequest) (maintenanceWorker, error) {
	if f.newWorker == nil {
		return nil, base.ErrInvalidConfig
	}
	return f.newWorker(r), nil
}

type fakeMaintenanceWorker struct {
	resources func(maintenancePhase) maintenanceResource
	run       func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition
}

func (w *fakeMaintenanceWorker) Resources(p maintenancePhase) maintenanceResource {
	return w.resources(p)
}
func (w *fakeMaintenanceWorker) Run(ctx context.Context, p maintenancePhase, r maintenanceResult) maintenanceTransition {
	return w.run(ctx, p, r)
}

func TestMaintenanceSchedulerPriorityAndFIFO(t *testing.T) {
	blockStarted, release := make(chan struct{}), make(chan struct{})
	order := make(chan maintenanceRequestKind, 3)
	factory := fakeMaintenanceFactory{newWorker: func(r maintenanceRequest) maintenanceWorker {
		return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceHeavyIO }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
			if r.source == 1 {
				close(blockStarted)
				<-release
			} else {
				order <- r.kind
			}
			return maintenanceTransition{done: true}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	defer scheduler.Close()
	if err := scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceSegmentRelocateRequest, source: 1}); err != nil {
		t.Fatal(err)
	}
	<-blockStarted
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingSurveyRequest})
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingGCRequest})
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceCheckpointRequest})
	close(release)
	got := []maintenanceRequestKind{<-order, <-order, <-order}
	want := []maintenanceRequestKind{maintenanceCheckpointRequest, maintenanceMappingSurveyRequest, maintenanceMappingGCRequest}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v want=%v", got, want)
		}
	}
}

func TestMaintenanceSchedulerCoalescesAndRerunsActiveCheckpoint(t *testing.T) {
	started, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Uint64
	factory := fakeMaintenanceFactory{newWorker: func(maintenanceRequest) maintenanceWorker {
		return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceMappingWriter }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			} else {
				close(second)
			}
			return maintenanceTransition{done: true}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	defer scheduler.Close()
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceCheckpointRequest})
	<-started
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceCheckpointRequest})
	close(release)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("checkpoint was not rerun")
	}
	if scheduler.metrics().coalesced != 1 {
		t.Fatalf("coalesced=%d", scheduler.metrics().coalesced)
	}
}

func TestMaintenanceSchedulerDependencyReleasesPhaseResourcesAndResumes(t *testing.T) {
	var mu sync.Mutex
	order := make([]string, 0, 3)
	factory := fakeMaintenanceFactory{newWorker: func(r maintenanceRequest) maintenanceWorker {
		if r.kind == maintenanceCheckpointRequest {
			return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceMappingWriter }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
				mu.Lock()
				order = append(order, "checkpoint")
				mu.Unlock()
				return maintenanceTransition{done: true, result: maintenanceResult{found: true}}
			}}
		}
		return &fakeMaintenanceWorker{resources: func(p maintenancePhase) maintenanceResource {
			if p == maintenancePhaseStart {
				return maintenanceHeavyIO | maintenanceRecoveryProtocol
			}
			return maintenanceRecoveryProtocol
		}, run: func(_ context.Context, p maintenancePhase, dependency maintenanceResult) maintenanceTransition {
			mu.Lock()
			defer mu.Unlock()
			if p == maintenancePhaseStart {
				order = append(order, "copy")
				return maintenanceTransition{next: maintenancePhaseProve, retain: maintenanceRecoveryProtocol, dependency: &maintenanceRequest{kind: maintenanceCheckpointRequest}}
			}
			if !dependency.found {
				return maintenanceTransition{done: true, err: errors.New("missing dependency result")}
			}
			order = append(order, "prove")
			return maintenanceTransition{done: true}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	defer scheduler.Close()
	if _, err := scheduler.Submit(context.Background(), maintenanceRequest{kind: maintenanceSegmentCompactRequest, source: 9}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"copy", "checkpoint", "prove"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v want=%v", order, want)
		}
	}
}

func TestMaintenanceSchedulerCheckpointRunsDuringSegmentCopy(t *testing.T) {
	copyStarted, releaseCopy := make(chan struct{}), make(chan struct{})
	checkpointRan := make(chan struct{})
	factory := fakeMaintenanceFactory{newWorker: func(r maintenanceRequest) maintenanceWorker {
		if r.kind == maintenanceCheckpointRequest {
			return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceMappingWriter }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
				close(checkpointRan)
				return maintenanceTransition{done: true}
			}}
		}
		return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceHeavyIO | maintenanceRecoveryProtocol }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
			close(copyStarted)
			<-releaseCopy
			return maintenanceTransition{done: true}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	defer scheduler.Close()
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceSegmentCompactRequest, source: 7})
	<-copyStarted
	if _, err := scheduler.Submit(context.Background(), maintenanceRequest{kind: maintenanceCheckpointRequest}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-checkpointRan:
	case <-time.After(time.Second):
		t.Fatal("checkpoint waited behind segment copy")
	}
	close(releaseCopy)
}

func TestMaintenanceSchedulerWaiterCancellationDoesNotCancelBackgroundJob(t *testing.T) {
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	factory := fakeMaintenanceFactory{newWorker: func(maintenanceRequest) maintenanceWorker {
		return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceHeavyIO }, run: func(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
			close(started)
			<-release
			close(finished)
			return maintenanceTransition{done: true}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	defer scheduler.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := scheduler.Submit(ctx, maintenanceRequest{kind: maintenanceMappingSurveyRequest})
		done <- err
	}()
	<-started
	_ = scheduler.SubmitBackground(maintenanceRequest{kind: maintenanceMappingSurveyRequest})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	close(release)
	<-finished
}

func TestMaintenanceSchedulerCloseCancelsAndRejects(t *testing.T) {
	started := make(chan struct{})
	factory := fakeMaintenanceFactory{newWorker: func(maintenanceRequest) maintenanceWorker {
		return &fakeMaintenanceWorker{resources: func(maintenancePhase) maintenanceResource { return maintenanceHeavyIO }, run: func(ctx context.Context, _ maintenancePhase, _ maintenanceResult) maintenanceTransition {
			close(started)
			<-ctx.Done()
			return maintenanceTransition{done: true, err: ctx.Err()}
		}}
	}}
	scheduler := newMaintenanceScheduler(context.Background(), factory)
	done := make(chan error, 1)
	go func() {
		_, err := scheduler.Submit(context.Background(), maintenanceRequest{kind: maintenanceMappingSurveyRequest})
		done <- err
	}()
	<-started
	scheduler.Close()
	if err := <-done; !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := scheduler.Submit(context.Background(), maintenanceRequest{kind: maintenanceCheckpointRequest}); !errors.Is(err, base.ErrClosed) {
		t.Fatalf("late err=%v", err)
	}
}
