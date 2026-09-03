package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

func TestMaintenanceSchedulerPriorityAndFIFO(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	defer scheduler.Close()
	release, err := scheduler.acquire(context.Background(), maintenancePrioritySegment, maintenanceHeavyIO)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan string, 3)
	run := func(name string, priority maintenancePriority) {
		if err := scheduler.submitBackground(maintenanceJobSpec{
			key: name, priority: priority, resources: maintenanceHeavyIO,
			run: func(context.Context) error {
				order <- name
				return nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	run("low-1", maintenancePriorityMapping)
	run("low-2", maintenancePriorityMapping)
	run("high", maintenancePriorityCheckpoint)
	release()
	got := make([]string, 0, 3)
	for range 3 {
		got = append(got, <-order)
	}
	if want := []string{"high", "low-1", "low-2"}; !equalStrings(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

func TestMaintenanceSchedulerCoalescesWaiters(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	defer scheduler.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var calls atomic.Uint64
	spec := maintenanceJobSpec{key: "checkpoint", priority: maintenancePriorityCheckpoint, resources: maintenanceHeavyIO,
		run: func(context.Context) error {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			close(finished)
			return nil
		}}
	first := make(chan error, 1)
	go func() { first <- scheduler.submit(context.Background(), spec) }()
	<-started
	if err := scheduler.submitBackground(spec); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	<-finished
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestMaintenanceSchedulerWaiterCancellationDoesNotCancelSharedJob(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	defer scheduler.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	spec := maintenanceJobSpec{key: "shared", priority: maintenancePrioritySegment, resources: maintenanceHeavyIO,
		run: func(context.Context) error { close(started); <-release; return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- scheduler.submit(ctx, spec) }()
	<-started
	if err := scheduler.submitBackground(spec); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v", err)
	}
	close(release)
}

func TestMaintenanceSchedulerGrantsResourcesAtomically(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	defer scheduler.Close()
	releaseHeavy, err := scheduler.acquire(context.Background(), maintenancePrioritySegment, maintenanceHeavyIO)
	if err != nil {
		t.Fatal(err)
	}
	releaseWriter, err := scheduler.acquire(context.Background(), maintenancePrioritySegment, maintenanceMappingWriter)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan struct{}, 1)
	releaseBoth := make(chan struct{})
	if err := scheduler.submitBackground(maintenanceJobSpec{
		key: "both", priority: maintenancePriorityCheckpoint, resources: maintenanceHeavyIO | maintenanceMappingWriter,
		run: func(context.Context) error { acquired <- struct{}{}; <-releaseBoth; return nil },
	}); err != nil {
		t.Fatal(err)
	}
	releaseHeavy()
	select {
	case <-acquired:
		close(releaseBoth)
		t.Fatal("multi-resource lease granted while mappingWriter remained held")
	case <-time.After(10 * time.Millisecond):
	}
	releaseWriter()
	select {
	case <-acquired:
		close(releaseBoth)
	case <-time.After(time.Second):
		t.Fatal("multi-resource lease was not granted")
	}
}

func TestMaintenanceSchedulerCloseCancelsAndDrains(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- scheduler.submit(context.Background(), maintenanceJobSpec{
			key: "running", priority: maintenancePriorityMapping, resources: maintenanceHeavyIO,
			run: func(ctx context.Context) error { close(started); <-ctx.Done(); return ctx.Err() },
		})
	}()
	<-started
	scheduler.Close()
	if err := <-done; !errors.Is(err, base.ErrClosed) && !errors.Is(err, context.Canceled) {
		t.Fatalf("running job error = %v", err)
	}
	if err := scheduler.submit(context.Background(), maintenanceJobSpec{key: "late", run: func(context.Context) error { return nil }}); !errors.Is(err, base.ErrClosed) {
		t.Fatalf("late submit error = %v", err)
	}
}

// A Segment worker holds only recoveryProtocol while requesting Checkpoint's
// heavyIO+mappingWriter resources. This is the lock-ordering invariant that
// prevents the old nested maintenance deadlock.
func TestMaintenanceSchedulerSegmentCanWaitForCheckpoint(t *testing.T) {
	scheduler := newMaintenanceScheduler()
	defer scheduler.Close()
	err := scheduler.submit(context.Background(), maintenanceJobSpec{
		key: "segment", priority: maintenancePrioritySegment, resources: maintenanceRecoveryProtocol,
		run: func(context.Context) error {
			return scheduler.submit(context.Background(), maintenanceJobSpec{
				key: "checkpoint", priority: maintenancePriorityCheckpoint,
				resources: maintenanceHeavyIO | maintenanceMappingWriter,
				run:       func(context.Context) error { return nil },
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
