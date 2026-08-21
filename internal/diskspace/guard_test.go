package diskspace

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

func TestGuardRefreshDoesNotLoseConcurrentAdmissions(t *testing.T) {
	now := time.Unix(1, 0)
	refreshStarted := make(chan struct{})
	allowRefresh := make(chan struct{})
	checks := 0
	guard, err := NewGuard("/store", 100, time.Second, func(string) (uint64, error) {
		checks++
		if checks == 2 {
			close(refreshStarted)
			<-allowRefresh
		}
		return 1000, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	guard.now = func() time.Time { return now }
	if err := guard.Reserve(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	results := make(chan error, 2)
	go func() { results <- guard.Admit(context.Background(), 100) }()
	select {
	case <-refreshStarted:
		t.Fatal("refresh crossed an in-flight append reservation")
	case <-time.After(10 * time.Millisecond):
	}
	guard.Release()
	<-refreshStarted
	go func() { results <- guard.Admit(context.Background(), 100) }()
	close(allowRefresh)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if snapshot := guard.Snapshot(); checks != 2 || snapshot.AvailableBytes != 800 {
		t.Fatalf("checks=%d snapshot=%+v", checks, snapshot)
	}
}

func TestGuardConservativelyAccountsAndRefreshes(t *testing.T) {
	now := time.Unix(1, 0)
	available := uint64(150)
	checks := 0
	guard, err := NewGuard("/store", 100, time.Second, func(string) (uint64, error) {
		checks++
		return available, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	guard.now = func() time.Time { return now }
	if err := guard.Admit(context.Background(), 40); err != nil {
		t.Fatal(err)
	}
	if err := guard.Admit(context.Background(), 11); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("second admission error=%v", err)
	}
	snapshot := guard.Snapshot()
	if checks != 1 || snapshot.AvailableBytes != 110 || !snapshot.Stopped || snapshot.Rejections != 1 {
		t.Fatalf("checks=%d snapshot=%+v", checks, snapshot)
	}
	available = 300
	now = now.Add(time.Second)
	if err := guard.Admit(context.Background(), 50); err != nil {
		t.Fatal(err)
	}
	snapshot = guard.Snapshot()
	if checks != 2 || snapshot.AvailableBytes != 250 || snapshot.Stopped {
		t.Fatalf("checks=%d snapshot=%+v", checks, snapshot)
	}
}

func TestGuardPropagatesContextAndObservationErrors(t *testing.T) {
	if _, err := NewGuard("/store", 1, time.Minute+1, func(string) (uint64, error) { return 1, nil }); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("oversized interval error=%v", err)
	}
	guard, err := NewGuard("/store", 100, time.Second, func(string) (uint64, error) { return 0, syscall.EIO })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := guard.Admit(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission error=%v", err)
	}
	if err := guard.Admit(context.Background(), 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("observation error=%v", err)
	}
	if err := guard.Admit(context.Background(), 1); !errors.Is(err, syscall.EIO) {
		t.Fatalf("cached observation error=%v", err)
	}
	if snapshot := guard.Snapshot(); snapshot.CheckErrors != 1 || snapshot.Rejections != 0 || !snapshot.Stopped {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}
