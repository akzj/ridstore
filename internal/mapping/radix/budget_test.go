package radix

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func TestDeltaBudgetWaitsUntilCheckpointReleasesFrozenCharge(t *testing.T) {
	dir, manifest := radixFixture(t)
	mapping, err := Open(dir, manifest, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	if err := mapping.SetDeltaLimits(64, 128); err != nil {
		t.Fatal(err)
	}
	reservation, soft, err := mapping.ReserveDelta(context.Background(), 2)
	if err != nil || !soft {
		t.Fatalf("reservation=%v soft=%v error=%v", reservation, soft, err)
	}
	addr1, _ := base.NewVAddr(1, 4096)
	addr2, _ := base.NewVAddr(1, 8192)
	changes := []api.Change{{RecordID: 1, NewAddr: addr1}, {RecordID: 2, NewAddr: addr2}}
	if _, err := mapping.ApplyReserved(reservation, 1, api.ApplyUserCommit, changes); err != nil {
		t.Fatal(err)
	}
	reserved := make(chan api.DeltaReservation, 1)
	errResult := make(chan error, 1)
	go func() {
		reservation, _, err := mapping.ReserveDelta(context.Background(), 1)
		if err != nil {
			errResult <- err
			return
		}
		reserved <- reservation
	}()
	select {
	case <-reserved:
		t.Fatal("reservation crossed hard limit")
	case err := <-errResult:
		t.Fatal(err)
	case <-time.After(20 * time.Millisecond):
	}
	checkpoint, err := mapping.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	root, err := mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	select {
	case reservation := <-reserved:
		reservation.Release()
	case err := <-errResult:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not wake delta waiter")
	}
	charged, pending := mapping.DeltaBytes()
	if charged != 0 || pending != 0 {
		t.Fatalf("charged=%d reserved=%d", charged, pending)
	}
}

func TestDeltaBudgetCancellationReturnsReservation(t *testing.T) {
	dir, manifest := radixFixture(t)
	mapping, err := Open(dir, manifest, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	if err := mapping.SetDeltaLimits(64, 128); err != nil {
		t.Fatal(err)
	}
	reservation, _, err := mapping.ReserveDelta(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := mapping.ReserveDelta(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error=%v", err)
	}
	reservation.Release()
	if _, pending := mapping.DeltaBytes(); pending != 0 {
		t.Fatalf("reserved=%d", pending)
	}
}
