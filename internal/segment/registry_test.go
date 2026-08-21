package segment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestRegistryReadsActiveSegment(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	addr, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := registry.ReadFrame(addr)
	if err != nil || frame.RecordID != 1 {
		t.Fatalf("frame=%+v error=%v", frame, err)
	}
}

func TestRegistryRetireWaitsForPinnedReaderAndKeepsTombstone(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	oldActive, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	addr, _, err := oldActive.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("pinned")})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := oldActive.Seal(2)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := OpenSealedData(root, uuid, summary, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	createActiveDataFileWithFirstSeq(t, root, uuid, 2, 3)
	active, err := OpenActiveData(root, uuid, 2, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(active, []*SealedData{sealed})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	pin, err := registry.Acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Retire(1); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(1); !errors.Is(err, ErrRetired) {
		t.Fatalf("acquire retired error=%v", err)
	}
	if frame, err := pin.ReadFrame(addr); err != nil || string(frame.Payload) != "pinned" {
		t.Fatalf("frame=%+v error=%v", frame, err)
	}
	waited := make(chan error, 1)
	go func() { waited <- registry.WaitForNoReaders(context.Background(), 1) }()
	select {
	case err := <-waited:
		t.Fatalf("wait returned before release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := pin.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	detached, err := registry.DetachRetired(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := detached.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(1); !errors.Is(err, ErrRetired) {
		t.Fatalf("detached tombstone error=%v", err)
	}
}

func TestRegistryRetireRejectsOpenBatchReference(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	oldActive, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := oldActive.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1}); err != nil {
		t.Fatal(err)
	}
	summary, err := oldActive.Seal(2)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := OpenSealedData(root, uuid, summary, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	createActiveDataFileWithFirstSeq(t, root, uuid, 2, 3)
	active, err := OpenActiveData(root, uuid, 2, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(active, []*SealedData{sealed})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := registry.PinOpenBatch(1); err != nil {
		t.Fatal(err)
	}
	if err := registry.Retire(1); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("retire error=%v", err)
	}
	if err := registry.UnpinOpenBatch(1); err != nil {
		t.Fatal(err)
	}
	if err := registry.Retire(1); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryCleaningExcludesLateOpenBatchPin(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	oldActive, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	addr, _, err := oldActive.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := oldActive.Seal(2)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := OpenSealedData(root, uuid, summary, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	createActiveDataFileWithFirstSeq(t, root, uuid, 2, 3)
	active, err := OpenActiveData(root, uuid, 2, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRegistry(active, []*SealedData{sealed})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := registry.BeginCleaning(1); err != nil {
		t.Fatal(err)
	}
	if err := registry.PinOpenBatch(1); !errors.Is(err, ErrCleaning) {
		t.Fatalf("late open ref error=%v", err)
	}
	var scanned int
	if err := registry.ScanCleaning(1, func(got base.VAddr, _ storeformat.Frame) error {
		if got == addr {
			scanned++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if scanned != 1 {
		t.Fatalf("scanned=%d", scanned)
	}
	if err := registry.RetireCleaning(1); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(1); !errors.Is(err, ErrRetired) {
		t.Fatalf("retired acquire error=%v", err)
	}
}

func createActiveDataFileWithFirstSeq(t *testing.T, root string, uuid base.StoreUUID, id base.DataSegmentID, first base.FrameSeq) {
	t.Helper()
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{
		Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: uint32(id), FirstSeq: uint64(first),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", ActiveDataFileName(id)), header[:], 0o600); err != nil {
		t.Fatal(err)
	}
}
