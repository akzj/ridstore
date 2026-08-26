package mapstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/recordlog"
)

func TestVerifyFilesAcceptsCompleteMappingSet(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	var slots [NodeSlots]uint64
	addr, _ := recordlog.NewVAddr(1, 64, 64)
	slots[1] = uint64(addr)
	if _, err := store.Append(0, 0, 1, slots); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyFiles(context.Background(), root, state)
	if err != nil {
		t.Fatal(err)
	}
	if report.Segments != 1 || report.SealedSegments != 0 || report.Nodes != 1 || report.ActiveEnd <= SegmentHeaderSize {
		t.Fatalf("report=%+v", report)
	}
}

func TestVerifyFilesRejectsPartialMappingTailWithoutRepair(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, mappingDirectory, activeName(1))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFiles(context.Background(), root, state); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("verify err=%v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Fatalf("verify changed active size from %d to %d", before.Size(), after.Size())
	}
}

func TestVerifyFilesRejectsUnexpectedMappingFile(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, mappingDirectory, "unknown"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFiles(context.Background(), root, state); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify err=%v", err)
	}
}
