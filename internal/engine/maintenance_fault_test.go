package engine

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestCompactMarkerPublicationBoundary(t *testing.T) {
	for _, test := range []struct {
		point            maintstate.FaultPoint
		recoveryRequired bool
	}{
		{maintstate.FaultBeforeWrite, false},
		{maintstate.FaultBeforeFileSync, false},
		{maintstate.FaultBeforeFileClose, false},
		{maintstate.FaultBeforePublishRename, false},
		{maintstate.FaultBeforeJournalDirSync, true},
	} {
		t.Run(string(test.point), func(t *testing.T) {
			store, source, id, _, _ := relocationFixture(t)
			root := store.root
			injected := errors.New("injected marker install failure")
			store.maintenanceHook = func(got maintstate.FaultPoint) error {
				if got == test.point {
					return injected
				}
				return nil
			}
			_, err := store.CompactSegment(context.Background(), source)
			if !errors.Is(err, injected) || errors.Is(err, base.ErrRecoveryRequired) != test.recoveryRequired {
				t.Fatalf("compact err=%v recoveryRequired=%v", err, test.recoveryRequired)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if _, err := reopened.Get(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if !containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
				t.Fatal("source retired before durable marker and Catalog transition")
			}
		})
	}
}

func TestCompactMarkerRemovalFailureRecovers(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	root := store.root
	injected := errors.New("injected marker removal failure")
	store.maintenanceHook = func(got maintstate.FaultPoint) error {
		if got == maintstate.FaultBeforeMarkerRemove {
			return injected
		}
		return nil
	}
	if _, err := store.CompactSegment(context.Background(), source); !errors.Is(err, base.ErrRecoveryRequired) || !errors.Is(err, injected) {
		t.Fatalf("compact err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("retired source returned to Catalog")
	}
	if record, err := reopened.Get(context.Background(), id); err != nil || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if _, err := os.Stat(maintstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker remains: %v", err)
	}
}

func TestOpenRetriesEveryRetiredSegmentCleanupBoundary(t *testing.T) {
	points := []recordlog.FaultPoint{
		recordlog.FaultBeforeTrashRootSync,
		recordlog.FaultBeforeRetireRename,
		recordlog.FaultBeforeRecordsDirSync,
		recordlog.FaultBeforeTrashDirSync,
		recordlog.FaultBeforeTrashRemove,
		recordlog.FaultBeforeTrashFinalSync,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			root, source, id := prepareCatalogRemovedRetirement(t)
			injected := errors.New("injected physical cleanup failure")
			if _, err := open(context.Background(), root, relocationConfig().Runtime, openFaultHooks{recordLog: func(got recordlog.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}}); !errors.Is(err, injected) {
				t.Fatalf("faulted open err=%v", err)
			}
			reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
				t.Fatal("retired source returned to Catalog")
			}
			if record, err := reopened.Get(context.Background(), id); err != nil || string(record.Value) != "source-value" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
		})
	}
}

func TestOpenRetriesMaintenanceMarkerRemoval(t *testing.T) {
	root, source, id := prepareCatalogRemovedRetirement(t)
	injected := errors.New("injected marker removal failure")
	if _, err := open(context.Background(), root, relocationConfig().Runtime, openFaultHooks{maintenance: func(got maintstate.FaultPoint) error {
		if got == maintstate.FaultBeforeMarkerRemove {
			return injected
		}
		return nil
	}}); !errors.Is(err, injected) {
		t.Fatalf("faulted open err=%v", err)
	}
	reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source) {
		t.Fatal("retired source returned to Catalog")
	}
	if record, err := reopened.Get(context.Background(), id); err != nil || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func prepareCatalogRemovedRetirement(t *testing.T) (string, recordlog.SegmentID, model.ID) {
	t.Helper()
	store, source, id, _, _ := relocationFixture(t)
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintstate.Install(store.root, retirementState(store, proof)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.catalog.RemoveRecordLogSegment(proof.CatalogGeneration, proof.Source); err != nil {
		t.Fatal(err)
	}
	root := store.root
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root, source, id
}

func containsSegmentID(segments []recordlog.SegmentSummary, id recordlog.SegmentID) bool {
	for _, segment := range segments {
		if segment.SegmentID == id {
			return true
		}
	}
	return false
}
