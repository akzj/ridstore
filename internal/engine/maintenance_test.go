package engine

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/akzj/ridstore/internal/maintstate"
)

func TestOpenRollsBackPreparedRetirementWhenCatalogStillOwnsSource(t *testing.T) {
	store, source, _, _, _ := relocationFixture(t)
	root := store.root
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintstate.Install(root, retirementState(store, proof)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !containsSealedSegment(reopened.catalog.Snapshot(), proof.Source) {
		t.Fatal("prepared retirement removed Catalog source")
	}
	if _, err := os.Stat(maintstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("maintenance marker remains: %v", err)
	}
}

func TestOpenFinishesRetirementAfterDurableCatalogRemoval(t *testing.T) {
	store, source, id, _, _ := relocationFixture(t)
	root := store.root
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := maintstate.Install(root, retirementState(store, proof)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.catalog.RemoveRecordLogSegment(proof.CatalogGeneration, proof.Source); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if containsSealedSegment(reopened.catalog.Snapshot(), proof.Source) {
		t.Fatal("retired source returned to Catalog")
	}
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "source-value" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if _, err := os.Stat(maintstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("maintenance marker remains: %v", err)
	}
}

func TestRetirementRecoveryAllowsInterveningDataRotations(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "finish"}[published], func(t *testing.T) {
			store, source, id, _, _ := relocationFixture(t)
			root := store.root
			proof, _, err := store.PrepareSegmentRetirement(context.Background(), source)
			if err != nil {
				t.Fatal(err)
			}
			if err := maintstate.Install(root, retirementState(store, proof)); err != nil {
				t.Fatal(err)
			}
			if published {
				if _, err := store.catalog.RemoveRecordLogSegment(proof.CatalogGeneration, proof.Source); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			rotateDataOutsideEngine(t, root, relocationConfig().Runtime)

			reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if containsSealedSegment(reopened.catalog.Snapshot(), proof.Source) != !published {
				t.Fatalf("published=%v source membership mismatch", published)
			}
			record, err := reopened.Get(context.Background(), id)
			if err != nil || string(record.Value) != "source-value" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if _, err := os.Stat(maintstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("maintenance marker remains: %v", err)
			}
		})
	}
}

func retirementState(store *Store, proof SegmentRetirementProof) maintstate.State {
	manifest := store.catalog.Snapshot()
	return maintstate.State{
		Operation: maintstate.DataRetire, StoreUUID: manifest.StoreUUID, LogID: manifest.RecordLogID,
		BaseGeneration: proof.CatalogGeneration, CoveredCommitSeq: proof.CoveredCommitSeq,
		ReplayStart: proof.ReplayStart, Source: proof.Source,
	}
}
