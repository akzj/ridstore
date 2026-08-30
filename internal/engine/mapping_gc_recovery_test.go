package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestOpenRollsBackUnpublishedMappingGeneration(t *testing.T) {
	root, config, state := prepareMappingGCRecovery(t, false)
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertMappingGCRecovered(t, root, state, false)
}

func TestOpenFinishesPublishedMappingGeneration(t *testing.T) {
	root, config, state := prepareMappingGCRecovery(t, true)
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertMappingGCRecovered(t, root, state, true)
}

func TestMappingGCRecoveryAllowsInterveningDataRotations(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "finish"}[published], func(t *testing.T) {
			root, config, state := prepareMappingGCRecovery(t, published)
			rotateDataOutsideEngine(t, root, config)
			store, err := Open(context.Background(), root, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			assertMappingGCRecovered(t, root, state, published)
		})
	}
}

func TestMappingGCRecoveryFaultsConvergeOnRetry(t *testing.T) {
	for _, test := range []struct {
		name      string
		published bool
		point     mapstore.FaultPoint
	}{
		{name: "rollback remove", point: mapstore.FaultBeforeGCRollbackRemove},
		{name: "rollback sync", point: mapstore.FaultBeforeGCRollbackSync},
		{name: "retire rename", published: true, point: mapstore.FaultBeforeGCRetireRename},
		{name: "retire sync", published: true, point: mapstore.FaultBeforeGCRetireSync},
		{name: "trash remove", published: true, point: mapstore.FaultBeforeGCTrashRemove},
		{name: "trash sync", published: true, point: mapstore.FaultBeforeGCTrashSync},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, config, state := prepareMappingGCRecovery(t, test.published)
			injected := errors.New("injected")
			if _, err := open(context.Background(), root, config, openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
				if got == test.point {
					return injected
				}
				return nil
			}}); !errors.Is(err, injected) {
				t.Fatalf("faulted open err=%v", err)
			}
			store, err := Open(context.Background(), root, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			assertMappingGCRecovered(t, root, state, test.published)
		})
	}
}

func TestMappingGCMarkerRemovalFaultConvergesOnRetry(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "retire"}[published], func(t *testing.T) {
			root, config, state := prepareMappingGCRecovery(t, published)
			injected := errors.New("injected")
			if _, err := open(context.Background(), root, config, openFaultHooks{mapGC: func(got mapgcstate.FaultPoint) error {
				if got == mapgcstate.FaultBeforeMarkerRemove {
					return injected
				}
				return nil
			}}); !errors.Is(err, injected) {
				t.Fatalf("faulted open err=%v", err)
			}
			store, err := Open(context.Background(), root, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			assertMappingGCRecovered(t, root, state, published)
		})
	}
}

func TestMappingGCMarkerDirectorySyncFaultConvergesOnRetry(t *testing.T) {
	root, config, state := prepareMappingGCRecovery(t, true)
	injected := errors.New("injected")
	calls := 0
	if _, err := open(context.Background(), root, config, openFaultHooks{mapGC: func(got mapgcstate.FaultPoint) error {
		if got == mapgcstate.FaultBeforeCleanupDirSync {
			calls++
			if calls == 2 {
				return injected
			}
		}
		return nil
	}}); !errors.Is(err, injected) {
		t.Fatalf("faulted open err=%v calls=%d", err, calls)
	}
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	assertMappingGCRecovered(t, root, state, true)
}

func TestOpenCleansUnpublishedStagingWithoutMappingGCMarker(t *testing.T) {
	root := t.TempDir()
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(mapgcstate.StagingRoot(root), 0o700); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(context.Background(), root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(mapgcstate.StagingRoot(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging remains: %v", err)
	}
}

func TestPublishedMappingGCVerifiesNewRootBeforeRetiringOld(t *testing.T) {
	root, config, state := prepareMappingGCRecovery(t, true)
	newPath := filepath.Join(root, "mapping-v2", fmt.Sprintf("map-%010d.active", state.New.Active))
	file, err := os.OpenFile(newPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), root, config); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("open err=%v", err)
	}
	oldPath := filepath.Join(root, "mapping-v2", fmt.Sprintf("map-%010d.active", state.Old.Active))
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old generation was retired before new verification: %v", err)
	}
}

func prepareMappingGCRecovery(t *testing.T, publish bool) (string, OpenConfig, mapgcstate.State) {
	t.Helper()
	root := t.TempDir()
	config := testCreateConfig()
	store, err := Create(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := storecatalog.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	staging := mapgcstate.StagingRoot(root)
	if err := os.Mkdir(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	writer, err := mapstore.CreateGenerationWriter(staging, mapstore.StoreID(manifest.StoreUUID), uint32(manifest.HardLimits.SegmentSize), manifest.NextMapSegmentID, nil)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := writer.Finish(0, manifest.CoveredCommitSeq)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	state := mapgcstate.State{
		StoreID: [16]byte(manifest.StoreUUID), BaseGeneration: manifest.Generation,
		SegmentSize: uint32(manifest.HardLimits.SegmentSize), Covered: manifest.CoveredCommitSeq,
		Old: mapgcFileSet(manifest.SealedMapSegments, manifest.ActiveMapSegmentID, manifest.NextMapSegmentID, manifest.MappingRoot),
		New: mapgcstate.FileSet{Sealed: generation.SealedSegments, Active: generation.ActiveSegment, Next: generation.NextSegment, Root: generation.Root},
	}
	if err := mapgcstate.Install(root, state, nil); err != nil {
		t.Fatal(err)
	}
	if err := mapstore.PromoteGeneration(root, staging, generation, nil); err != nil {
		t.Fatal(err)
	}
	if publish {
		manager, err := storecatalog.OpenManager(root, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.InstallMappingRewrite(manifest, storecatalog.MappingRewrite{
			SealedSegments: mapSummaries(generation.SealedSegments), ActiveSegment: generation.ActiveSegment,
			NextSegment: generation.NextSegment, Root: generation.Root, Covered: generation.Covered,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return root, config.Runtime, state
}

func rotateDataOutsideEngine(t *testing.T, root string, config OpenConfig) {
	t.Helper()
	manager, err := storecatalog.OpenManager(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	log, err := recordlog.Open(root, config.RecordLog, manager)
	if err != nil {
		t.Fatal(err)
	}
	start := manager.Snapshot().ActiveDataSegmentID
	payload := recordcodec.EncodeCheckpoint(recordcodec.CheckpointMarker{CoveredCommitSeq: manager.Snapshot().CoveredCommitSeq})
	for manager.Snapshot().ActiveDataSegmentID == start {
		if _, err := log.Append(context.Background(), payload, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertMappingGCRecovered(t *testing.T, root string, state mapgcstate.State, published bool) {
	t.Helper()
	if found, err := mapgcstate.RecoveryArtifacts(root); err != nil || found {
		t.Fatalf("artifacts=%v err=%v", found, err)
	}
	manifest, err := storecatalog.LoadStrict(root)
	if err != nil {
		t.Fatal(err)
	}
	want := state.Old
	if published {
		want = state.New
	}
	if !manifestMatchesMappingSet(manifest, want) {
		t.Fatalf("manifest=%+v want=%+v", manifest, want)
	}
}

func mapgcFileSet(sealed []storecatalog.MapSegmentSummary, active, next model.MapSegmentID, root model.MapAddr) mapgcstate.FileSet {
	refs := make([]mapstore.SegmentRef, len(sealed))
	for index, summary := range sealed {
		refs[index] = mapstore.SegmentRef{SegmentID: summary.SegmentID, ValidEnd: summary.ValidEnd}
	}
	return mapgcstate.FileSet{Sealed: refs, Active: active, Next: next, Root: root}
}

func mapSummaries(refs []mapstore.SegmentRef) []storecatalog.MapSegmentSummary {
	result := make([]storecatalog.MapSegmentSummary, len(refs))
	for index, ref := range refs {
		result[index] = storecatalog.MapSegmentSummary{SegmentID: ref.SegmentID, ValidEnd: ref.ValidEnd}
	}
	return result
}
