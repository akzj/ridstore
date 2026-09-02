package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/storecatalog"
)

func TestCompactMappingSwitchesRuntimeAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[uint64]string)
	for index := 0; index < 40; index++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		value := string(rune('a' + index%26))
		id, err := batch.Create(ctx, []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		want[uint64(id)] = value
	}
	before := store.catalog.Snapshot()
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	after := store.catalog.Snapshot()
	if after.Generation <= before.Generation || after.CoveredCommitSeq != 40 ||
		after.ActiveMapSegmentID < before.NextMapSegmentID || after.MappingRoot == before.MappingRoot {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if _, err := os.Stat(mapgcstate.StagingRoot(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging remains: %v", err)
	}
	if _, found, err := mapgcstate.Load(root); err != nil || found {
		t.Fatalf("marker found=%v err=%v", found, err)
	}
	oldActive := filepath.Join(root, "mapping-v2", fmt.Sprintf("map-%010d.active", before.ActiveMapSegmentID))
	if _, err := os.Stat(oldActive); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old active mapping remains: %v", err)
	}
	for raw, value := range want {
		record, err := store.Get(ctx, model.ID(raw))
		if err != nil || string(record.Value) != value {
			t.Fatalf("id=%d value=%q err=%v", raw, record.Value, err)
		}
	}
	// A second rewrite proves the new physical owner is writable and that
	// generation IDs continue from the first rewrite rather than being reused.
	firstRewriteActive := after.ActiveMapSegmentID
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	afterSecond := store.catalog.Snapshot()
	if afterSecond.ActiveMapSegmentID <= firstRewriteActive {
		t.Fatalf("mapping segment ID was not advanced: first=%d second=%d", firstRewriteActive, afterSecond.ActiveMapSegmentID)
	}
	metrics := store.Metrics()
	if metrics.MappingGCStarted != 2 || metrics.MappingGCCompleted != 2 || metrics.MappingGCFailed != 0 ||
		metrics.MappingGCDurationNanos == 0 || metrics.MappingGCMaxDurationNanos == 0 ||
		metrics.MappingGCRebuildNanos == 0 || metrics.MappingGCVerifyNanos == 0 ||
		metrics.MappingGCPublishNanos == 0 || metrics.MappingGCMaxPublishNanos == 0 {
		t.Fatalf("mapping gc metrics=%+v", metrics)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for raw, value := range want {
		record, err := reopened.Get(ctx, model.ID(raw))
		if err != nil || string(record.Value) != value {
			t.Fatalf("reopen id=%d value=%q err=%v", raw, record.Value, err)
		}
	}
	manifest, err := storecatalog.LoadStrict(root)
	if err != nil || manifest.Generation != afterSecond.Generation {
		t.Fatalf("strict manifest generation=%d err=%v", manifest.Generation, err)
	}
}

func TestCompactMappingSpaceAdmissionRejectsBeforeStaging(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := createMappingGCRecord(t, ctx, store, "space-safe")
	store.space = newSpaceGate(root, 1, time.Second, func(string) (uint64, error) { return 1, nil })
	store.gcMinFreeBytes = 1

	if err := store.CompactMapping(ctx); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("compact err=%v", err)
	}
	if _, err := os.Lstat(mapgcstate.StagingRoot(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging exists after rejected admission: %v", err)
	}
	if artifacts, err := mapgcstate.RecoveryArtifacts(root); err != nil || artifacts {
		t.Fatalf("recovery artifacts=%v err=%v", artifacts, err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "space-safe" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactMappingAdmissionCountsLiveRecordInActiveSegment(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.HardLimits.SegmentSize = 8192
	config.HardLimits.MaxRecordLogPayload = 4096
	config.Runtime.Commit.MaxGroupPayload = 4096
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	id := createMappingGCRecord(t, ctx, store, "active-only")

	// One live Mapping entry requires eight worst-case dense radix nodes. At
	// this segment size only one dense node fits per segment. The complete
	// checkpoint Stats includes the active Data Segment, while MappingEntryCount
	// remains the admission authority for the Mapping rewrite.
	minimum := uint64(1)
	free := uint64(config.HardLimits.SegmentSize) + minimum
	store.space = newSpaceGate(root, 1, time.Second, func(string) (uint64, error) { return free, nil })
	store.gcMinFreeBytes = minimum
	if err := store.CompactMapping(ctx); !errors.Is(err, base.ErrInsufficientSpace) {
		t.Fatalf("compact err=%v", err)
	}
	manifest := store.catalog.Snapshot()
	if manifest.MappingEntryCount != 1 || len(manifest.SegmentStats) != 1 ||
		manifest.SegmentStats[0].SegmentID != manifest.ActiveDataSegmentID || manifest.SegmentStats[0].LiveRecords != 1 {
		t.Fatalf("entries=%d stats=%+v", manifest.MappingEntryCount, manifest.SegmentStats)
	}
	if _, err := os.Lstat(mapgcstate.StagingRoot(root)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging exists after rejected admission: %v", err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "active-only" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactMappingPromotionFailureRollsBackAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	created, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	id := createMappingGCRecord(t, ctx, created, "still live")
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("promote failed")
	store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if point == mapstore.FaultBeforeGCPromoteRename {
			return injected
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompactMapping(ctx); !errors.Is(err, injected) {
		t.Fatalf("compact err=%v", err)
	}
	if _, err := store.Get(ctx, id); !errors.Is(err, base.ErrReadOnly) {
		t.Fatalf("faulted get err=%v", err)
	}
	if _, found, err := mapgcstate.Load(root); err != nil || found {
		t.Fatalf("marker found=%v err=%v", found, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Get(ctx, id)
	if err != nil || string(record.Value) != "still live" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactMappingPostPublishFailureRecoversOnOpen(t *testing.T) {
	for _, test := range []struct {
		name  string
		hooks openFaultHooks
	}{
		{name: "retire", hooks: openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
			if point == mapstore.FaultBeforeGCRetireRename {
				return errors.New("retire failed")
			}
			return nil
		}}},
		{name: "marker remove", hooks: openFaultHooks{mapGC: func(point mapgcstate.FaultPoint) error {
			if point == mapgcstate.FaultBeforeMarkerRemove {
				return errors.New("marker remove failed")
			}
			return nil
		}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(t.TempDir(), "store")
			config := testCreateConfig()
			created, err := Create(ctx, root, config)
			if err != nil {
				t.Fatal(err)
			}
			id := createMappingGCRecord(t, ctx, created, "recover me")
			if err := created.Close(); err != nil {
				t.Fatal(err)
			}
			store, err := open(ctx, root, config.Runtime, test.hooks)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CompactMapping(ctx); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("compact err=%v", err)
			}
			if _, err := store.Get(ctx, id); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("faulted get err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || string(record.Value) != "recover me" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || found {
				t.Fatalf("marker found=%v err=%v", found, err)
			}
		})
	}
}

func TestCompactMappingDoesNotBlockDataOperationsDuringGenerationPromotion(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	created, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	id := createMappingGCRecord(t, ctx, created, "stable")
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if point == mapstore.FaultBeforeGCPromoteRename {
			once.Do(func() { close(entered) })
			<-release
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	compactDone := make(chan error, 1)
	go func() { compactDone <- store.CompactMapping(ctx) }()
	<-entered
	getDone := make(chan error, 1)
	go func() {
		record, err := store.Get(ctx, id)
		if err == nil && string(record.Value) != "stable" {
			err = errors.New("unexpected value")
		}
		getDone <- err
	}()
	select {
	case err := <-getDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Get blocked behind Mapping generation promotion")
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("updated")); err != nil {
		t.Fatal(err)
	}
	commitDone := make(chan error, 1)
	go func() {
		_, err := batch.Commit(ctx)
		commitDone <- err
	}()
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Commit blocked behind Mapping generation promotion")
	}
	close(release)
	if err := <-compactDone; err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "updated" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactMappingAllowsCommitDuringRebuildAndPreservesDelta(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	created, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	id := createMappingGCRecord(t, ctx, created, "before")
	if err := created.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if point == mapstore.FaultBeforeAppendWrite {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	compactDone := make(chan error, 1)
	go func() { compactDone <- store.CompactMapping(ctx) }()
	<-entered

	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("during")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-compactDone; err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "during" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, root, config.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err = reopened.Get(ctx, id)
	if err != nil || string(record.Value) != "during" {
		t.Fatalf("reopened record=%+v err=%v", record, err)
	}
}

func TestCompactMappingAllowsDataRotationDuringRebuild(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := relocationConfig()
	created, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	createMappingGCRecord(t, ctx, created, "before")
	if err := created.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if point == mapstore.FaultBeforeAppendWrite {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	compactDone := make(chan error, 1)
	go func() { compactDone <- store.CompactMapping(ctx) }()
	<-entered

	active := store.catalogSnapshot().ActiveDataSegmentID
	for store.catalogSnapshot().ActiveDataSegmentID == active {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Create(ctx, bytes.Repeat([]byte{'x'}, 512)); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := <-compactDone; err != nil {
		t.Fatal(err)
	}
	assertPublishedStateMatchesCatalog(t, store)
}

func TestCompactMappingAllowsCheckpointDuringRebuild(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.Runtime.CheckpointSortBytes = 24
	config.Runtime.DeltaSoftLimitBytes = 32
	config.Runtime.DeltaHardLimitBytes = 64
	created, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	id := createMappingGCRecord(t, ctx, created, "before")
	if err := created.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}

	enteredRebuild := make(chan struct{})
	releaseRebuild := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRebuild) }) }
	checkpointWrite := make(chan struct{})
	var appendWrites atomic.Uint64
	store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
		if point != mapstore.FaultBeforeAppendWrite {
			return nil
		}
		switch appendWrites.Add(1) {
		case 1:
			close(enteredRebuild)
			<-releaseRebuild
		case 2:
			close(checkpointWrite)
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer release()
	compactDone := make(chan error, 1)
	go func() { compactDone <- store.CompactMapping(ctx) }()
	select {
	case <-enteredRebuild:
	case <-time.After(time.Second):
		t.Fatal("Mapping GC rebuild did not start")
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Put(ctx, id, []byte("during")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	pressure, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pressure.Create(ctx, []byte("forces checkpoint")); err != nil {
		t.Fatal(err)
	}
	commitDone := make(chan error, 1)
	go func() {
		_, err := pressure.Commit(ctx)
		commitDone <- err
	}()
	select {
	case <-checkpointWrite:
		// Reaching Mapping append proves the hard-pressure Checkpoint is not
		// blocked by the Mapping GC rebuild.
	case <-time.After(time.Second):
		t.Fatal("hard-pressure Checkpoint blocked behind Mapping GC rebuild")
	}
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("hard-pressure Commit did not resume after Checkpoint")
	}
	release()
	select {
	case err := <-compactDone:
		if !errors.Is(err, base.ErrConflict) {
			t.Fatalf("stale mapping gc err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale Mapping GC did not finish")
	}
	store.state.mu.Lock()
	fault := store.operationFaultLocked()
	store.state.mu.Unlock()
	if fault != nil {
		t.Fatalf("optimistic conflict faulted store: %v", fault)
	}
	metrics := store.Metrics()
	if metrics.MappingGCStarted != 1 || metrics.MappingGCCompleted != 0 || metrics.MappingGCFailed != 0 || metrics.MappingGCConflicts != 1 {
		t.Fatalf("conflict metrics=%+v", metrics)
	}
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, id)
	if err != nil || string(record.Value) != "during" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
}

func TestCompactMappingRuntimeFaultMatrixConvergesOnFreshOpen(t *testing.T) {
	type faultCase struct {
		name  string
		hooks func(error) openFaultHooks
	}
	var cases []faultCase
	for _, point := range []mapstore.FaultPoint{
		mapstore.FaultBeforeHeaderWrite,
		mapstore.FaultBeforeHeaderSync,
		mapstore.FaultBeforeCreateRename,
		mapstore.FaultBeforeCreateDirSync,
		mapstore.FaultBeforeAppendWrite,
	} {
		point := point
		cases = append(cases, faultCase{name: string(point), hooks: func(injected error) openFaultHooks {
			return openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}}
		}})
	}
	// The ordinary zero-delta checkpoint sync is the first call; the isolated
	// generation's final sync is the second one.
	cases = append(cases, faultCase{name: "generation " + string(mapstore.FaultBeforeSync), hooks: func(injected error) openFaultHooks {
		calls := 0
		return openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
			if got == mapstore.FaultBeforeSync {
				calls++
				if calls == 2 {
					return injected
				}
			}
			return nil
		}}
	}})
	for _, point := range []mapgcstate.FaultPoint{
		mapgcstate.FaultBeforeTempCreate,
		mapgcstate.FaultBeforeWrite,
		mapgcstate.FaultBeforeFileSync,
		mapgcstate.FaultBeforeFileClose,
		mapgcstate.FaultBeforePublishRename,
		mapgcstate.FaultBeforeJournalDirSync,
		mapgcstate.FaultBeforeMarkerRemove,
		mapgcstate.FaultBeforeCleanupDirSync,
	} {
		point := point
		cases = append(cases, faultCase{name: string(point), hooks: func(injected error) openFaultHooks {
			return openFaultHooks{mapGC: func(got mapgcstate.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}}
		}})
	}
	for _, point := range []mapstore.FaultPoint{
		mapstore.FaultBeforeGCPromoteRename,
		mapstore.FaultBeforeGCPromoteSync,
		mapstore.FaultBeforeGCRetireRename,
		mapstore.FaultBeforeGCRetireSync,
		mapstore.FaultBeforeGCTrashRemove,
		mapstore.FaultBeforeGCTrashSync,
	} {
		point := point
		cases = append(cases, faultCase{name: string(point), hooks: func(injected error) openFaultHooks {
			return openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}}
		}})
	}
	for _, point := range []storecatalog.FaultPoint{
		storecatalog.FaultBeforeManifestWrite,
		storecatalog.FaultBeforeManifestSync,
		storecatalog.FaultBeforeManifestRename,
		storecatalog.FaultBeforeManifestDirSync,
	} {
		point := point
		cases = append(cases, faultCase{name: "rewrite " + string(point), hooks: func(injected error) openFaultHooks {
			calls := 0
			return openFaultHooks{catalog: func(got storecatalog.FaultPoint) error {
				if got == point {
					calls++
					if calls == 2 {
						return injected
					}
				}
				return nil
			}}
		}})
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(t.TempDir(), "store")
			config := testCreateConfig()
			created, err := Create(ctx, root, config)
			if err != nil {
				t.Fatal(err)
			}
			id := createMappingGCRecord(t, ctx, created, "fault-safe")
			// Leave no Delta so generic append/sync fault points target the
			// isolated generation instead of the prerequisite checkpoint.
			if err := created.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			if err := created.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected mapping gc fault")
			store, err := open(ctx, root, config.Runtime, test.hooks(injected))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CompactMapping(ctx); !errors.Is(err, injected) {
				t.Fatalf("compact err=%v", err)
			}
			if _, err := store.Get(ctx, id); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("faulted get err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || string(record.Value) != "fault-safe" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || found {
				t.Fatalf("marker found=%v err=%v", found, err)
			}
			if _, err := os.Stat(mapgcstate.StagingRoot(root)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("staging remains: %v", err)
			}
			if _, err := storecatalog.LoadStrict(root); err != nil {
				t.Fatalf("strict catalog: %v", err)
			}
		})
	}
}

func TestCompactMappingRollbackFailureRetainsRecoveryMarker(t *testing.T) {
	for _, rollbackPoint := range []mapstore.FaultPoint{
		mapstore.FaultBeforeGCRollbackRemove,
		mapstore.FaultBeforeGCRollbackSync,
	} {
		t.Run(string(rollbackPoint), func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(t.TempDir(), "store")
			config := testCreateConfig()
			created, err := Create(ctx, root, config)
			if err != nil {
				t.Fatal(err)
			}
			id := createMappingGCRecord(t, ctx, created, "rollback-safe")
			if err := created.Close(); err != nil {
				t.Fatal(err)
			}
			promoteErr := errors.New("promote sync failed")
			rollbackErr := errors.New("rollback failed")
			store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
				switch point {
				case mapstore.FaultBeforeGCPromoteSync:
					return promoteErr
				case rollbackPoint:
					return rollbackErr
				default:
					return nil
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			err = store.CompactMapping(ctx)
			if !errors.Is(err, promoteErr) || !errors.Is(err, rollbackErr) || !errors.Is(err, base.ErrRecoveryRequired) {
				t.Fatalf("compact err=%v", err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || !found {
				t.Fatalf("recovery marker found=%v err=%v", found, err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || string(record.Value) != "rollback-safe" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || found {
				t.Fatalf("marker after recovery found=%v err=%v", found, err)
			}
		})
	}
}

func TestCompactMappingPartialMultiFileOperationsRecover(t *testing.T) {
	for _, phase := range []string{"promote", "retire"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			root, config, id := createMultiFileMappingFixture(t, ctx)
			injected := errors.New("second file failed")
			calls := 0
			store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(point mapstore.FaultPoint) error {
				target := mapstore.FaultBeforeGCPromoteRename
				if phase == "retire" {
					target = mapstore.FaultBeforeGCRetireRename
				}
				if point == target {
					calls++
					if calls == 2 {
						return injected
					}
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CompactMapping(ctx); !errors.Is(err, injected) || calls < 2 {
				t.Fatalf("compact err=%v calls=%d", err, calls)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || len(record.Value) != 1 {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || found {
				t.Fatalf("marker found=%v err=%v", found, err)
			}
		})
	}
}

func TestCompactMappingGenerationRotationFaultsRecover(t *testing.T) {
	for _, point := range []mapstore.FaultPoint{
		mapstore.FaultBeforeFooterWrite,
		mapstore.FaultBeforeFooterSync,
		mapstore.FaultBeforeSealRename,
		mapstore.FaultBeforeSealDirSync,
	} {
		t.Run(string(point), func(t *testing.T) {
			ctx := context.Background()
			root, config, id := createMultiFileMappingFixture(t, ctx)
			injected := errors.New("generation rotation failed")
			store, err := open(ctx, root, config.Runtime, openFaultHooks{mapStore: func(got mapstore.FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.CompactMapping(ctx); !errors.Is(err, injected) {
				t.Fatalf("compact err=%v", err)
			}
			if _, err := store.Get(ctx, id); !errors.Is(err, base.ErrReadOnly) {
				t.Fatalf("faulted get err=%v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || len(record.Value) != 1 {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, found, err := mapgcstate.Load(root); err != nil || found {
				t.Fatalf("marker found=%v err=%v", found, err)
			}
		})
	}
}

func createMultiFileMappingFixture(t *testing.T, ctx context.Context) (string, CreateConfig, model.ID) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	config.HardLimits.SegmentSize = 8192
	config.HardLimits.MaxValueSize = 128
	config.HardLimits.MaxBatchBytes = 4096
	config.HardLimits.MaxBatchMutations = 128
	config.HardLimits.MaxBatchConditions = 128
	config.HardLimits.MaxRecordLogPayload = 7000
	config.Runtime.Commit.MaxGroupPayload = 7000
	config.Runtime.CheckpointSortBytes = 96 << 10
	config.Runtime.DeltaSoftLimitBytes = 128 << 10
	config.Runtime.DeltaHardLimitBytes = 256 << 10
	config.Runtime.StatusRetention = 4096
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	var first model.ID
	for batchIndex := 0; batchIndex < 16; batchIndex++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 128; index++ {
			id, err := batch.Create(ctx, []byte{byte(batchIndex)})
			if err != nil {
				t.Fatal(err)
			}
			if first == 0 {
				first = id
			}
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	manifest := store.catalog.Snapshot()
	if len(manifest.SealedMapSegments) == 0 {
		t.Fatal("fixture did not create a multi-file Mapping generation")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root, config, first
}

func TestCompactMappingReclaimsUnreachableCheckpointNodes(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	config := testCreateConfig()
	store, err := Create(ctx, root, config)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]model.ID, 0, 128)
	for group := 0; group < 8; group++ {
		batch, err := store.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index < 16; index++ {
			id, err := batch.Create(ctx, []byte{0})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if _, err := batch.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	for round := byte(1); round <= 5; round++ {
		for start := 0; start < len(ids); start += 16 {
			batch, err := store.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ids[start : start+16] {
				if err := batch.Put(ctx, id, []byte{round}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := batch.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.Checkpoint(ctx); err != nil {
			t.Fatal(err)
		}
	}
	before := mappingPhysicalBytes(t, root)
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	after := mappingPhysicalBytes(t, root)
	if after >= before {
		t.Fatalf("Mapping GC did not reclaim space: before=%d after=%d", before, after)
	}
	for _, id := range ids {
		record, err := store.Get(ctx, id)
		if err != nil || len(record.Value) != 1 || record.Value[0] != 5 {
			t.Fatalf("id=%d record=%+v err=%v", id, record, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func mappingPhysicalBytes(t *testing.T, root string) uint64 {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "mapping-v2"))
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().IsRegular() {
			total += uint64(info.Size())
		}
	}
	return total
}

func createMappingGCRecord(t *testing.T, ctx context.Context, store *Store, value string) model.ID {
	t.Helper()
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(ctx, []byte(value))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return id
}
