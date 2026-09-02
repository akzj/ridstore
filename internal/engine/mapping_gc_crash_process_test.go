package engine

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const (
	mappingGCCrashHelperEnv = "RIDSTORE_MAPPING_GC_CRASH_HELPER"
	mappingGCCrashRootEnv   = "RIDSTORE_MAPPING_GC_CRASH_ROOT"
	mappingGCCrashPhaseEnv  = "RIDSTORE_MAPPING_GC_CRASH_PHASE"
)

func TestMappingGCRecoveryAcrossProcessExit(t *testing.T) {
	for _, phase := range []string{"staging", "marker", "catalog", "trash", "deleted"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(t.TempDir(), "store")
			config := testCreateConfig()
			store, err := Create(ctx, root, config)
			if err != nil {
				t.Fatal(err)
			}
			id := createMappingGCRecord(t, ctx, store, "crash-safe")
			if err := store.Checkpoint(ctx); err != nil {
				t.Fatal(err)
			}
			before := store.core.catalog.Snapshot()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestMappingGCCrashHelper$")
			command.Env = append(os.Environ(),
				mappingGCCrashHelperEnv+"=1",
				mappingGCCrashRootEnv+"="+root,
				mappingGCCrashPhaseEnv+"="+phase,
			)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}

			reopened, err := Open(ctx, root, config.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			record, err := reopened.Get(ctx, id)
			if err != nil || string(record.Value) != "crash-safe" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			after := reopened.core.catalog.Snapshot()
			if (phase == "staging" || phase == "marker") && after.ActiveMapSegmentID != before.ActiveMapSegmentID {
				t.Fatalf("unpublished generation became visible: before=%d after=%d", before.ActiveMapSegmentID, after.ActiveMapSegmentID)
			}
			if phase != "staging" && phase != "marker" && after.ActiveMapSegmentID < before.NextMapSegmentID {
				t.Fatalf("published generation was lost: before next=%d after=%d", before.NextMapSegmentID, after.ActiveMapSegmentID)
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

func TestMappingGCCrashHelper(t *testing.T) {
	if os.Getenv(mappingGCCrashHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	phase := os.Getenv(mappingGCCrashPhaseEnv)
	manifestSyncs := 0
	hooks := openFaultHooks{
		mapStore: func(point mapstore.FaultPoint) error {
			if phase == "staging" && point == mapstore.FaultBeforeAppendWrite {
				os.Exit(0)
			}
			if phase == "marker" && point == mapstore.FaultBeforeGCPromoteRename {
				os.Exit(0)
			}
			if phase == "trash" && point == mapstore.FaultBeforeGCTrashRemove {
				os.Exit(0)
			}
			return nil
		},
		mapGC: func(point mapgcstate.FaultPoint) error {
			if phase == "deleted" && point == mapgcstate.FaultBeforeMarkerRemove {
				os.Exit(0)
			}
			return nil
		},
		catalog: func(point storecatalog.FaultPoint) error {
			if point == storecatalog.FaultBeforeManifestDirSync {
				manifestSyncs++
				if phase == "catalog" && manifestSyncs == 2 {
					os.Exit(0)
				}
			}
			return nil
		},
	}
	store, err := open(context.Background(), os.Getenv(mappingGCCrashRootEnv), testCreateConfig().Runtime, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompactMapping(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("phase %q did not stop the process", phase)
}
