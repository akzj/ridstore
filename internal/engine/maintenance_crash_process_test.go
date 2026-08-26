package engine

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	maintenanceCrashHelperEnv = "RIDSTORE_MAINTENANCE_CRASH_HELPER"
	maintenanceCrashRootEnv   = "RIDSTORE_MAINTENANCE_CRASH_ROOT"
	maintenanceCrashSourceEnv = "RIDSTORE_MAINTENANCE_CRASH_SOURCE"
	maintenanceCrashPhaseEnv  = "RIDSTORE_MAINTENANCE_CRASH_PHASE"
)

func TestMaintenanceRecoveryAcrossProcessExit(t *testing.T) {
	for _, phase := range []string{"marker", "catalog", "trash", "deleted"} {
		t.Run(phase, func(t *testing.T) {
			store, source, id, _, _ := relocationFixture(t)
			root := store.root
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestMaintenanceCrashHelper$")
			command.Env = append(os.Environ(),
				maintenanceCrashHelperEnv+"=1",
				maintenanceCrashRootEnv+"="+root,
				maintenanceCrashSourceEnv+"="+strconv.FormatUint(uint64(source), 10),
				maintenanceCrashPhaseEnv+"="+phase,
			)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}

			reopened, err := Open(context.Background(), root, relocationConfig().Runtime)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			present := containsSegmentID(reopened.catalog.Snapshot().SealedDataSegments, source)
			if present != (phase == "marker") {
				t.Fatalf("source present=%v phase=%s", present, phase)
			}
			if record, err := reopened.Get(context.Background(), id); err != nil || string(record.Value) != "source-value" {
				t.Fatalf("record=%+v err=%v", record, err)
			}
			if _, err := os.Stat(maintstate.Path(root)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("marker remains: %v", err)
			}
		})
	}
}

func TestMaintenanceCrashHelper(t *testing.T) {
	if os.Getenv(maintenanceCrashHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	root := os.Getenv(maintenanceCrashRootEnv)
	rawSource, err := strconv.ParseUint(os.Getenv(maintenanceCrashSourceEnv), 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(context.Background(), root, relocationConfig().Runtime)
	if err != nil {
		t.Fatal(err)
	}
	proof, _, err := store.PrepareSegmentRetirement(context.Background(), recordlog.SegmentID(rawSource))
	if err != nil {
		t.Fatal(err)
	}
	if err := maintstate.Install(root, retirementState(store, proof)); err != nil {
		t.Fatal(err)
	}
	phase := os.Getenv(maintenanceCrashPhaseEnv)
	if phase == "marker" {
		os.Exit(0)
	}
	manifest, err := store.catalog.RemoveRecordLogSegment(proof.CatalogGeneration, proof.Source)
	if err != nil {
		t.Fatal(err)
	}
	if phase == "catalog" {
		os.Exit(0)
	}
	if phase == "trash" {
		stop := errors.New("stop before trash unlink")
		err := recordlog.CleanupRetiredSegmentWithFaultHook(root, proof.Source.SegmentID, manifest.Generation, func(point recordlog.FaultPoint) error {
			if point == recordlog.FaultBeforeTrashRemove {
				return stop
			}
			return nil
		})
		if !errors.Is(err, stop) {
			t.Fatalf("cleanup err=%v", err)
		}
		os.Exit(0)
	}
	if phase != "deleted" {
		t.Fatalf("unknown phase %q", phase)
	}
	if err := recordlog.CleanupRetiredSegment(root, proof.Source.SegmentID, manifest.Generation); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
