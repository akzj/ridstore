package catalog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
)

func TestInstallRejectsStaleGenerationBeforeMutation(t *testing.T) {
	manager := &Manager{root: t.TempDir(), current: storeformat.Manifest{Generation: 2}}
	called := false
	_, err := manager.Install(1, func(*storeformat.Manifest) error { called = true; return nil })
	if !errors.Is(err, base.ErrConflict) || called {
		t.Fatalf("error=%v called=%v", err, called)
	}
}

func TestManagerPassesHookToManifestInstaller(t *testing.T) {
	stop := errors.New("stop")
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, manifest.ManifestDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	replay, _ := base.NewLogPos(1, storeformat.SegmentHeaderSize)
	current := storeformat.Manifest{
		Generation: 1, StoreUUID: base.StoreUUID{1},
		HardLimits: storeformat.HardLimits{
			SegmentSize: 16 << 10, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 16,
			IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		NextDataSegmentID: 2, NextMapSegmentID: 2, ActiveDataSegmentID: 1, ActiveMapSegmentID: 1,
		ReplayStart: replay, ReservedIDHighExclusive: 1, ReservedBatchIDHighExclusive: 1,
		IssuedBatchIDHighExclusiveAtCut: 1, NextFrameSeq: 1, NextCommitSeq: 1,
	}
	if err := (manifest.Installer{Dir: root}).Install(current); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{
		root: root, current: current,
		hook: failpoint.Func(func(point failpoint.Point) error {
			if point == "manifest.manifest-written" {
				return stop
			}
			return nil
		}),
	}
	called := false
	_, err := manager.Install(1, func(next *storeformat.Manifest) error {
		called = true
		return nil
	})
	if !called || !errors.Is(err, stop) {
		t.Fatalf("called=%v error=%v", called, err)
	}
}
