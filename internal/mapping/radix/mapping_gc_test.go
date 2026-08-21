package radix

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func TestMappingGCWaitsForOldRootReaderBeforeDeletingFiles(t *testing.T) {
	dir, manifest := radixFixture(t)
	catalogManager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := Open(dir, manifest, 4096, catalogManager)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	addr, _ := base.NewVAddr(1, 4096)
	if _, err := mapping.Apply(1, api.ApplyUserCommit, []api.Change{{RecordID: 1, NewAddr: addr}}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := mapping.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	root, err := mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := catalogManager.Install(0, func(next *storeformat.Manifest) error {
		next.MappingRoot = root
		next.CoveredCommitSeq = 1
		next.StatsCoveredCommitSeq = 1
		next.NextCommitSeq = 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	oldID := installed.ActiveMapSegmentID
	oldPath := filepath.Join(dir, "mapping", activeMapFileName(oldID))
	mapping.readerMu.Lock()
	mapping.readers[root]++
	mapping.readerMu.Unlock()
	result := make(chan error, 1)
	go func() {
		_, err := mapping.Compact(context.Background())
		result <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for catalogManager.Snapshot().MaintenanceGeneration == installed.MaintenanceGeneration {
		if time.Now().After(deadline) {
			t.Fatal("mapping GC did not install new manifest")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old file disappeared while root reader was pinned: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("mapping GC completed while root reader was pinned: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	mapping.releaseRoot(root)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old mapping file remained: %v", err)
	}
}

func TestMappingGCCompactsEmptyRoot(t *testing.T) {
	dir, manifest := radixFixture(t)
	catalogManager, err := catalog.New(dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := Open(dir, manifest, 4096, catalogManager)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	installed, err := mapping.Compact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if installed.MappingRoot != 0 || installed.ActiveMapSegmentID == manifest.ActiveMapSegmentID || len(installed.SealedMappingSegments) != 0 {
		t.Fatalf("manifest=%+v", installed)
	}
	if _, ok, err := mapping.Lookup(1); err != nil || ok {
		t.Fatalf("ok=%v error=%v", ok, err)
	}
}
