package radix

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func radixFixture(t *testing.T) (string, storeformat.Manifest) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	hard := storeformat.HardLimits{
		SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
		MaxBatchMutations: 64, MaxBatchConditions: 64, MaxOpenBatches: 64,
		IDReserveSize: 64, BatchIDReserveSize: 64,
	}
	manifest, err := initialize.Create(dir, hard)
	if err != nil {
		t.Fatal(err)
	}
	return dir, manifest
}

func TestRadixCheckpointLookupDeleteAndReopen(t *testing.T) {
	dir, manifest := radixFixture(t)
	mapping, err := Open(dir, manifest, 4096)
	if err != nil {
		t.Fatal(err)
	}
	ids := []base.ID{1, 512, 1 << 20, base.ID(math.MaxUint64)}
	changes := make([]api.Change, len(ids))
	for i, id := range ids {
		addr, err := base.NewVAddr(1, uint32(4096+i*8))
		if err != nil {
			t.Fatal(err)
		}
		changes[i] = api.Change{RecordID: id, NewAddr: addr}
	}
	if _, err := mapping.Apply(1, api.ApplyUserCommit, changes); err != nil {
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
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		addr, ok, err := mapping.Lookup(id)
		if err != nil || !ok || addr != changes[i].NewAddr {
			t.Fatalf("id=%d addr=%x ok=%v error=%v", id, addr, ok, err)
		}
	}
	if _, err := mapping.Apply(2, api.ApplyUserCommit, []api.Change{{RecordID: ids[0]}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := mapping.Lookup(ids[0]); err != nil || ok {
		t.Fatalf("deleted ok=%v error=%v", ok, err)
	}
	if err := mapping.Close(); err != nil {
		t.Fatal(err)
	}
	manifest.MappingRoot = root
	manifest.CoveredCommitSeq = 1
	manifest.StatsCoveredCommitSeq = 1
	manifest.NextCommitSeq = 2
	reopened, err := Open(dir, manifest, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i, id := range ids {
		addr, ok, err := reopened.Lookup(id)
		if err != nil || !ok || addr != changes[i].NewAddr {
			t.Fatalf("reopen id=%d addr=%x ok=%v error=%v", id, addr, ok, err)
		}
	}
}

func TestCheckpointRewritesOnlyDirtyRadixPath(t *testing.T) {
	dir, manifest := radixFixture(t)
	mapping, err := Open(dir, manifest, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	changes := make([]api.Change, 64)
	for i := range changes {
		changes[i].RecordID = base.ID((i + 1) << 9)
		changes[i].NewAddr, _ = base.NewVAddr(1, uint32(4096+i*8))
	}
	if _, err := mapping.Apply(1, api.ApplyUserCommit, changes); err != nil {
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
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	mapping.store.mu.RLock()
	before := mapping.store.nextNodeSeq
	mapping.store.mu.RUnlock()
	updated, _ := base.NewVAddr(2, 4096)
	if _, err := mapping.Apply(2, api.ApplyUserCommit, []api.Change{{RecordID: changes[17].RecordID, NewAddr: updated}}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err = mapping.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	root, err = mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	mapping.store.mu.RLock()
	after := mapping.store.nextNodeSeq
	mapping.store.mu.RUnlock()
	if written := after - before; written != 8 {
		t.Fatalf("incremental checkpoint wrote %d nodes, want one eight-level path", written)
	}
	for i, change := range changes {
		addr, ok, err := mapping.Lookup(change.RecordID)
		want := change.NewAddr
		if i == 17 {
			want = updated
		}
		if err != nil || !ok || addr != want {
			t.Fatalf("id=%d addr=%x want=%x ok=%v error=%v", change.RecordID, addr, want, ok, err)
		}
	}
}

func TestCheckpointPrunesEmptyPath(t *testing.T) {
	dir, manifest := radixFixture(t)
	mapping, err := Open(dir, manifest, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	addr, _ := base.NewVAddr(1, 4096)
	if _, err := mapping.Apply(1, api.ApplyUserCommit, []api.Change{{RecordID: 42, NewAddr: addr}}); err != nil {
		t.Fatal(err)
	}
	checkpoint, _ := mapping.BeginCheckpoint()
	root, err := mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.Apply(2, api.ApplyUserCommit, []api.Change{{RecordID: 42}}); err != nil {
		t.Fatal(err)
	}
	checkpoint, _ = mapping.BeginCheckpoint()
	root, err = mapping.BuildCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if root != 0 {
		t.Fatalf("root=%x", root)
	}
	if err := mapping.CompleteCheckpoint(checkpoint, root); err != nil {
		t.Fatal(err)
	}
}
