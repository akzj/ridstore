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
	root, _, err := mapping.BuildCheckpoint(checkpoint)
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
