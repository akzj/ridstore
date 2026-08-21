package radix

import (
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping/api"
	"github.com/akzj/ridstore/internal/mapping/memory"
)

func TestRadixMatchesMemoryMappingRandomModel(t *testing.T) {
	dir, manifest := radixFixture(t)
	persistent, err := Open(dir, manifest, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer persistent.Close()
	oracle := memory.NewEmpty()
	rng := rand.New(rand.NewSource(0x5eed))
	ids := []base.ID{1, 2, 511, 512, 513, 1 << 20, 1 << 40, base.ID(math.MaxUint64)}
	nextOffset := uint32(base.FirstContentOffset)
	for seq := base.CommitSeq(1); seq <= 250; seq++ {
		id := ids[rng.Intn(len(ids))]
		kind := api.ApplyUserCommit
		change := api.Change{RecordID: id}
		switch rng.Intn(4) {
		case 0:
			// A tombstone is a valid user mutation whether or not the ID exists.
		case 1:
			kind = api.ApplyRelocation
			current, exists, err := oracle.Lookup(id)
			if err != nil {
				t.Fatal(err)
			}
			if exists && rng.Intn(2) == 0 {
				change.ExpectedOldAddr = current
			} else {
				change.ExpectedOldAddr, _ = base.NewVAddr(99, nextOffset)
				nextOffset += 8
			}
			change.NewAddr, _ = base.NewVAddr(2, nextOffset)
			nextOffset += 8
		default:
			change.NewAddr, _ = base.NewVAddr(1, nextOffset)
			nextOffset += 8
		}
		wantResult, wantErr := oracle.Apply(seq, kind, []api.Change{change})
		gotResult, gotErr := persistent.Apply(seq, kind, []api.Change{change})
		if (wantErr != nil) != (gotErr != nil) || wantResult != gotResult {
			t.Fatalf("seq=%d kind=%d change=%+v oracle=(%+v,%v) radix=(%+v,%v)", seq, kind, change, wantResult, wantErr, gotResult, gotErr)
		}
		if seq%17 == 0 {
			checkpoint, err := persistent.BeginCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			root, _, err := persistent.BuildCheckpoint(checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			if err := persistent.CompleteCheckpoint(checkpoint, root); err != nil {
				t.Fatal(err)
			}
		}
		want, got := oracle.Snapshot(), persistent.Snapshot()
		if want.CoveredCommitSeq != got.CoveredCommitSeq || !reflect.DeepEqual(want.Entries, got.Entries) {
			t.Fatalf("seq=%d oracle=%+v radix=%+v", seq, want, got)
		}
		for _, probe := range ids {
			wantAddr, wantOK, wantErr := oracle.Lookup(probe)
			gotAddr, gotOK, gotErr := persistent.Lookup(probe)
			if wantAddr != gotAddr || wantOK != gotOK || (wantErr != nil) != (gotErr != nil) {
				t.Fatalf("seq=%d id=%d oracle=(%x,%v,%v) radix=(%x,%v,%v)", seq, probe, wantAddr, wantOK, wantErr, gotAddr, gotOK, gotErr)
			}
		}
	}
}
