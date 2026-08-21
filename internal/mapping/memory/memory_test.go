package memory

import (
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping/api"
)

func addr(t *testing.T, offset uint32) base.VAddr {
	t.Helper()
	a, err := base.NewVAddr(1, offset)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestUserApplyDeleteAndSnapshotIsolation(t *testing.T) {
	m := NewEmpty()
	a1, a2 := addr(t, 4096), addr(t, 8192)
	result, err := m.Apply(1, api.ApplyUserCommit, []api.Change{{RecordID: 1, NewAddr: a1}, {RecordID: 2, NewAddr: a2}})
	if err != nil || result.Applied != 2 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if _, err := m.Apply(2, api.ApplyUserCommit, []api.Change{{RecordID: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := m.Lookup(1); err != nil || ok {
		t.Fatalf("deleted lookup ok=%v error=%v", ok, err)
	}
	snapshot := m.Snapshot()
	snapshot.Entries[2] = a1
	if got, ok, _ := m.Lookup(2); !ok || got != a2 {
		t.Fatalf("mapping changed through snapshot: %x", got)
	}
}

func TestRelocationCAS(t *testing.T) {
	m := NewEmpty()
	a1, a2, a3 := addr(t, 4096), addr(t, 8192), addr(t, 12288)
	if _, err := m.Apply(1, api.ApplyUserCommit, []api.Change{{RecordID: 1, NewAddr: a1}, {RecordID: 2, NewAddr: a2}}); err != nil {
		t.Fatal(err)
	}
	result, err := m.Apply(2, api.ApplyRelocation, []api.Change{
		{RecordID: 1, NewAddr: a3, ExpectedOldAddr: a1},
		{RecordID: 2, NewAddr: a3, ExpectedOldAddr: a1},
	})
	if err != nil || result.Applied != 1 || result.Skipped != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if got, _, _ := m.Lookup(1); got != a3 {
		t.Fatalf("relocated addr=%x", got)
	}
	if got, _, _ := m.Lookup(2); got != a2 {
		t.Fatalf("CAS skip addr=%x", got)
	}
}

func TestRejectsInvalidChangesAndSequenceRegression(t *testing.T) {
	m := NewEmpty()
	a := addr(t, 4096)
	cases := []struct {
		seq     base.CommitSeq
		kind    api.ApplyKind
		changes []api.Change
	}{
		{0, api.ApplyUserCommit, nil},
		{1, 99, nil},
		{1, api.ApplyUserCommit, []api.Change{{RecordID: 0, NewAddr: a}}},
		{1, api.ApplyUserCommit, []api.Change{{RecordID: 2, NewAddr: a}, {RecordID: 1, NewAddr: a}}},
		{1, api.ApplyRelocation, []api.Change{{RecordID: 1, NewAddr: a}}},
	}
	for i, tc := range cases {
		if _, err := m.Apply(tc.seq, tc.kind, tc.changes); !errors.Is(err, base.ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
	if _, err := m.Apply(2, api.ApplyUserCommit, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Apply(2, api.ApplyUserCommit, nil); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("regression error=%v", err)
	}
}

func TestRandomModelAndConcurrentLookup(t *testing.T) {
	m := NewEmpty()
	model := map[base.ID]base.VAddr{}
	rng := rand.New(rand.NewSource(1))
	for seq := 1; seq <= 500; seq++ {
		id := base.ID(rng.Intn(32) + 1)
		change := api.Change{RecordID: id}
		if rng.Intn(4) != 0 {
			change.NewAddr = addr(t, uint32(4096+8*(seq+1)))
			model[id] = change.NewAddr
		} else {
			delete(model, id)
		}
		if _, err := m.Apply(base.CommitSeq(seq), api.ApplyUserCommit, []api.Change{change}); err != nil {
			t.Fatal(err)
		}
	}
	if got := m.Snapshot().Entries; !reflect.DeepEqual(got, model) {
		t.Fatalf("snapshot differs: got=%v want=%v", got, model)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := base.ID(1); id <= 32; id++ {
				_, _, _ = m.Lookup(id)
			}
		}()
	}
	wg.Wait()
}
