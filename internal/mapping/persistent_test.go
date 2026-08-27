package mapping

import (
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
)

type persistentCatalog struct {
	mu    sync.Mutex
	state mapstore.CatalogSnapshot
}

func (c *persistentCatalog) SnapshotMapStore() mapstore.CatalogSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.Clone()
}

func (c *persistentCatalog) InstallMapStoreRotation(expect uint64, sealed mapstore.SegmentRef, active, next model.MapSegmentID) (mapstore.CatalogSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.Generation != expect || c.state.ActiveSegment != sealed.SegmentID || c.state.NextSegment != active || next != active+1 {
		return mapstore.CatalogSnapshot{}, ErrInvalid
	}
	c.state.Generation++
	c.state.SealedSegments = append(c.state.SealedSegments, sealed)
	c.state.ActiveSegment = active
	c.state.NextSegment = next
	return c.state.Clone(), nil
}

func newPersistentForTest(t *testing.T) (*Persistent, *mapstore.Store) {
	t.Helper()
	root := t.TempDir()
	catalog := &persistentCatalog{state: mapstore.CatalogSnapshot{
		Generation: 1, StoreID: mapstore.StoreID{1}, SegmentSize: 8192, ActiveSegment: 1, NextSegment: 2,
	}}
	if err := mapstore.CreateInitialSegment(root, catalog.state.StoreID, catalog.state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	physical, err := mapstore.Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := radix.Open(physical, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	current, err := OpenPersistent(tree, physical, PersistentConfig{
		CheckpointSortBytes: 16 << 10, DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return current, physical
}

func TestPersistentCheckpointKeepsNewCommitsVisible(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	a := testAddr(t, 1, 64)
	b := testAddr(t, 1, 128)
	plan, err := current.ResolveGroup([]Proposal{
		{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 7, NewAddr: a, Operation: OperationPut}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.PublishGroup(1, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	frozen, err := current.Freeze(1)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := current.BuildCheckpoint(frozen)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = current.ResolveGroup([]Proposal{
		{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 8, NewAddr: b, Operation: OperationPut}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.PublishGroup(2, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	if err := current.InstallCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[model.ID]recordlog.VAddr{7: a, 8: b} {
		got, exists, err := current.Lookup(id)
		if err != nil || !exists || got != want {
			t.Fatalf("id=%d got=%+v exists=%v err=%v", id, got, exists, err)
		}
	}
	if candidate.Root() == 0 || candidate.CoveredCommitSeq() != 1 || current.CoveredCommitSeq() != 2 {
		t.Fatalf("candidate root=%v covered=%d runtime=%d", candidate.Root(), candidate.CoveredCommitSeq(), current.CoveredCommitSeq())
	}
}

func TestPersistentPublishGroupReservationFailureDoesNotExposePartialGroup(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	plan, err := current.ResolveGroup([]Proposal{
		{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: testAddr(t, 1, 64), Operation: OperationPut}}},
		{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 2, NewAddr: testAddr(t, 1, 128), Operation: OperationPut}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	reservations := reservePlan(t, current, plan)
	reservations[1] = failingReservation{}
	if _, err := current.PublishGroup(1, plan, reservations); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("publish err=%v", err)
	}
	for _, id := range []model.ID{1, 2} {
		if _, exists, err := current.Lookup(id); err != nil || exists {
			t.Fatalf("id=%d exists=%v err=%v", id, exists, err)
		}
	}
	if current.CoveredCommitSeq() != 0 {
		t.Fatalf("covered=%d", current.CoveredCommitSeq())
	}
}

func TestPersistentPublishGroupChargeOverflowDoesNotExposeGroup(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	plan, err := current.ResolveGroup([]Proposal{{
		Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: testAddr(t, 1, 64), Operation: OperationPut}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	current.active.charge = 1
	if _, err := current.PublishGroup(1, plan, []DeltaReservation{fixedReservation(math.MaxUint64)}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("publish err=%v", err)
	}
	if _, exists, err := current.Lookup(1); err != nil || exists {
		t.Fatalf("exists=%v err=%v", exists, err)
	}
	if current.CoveredCommitSeq() != 0 {
		t.Fatalf("covered=%d", current.CoveredCommitSeq())
	}
}

func TestPersistentRelocationFromRootUsesAddressCAS(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	oldAddr := testAddr(t, 2, 64)
	newAddr := testAddr(t, 3, 64)
	plan, _ := current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 9, NewAddr: oldAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(1, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	frozen, _ := current.Freeze(1)
	candidate, err := current.BuildCheckpoint(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.InstallCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
	plan, err = current.ResolveGroup([]Proposal{{Kind: ProposalRelocation, Changes: []Change{{RecordID: 9, ExpectedOldAddr: oldAddr, NewAddr: newAddr, Operation: OperationRelocate}}}})
	if err != nil || !plan.Proposals[0].Changes[0].Apply {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if _, err := current.PublishGroup(2, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	got, exists, err := current.Lookup(9)
	if err != nil || !exists || got != newAddr {
		t.Fatalf("got=%+v exists=%v err=%v", got, exists, err)
	}
}

func TestPersistentAbortCheckpointRetainsFrozenLayers(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	addr := testAddr(t, 1, 64)
	plan, _ := current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: addr, Operation: OperationPut}}}})
	_, _ = current.PublishGroup(1, plan, reservePlan(t, current, plan))
	first, _ := current.Freeze(1)
	if err := current.AbortCheckpoint(first); err != nil {
		t.Fatal(err)
	}
	if _, err := current.BuildCheckpoint(first); err != ErrStalePlan {
		t.Fatalf("aborted checkpoint build err=%v", err)
	}
	second, err := current.Freeze(1)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := current.BuildCheckpoint(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.InstallCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
	if got, exists, err := current.Lookup(1); err != nil || !exists || got != addr {
		t.Fatalf("got=%+v exists=%v err=%v", got, exists, err)
	}
}

func TestPersistentCheckpointRejectsTemporaryMemoryOverflow(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	current.checkpointSortBytes = checkpointMutationBytes
	a := testAddr(t, 1, 64)
	b := testAddr(t, 1, 128)
	plan, err := current.ResolveGroup([]Proposal{{
		Kind:    ProposalUserCommit,
		Changes: []Change{{RecordID: 1, NewAddr: a, Operation: OperationPut}, {RecordID: 2, NewAddr: b, Operation: OperationPut}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.PublishGroup(1, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	frozen, err := current.Freeze(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.BuildCheckpoint(frozen); err != ErrBudget {
		t.Fatalf("err=%v", err)
	}
	if got, exists, err := current.Lookup(1); err != nil || !exists || got != a {
		t.Fatalf("got=%+v exists=%v err=%v", got, exists, err)
	}
}

func TestPersistentDeltaChargeTracksLayersUntilInstall(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	firstAddr := testAddr(t, 1, 64)
	secondAddr := testAddr(t, 1, 128)

	plan, _ := current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: firstAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(1, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	if charged, reserved, _, _ := current.DeltaUsage(); charged != deltaEntryCharge || reserved != 0 {
		t.Fatalf("first publish charged=%d reserved=%d", charged, reserved)
	}

	plan, _ = current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: secondAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(2, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	if charged, reserved, _, _ := current.DeltaUsage(); charged != deltaEntryCharge || reserved != 0 {
		t.Fatalf("hot update charged=%d reserved=%d", charged, reserved)
	}

	first, err := current.Freeze(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.AbortCheckpoint(first); err != nil {
		t.Fatal(err)
	}
	if charged, _, _, _ := current.DeltaUsage(); charged != deltaEntryCharge {
		t.Fatalf("abort released charge=%d", charged)
	}

	second, err := current.Freeze(2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := current.BuildCheckpoint(second)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ = current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 2, NewAddr: firstAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(3, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	if charged, _, _, _ := current.DeltaUsage(); charged != 2*deltaEntryCharge {
		t.Fatalf("before install charged=%d", charged)
	}
	if err := current.InstallCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
	if charged, reserved, _, _ := current.DeltaUsage(); charged != deltaEntryCharge || reserved != 0 {
		t.Fatalf("install released wrong prefix charged=%d reserved=%d", charged, reserved)
	}
}

func TestPersistentCheckpointKeepsNewestMutationAcrossFrozenLayers(t *testing.T) {
	current, physical := newPersistentForTest(t)
	defer physical.Close()
	oldAddr := testAddr(t, 1, 64)
	newAddr := testAddr(t, 1, 128)
	plan, _ := current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: oldAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(1, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	first, err := current.Freeze(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.AbortCheckpoint(first); err != nil {
		t.Fatal(err)
	}
	plan, _ = current.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: newAddr, Operation: OperationPut}}}})
	if _, err := current.PublishGroup(2, plan, reservePlan(t, current, plan)); err != nil {
		t.Fatal(err)
	}
	second, err := current.Freeze(2)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := current.BuildCheckpoint(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.InstallCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
	addr, exists, err := current.Lookup(1)
	if err != nil || !exists || addr != newAddr {
		t.Fatalf("addr=%v exists=%v err=%v", addr, exists, err)
	}
}

func TestPersistentRejectsDeltaLimitLargerThanCheckpointCapacity(t *testing.T) {
	nodes := emptyNodeStoreForPersistentTest{}
	tree, err := radix.Open(nodes, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPersistent(tree, nodes, PersistentConfig{
		CheckpointSortBytes: 16, DeltaSoftLimitBytes: 64, DeltaHardLimitBytes: 128,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("open err=%v", err)
	}
}

type emptyNodeStoreForPersistentTest struct{}

func (emptyNodeStoreForPersistentTest) Read(model.MapAddr) (mapstore.Node, error) {
	return mapstore.Node{}, ErrCorrupt
}

func (emptyNodeStoreForPersistentTest) Append(uint8, uint64, model.CommitSeq, [mapstore.NodeSlots]uint64) (model.MapAddr, error) {
	return 0, ErrCorrupt
}

func (emptyNodeStoreForPersistentTest) Sync() error { return nil }
