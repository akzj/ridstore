package mapping

import (
	"errors"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func testAddr(t *testing.T, segment recordlog.SegmentID, offset uint32) recordlog.VAddr {
	t.Helper()
	addr, err := recordlog.NewVAddr(segment, offset, 64)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestResolveGroupUsesVirtualStateAndPublishesAtomically(t *testing.T) {
	mapping := NewEmpty()
	firstAddr := testAddr(t, 1, 64)
	secondAddr := testAddr(t, 1, 128)
	plan, err := mapping.ResolveGroup([]Proposal{
		{
			Kind: ProposalUserCommit, Revision: 10,
			Conditions: []Condition{{RecordID: 7, Kind: ConditionAbsent}},
			Changes:    []Change{{RecordID: 7, NewAddr: firstAddr, Operation: OperationPut}},
		},
		{
			Kind: ProposalUserCommit, Revision: 11,
			Conditions: []Condition{{RecordID: 7, Kind: ConditionRevision, Revision: 10}},
			Changes:    []Change{{RecordID: 7, NewAddr: secondAddr, Operation: OperationPut}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Proposals[0].Accepted || !plan.Proposals[1].Accepted {
		t.Fatalf("plan=%+v", plan)
	}
	if _, exists, err := mapping.Lookup(7); err != nil || exists {
		t.Fatalf("mapping changed before publish: exists=%v err=%v", exists, err)
	}
	result, err := mapping.PublishGroup(1, plan)
	if err != nil || result.Committed != 2 || result.Applied != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	entry, exists, err := mapping.Lookup(7)
	if err != nil || !exists || entry.Addr != secondAddr || entry.Revision != 11 {
		t.Fatalf("entry=%+v exists=%v err=%v", entry, exists, err)
	}
	if mapping.CoveredCommitSeq() != 2 {
		t.Fatalf("covered=%d", mapping.CoveredCommitSeq())
	}
}

func TestResolveGroupRejectsConflictWithoutAffectingLaterProposal(t *testing.T) {
	old := testAddr(t, 1, 64)
	mapping, err := New(Snapshot{CoveredCommitSeq: 4, Entries: map[model.ID]Entry{1: {Addr: old, Revision: 8}}})
	if err != nil {
		t.Fatal(err)
	}
	newAddr := testAddr(t, 1, 128)
	plan, err := mapping.ResolveGroup([]Proposal{
		{Kind: ProposalUserCommit, Revision: 9, Conditions: []Condition{{RecordID: 1, Kind: ConditionRevision, Revision: 7}}, Changes: []Change{{RecordID: 1, Operation: OperationDelete}}},
		{Kind: ProposalUserCommit, Revision: 10, Conditions: []Condition{{RecordID: 1, Kind: ConditionRevision, Revision: 8}}, Changes: []Change{{RecordID: 1, NewAddr: newAddr, Operation: OperationPut}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Proposals[0].Accepted || !plan.Proposals[1].Accepted {
		t.Fatalf("plan=%+v", plan)
	}
	result, err := mapping.PublishGroup(5, plan)
	if err != nil || result.Committed != 1 || mapping.CoveredCommitSeq() != 5 {
		t.Fatalf("result=%+v covered=%d err=%v", result, mapping.CoveredCommitSeq(), err)
	}
	entry, exists, _ := mapping.Lookup(1)
	if !exists || entry.Addr != newAddr || entry.Revision != 10 {
		t.Fatalf("entry=%+v exists=%v", entry, exists)
	}
}

func TestRelocationCASPreservesLogicalRevision(t *testing.T) {
	old := testAddr(t, 2, 64)
	newAddr := testAddr(t, 3, 64)
	mapping, _ := New(Snapshot{CoveredCommitSeq: 2, Entries: map[model.ID]Entry{5: {Addr: old, Revision: 77}}})
	plan, err := mapping.ResolveGroup([]Proposal{{
		Kind: ProposalRelocation,
		Changes: []Change{
			{RecordID: 5, ExpectedOldAddr: old, NewAddr: newAddr, Operation: OperationRelocate},
			{RecordID: 6, ExpectedOldAddr: old, NewAddr: testAddr(t, 3, 128), Operation: OperationRelocate},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := mapping.PublishGroup(3, plan)
	if err != nil || result.Applied != 1 || result.Skipped != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	entry, exists, _ := mapping.Lookup(5)
	if !exists || entry.Addr != newAddr || entry.Revision != 77 {
		t.Fatalf("entry=%+v exists=%v", entry, exists)
	}
}

func TestPublishRejectsStaleOrMutatedPlan(t *testing.T) {
	mapping := NewEmpty()
	addr := testAddr(t, 1, 64)
	proposal := Proposal{Kind: ProposalUserCommit, Revision: 1, Changes: []Change{{RecordID: 1, NewAddr: addr, Operation: OperationPut}}}
	first, err := mapping.ResolveGroup([]Proposal{proposal})
	if err != nil {
		t.Fatal(err)
	}
	second, err := mapping.ResolveGroup([]Proposal{proposal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.PublishGroup(1, first); err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.PublishGroup(2, second); !errors.Is(err, ErrStalePlan) {
		t.Fatalf("stale error=%v", err)
	}
	third, err := mapping.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Revision: 2, Changes: []Change{{RecordID: 2, NewAddr: testAddr(t, 1, 128), Operation: OperationPut}}}})
	if err != nil {
		t.Fatal(err)
	}
	third.Proposals[0].Changes[0].Apply = false
	if _, err := mapping.PublishGroup(2, third); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mutated plan error=%v", err)
	}
}

func TestSnapshotOwnsEntries(t *testing.T) {
	addr := testAddr(t, 1, 64)
	mapping, err := New(Snapshot{Entries: map[model.ID]Entry{1: {Addr: addr, Revision: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mapping.Snapshot()
	delete(snapshot.Entries, 1)
	if _, exists, _ := mapping.Lookup(1); !exists {
		t.Fatal("snapshot mutation changed mapping")
	}
}
