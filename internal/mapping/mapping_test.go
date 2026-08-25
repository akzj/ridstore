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
			Kind:       ProposalUserCommit,
			Conditions: []Condition{{RecordID: 7}},
			Changes:    []Change{{RecordID: 7, NewAddr: firstAddr, Operation: OperationPut}},
		},
		{
			Kind:       ProposalUserCommit,
			Conditions: []Condition{{RecordID: 7, ExpectedAddr: firstAddr}},
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
	result, err := mapping.PublishGroup(1, plan, reservePlan(t, mapping, plan))
	if err != nil || result.Committed != 2 || result.Applied != 2 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	addr, exists, err := mapping.Lookup(7)
	if err != nil || !exists || addr != secondAddr {
		t.Fatalf("addr=%+v exists=%v err=%v", addr, exists, err)
	}
	if mapping.CoveredCommitSeq() != 2 {
		t.Fatalf("covered=%d", mapping.CoveredCommitSeq())
	}
}

func TestResolveGroupRejectsConflictWithoutAffectingLaterProposal(t *testing.T) {
	old := testAddr(t, 1, 64)
	mapping, err := New(Snapshot{CoveredCommitSeq: 4, Entries: map[model.ID]recordlog.VAddr{1: old}})
	if err != nil {
		t.Fatal(err)
	}
	newAddr := testAddr(t, 1, 128)
	plan, err := mapping.ResolveGroup([]Proposal{
		{Kind: ProposalUserCommit, Conditions: []Condition{{RecordID: 1, ExpectedAddr: testAddr(t, 9, 64)}}, Changes: []Change{{RecordID: 1, Operation: OperationDelete}}},
		{Kind: ProposalUserCommit, Conditions: []Condition{{RecordID: 1, ExpectedAddr: old}}, Changes: []Change{{RecordID: 1, NewAddr: newAddr, Operation: OperationPut}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Proposals[0].Accepted || !plan.Proposals[1].Accepted {
		t.Fatalf("plan=%+v", plan)
	}
	result, err := mapping.PublishGroup(5, plan, reservePlan(t, mapping, plan))
	if err != nil || result.Committed != 1 || mapping.CoveredCommitSeq() != 5 {
		t.Fatalf("result=%+v covered=%d err=%v", result, mapping.CoveredCommitSeq(), err)
	}
	addr, exists, _ := mapping.Lookup(1)
	if !exists || addr != newAddr {
		t.Fatalf("addr=%+v exists=%v", addr, exists)
	}
}

func TestRelocationUsesAddressCAS(t *testing.T) {
	old := testAddr(t, 2, 64)
	newAddr := testAddr(t, 3, 64)
	mapping, _ := New(Snapshot{CoveredCommitSeq: 2, Entries: map[model.ID]recordlog.VAddr{5: old}})
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
	result, err := mapping.PublishGroup(3, plan, reservePlan(t, mapping, plan))
	if err != nil || result.Applied != 1 || result.Skipped != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	addr, exists, _ := mapping.Lookup(5)
	if !exists || addr != newAddr {
		t.Fatalf("addr=%+v exists=%v", addr, exists)
	}
}

func TestPublishRejectsStaleOrMutatedPlan(t *testing.T) {
	mapping := NewEmpty()
	addr := testAddr(t, 1, 64)
	proposal := Proposal{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 1, NewAddr: addr, Operation: OperationPut}}}
	first, err := mapping.ResolveGroup([]Proposal{proposal})
	if err != nil {
		t.Fatal(err)
	}
	second, err := mapping.ResolveGroup([]Proposal{proposal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.PublishGroup(1, first, reservePlan(t, mapping, first)); err != nil {
		t.Fatal(err)
	}
	if _, err := mapping.PublishGroup(2, second, reservePlan(t, mapping, second)); !errors.Is(err, ErrStalePlan) {
		t.Fatalf("stale error=%v", err)
	}
	third, err := mapping.ResolveGroup([]Proposal{{Kind: ProposalUserCommit, Changes: []Change{{RecordID: 2, NewAddr: testAddr(t, 1, 128), Operation: OperationPut}}}})
	if err != nil {
		t.Fatal(err)
	}
	third.Proposals[0].Changes[0].Apply = false
	if _, err := mapping.PublishGroup(2, third, reservePlan(t, mapping, third)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mutated plan error=%v", err)
	}
}

func TestSnapshotOwnsEntries(t *testing.T) {
	addr := testAddr(t, 1, 64)
	mapping, err := New(Snapshot{Entries: map[model.ID]recordlog.VAddr{1: addr}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mapping.Snapshot()
	delete(snapshot.Entries, 1)
	if _, exists, _ := mapping.Lookup(1); !exists {
		t.Fatal("snapshot mutation changed mapping")
	}
}
