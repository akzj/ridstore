package api

import (
	"context"

	"github.com/akzj/ridstore/internal/base"
)

type ApplyKind uint8

const (
	ApplyUserCommit ApplyKind = iota + 1
	ApplyRelocation
)

type Change struct {
	RecordID        base.ID
	NewAddr         base.VAddr
	ExpectedOldAddr base.VAddr
}

type ApplyResult struct {
	Applied uint32
	Skipped uint32
}

// Mapping is the logical ID-to-physical-address contract shared by the memory
// oracle and the persistent radix implementation.
type Mapping interface {
	Lookup(base.ID) (base.VAddr, bool, error)
	Apply(base.CommitSeq, ApplyKind, []Change) (ApplyResult, error)
	CoveredCommitSeq() base.CommitSeq
	Snapshot() Snapshot
}

// DeltaBudget is implemented by persistent mappings whose committed overlay
// has a bounded memory budget. Reservations are acquired before a commit is
// admitted to the coordinator, so a checkpoint barrier can always overtake a
// writer waiting for memory.
type DeltaBudget interface {
	ReserveDelta(context.Context, uint64) (DeltaReservation, bool, error)
	ApplyReserved(DeltaReservation, base.CommitSeq, ApplyKind, []Change) (ApplyResult, error)
}

type DeltaReservation interface {
	Release()
}

type Snapshot struct {
	CoveredCommitSeq base.CommitSeq
	Entries          map[base.ID]base.VAddr
}
