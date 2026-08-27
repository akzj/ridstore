package mapping

import (
	"testing"

	"github.com/akzj/ridstore/internal/model"
)

type failingReservation struct{}

func (failingReservation) Release() {}

func (failingReservation) consume(uint64) (uint64, error) { return 0, ErrCorrupt }

type fixedReservation uint64

func (fixedReservation) Release() {}

func (r fixedReservation) consume(uint64) (uint64, error) { return uint64(r), nil }

func reservePlan(t *testing.T, index Index, plan GroupPlan) []DeltaReservation {
	t.Helper()
	reservations := make([]DeltaReservation, len(plan.Proposals))
	for proposalIndex, proposal := range plan.Proposals {
		ids := make([]model.ID, len(proposal.Changes))
		for changeIndex, change := range proposal.Changes {
			ids[changeIndex] = change.Change.RecordID
		}
		reservation, _, err := index.ReserveDelta(ids)
		if err != nil {
			t.Fatal(err)
		}
		reservations[proposalIndex] = reservation
	}
	return reservations
}
