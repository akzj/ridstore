package mapping

import (
	"testing"

	"github.com/akzj/ridstore/internal/model"
)

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
