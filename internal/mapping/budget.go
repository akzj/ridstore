package mapping

import (
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
)

const deltaEntryCharge = uint64(64)

type DeltaReservation interface {
	Release()
	consume(uint64) (uint64, error)
}

type deltaBudget struct {
	mu       sync.Mutex
	soft     uint64
	hard     uint64
	charged  uint64
	reserved uint64
}

type deltaReservation struct {
	mu       sync.Mutex
	budget   *deltaBudget
	upper    uint64
	consumed bool
}

func newDeltaBudget(soft, hard uint64) (*deltaBudget, error) {
	if soft == 0 || hard <= soft || hard < deltaEntryCharge {
		return nil, ErrInvalid
	}
	return &deltaBudget{soft: soft, hard: hard}, nil
}

func (b *deltaBudget) reserve(entries uint64) (DeltaReservation, bool, error) {
	bytes, overflow := multiply(entries, deltaEntryCharge)
	if overflow || bytes > b.hard {
		return nil, false, base.ErrBatchTooLarge
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.charged > b.hard || b.reserved > b.hard-b.charged || bytes > b.hard-b.charged-b.reserved {
		return nil, true, ErrBudget
	}
	b.reserved += bytes
	return &deltaReservation{budget: b, upper: bytes}, b.charged+b.reserved >= b.soft, nil
}

func (r *deltaReservation) Release() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.consumed {
		r.mu.Unlock()
		return
	}
	r.consumed = true
	budget, upper := r.budget, r.upper
	r.mu.Unlock()
	budget.mu.Lock()
	if upper <= budget.reserved {
		budget.reserved -= upper
	}
	budget.mu.Unlock()
}

func (r *deltaReservation) consume(entries uint64) (uint64, error) {
	actual, overflow := multiply(entries, deltaEntryCharge)
	if overflow {
		return 0, ErrCorrupt
	}
	r.mu.Lock()
	if r.consumed || r.budget == nil || actual > r.upper {
		r.mu.Unlock()
		return 0, ErrCorrupt
	}
	r.consumed = true
	budget, upper := r.budget, r.upper
	r.mu.Unlock()
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if upper > budget.reserved || budget.charged > budget.hard-actual {
		return 0, ErrCorrupt
	}
	budget.reserved -= upper
	budget.charged += actual
	return actual, nil
}

func (b *deltaBudget) usage() (charged, reserved, soft, hard uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.charged, b.reserved, b.soft, b.hard
}

func multiply(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, true
	}
	return left * right, false
}

type unlimitedReservation struct{}

func (*unlimitedReservation) Release() {}

func (*unlimitedReservation) consume(uint64) (uint64, error) { return 0, nil }
