package radix

import (
	"context"
	"math"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping/api"
)

const deltaEntryCharge = uint64(64)

type deltaBudget struct {
	mu       sync.Mutex
	soft     uint64
	hard     uint64
	charged  uint64
	reserved uint64
	notify   chan struct{}
}

type deltaReservation struct {
	mu       sync.Mutex
	budget   *deltaBudget
	bytes    uint64
	consumed bool
}

func newDeltaBudget() *deltaBudget {
	return &deltaBudget{soft: math.MaxUint64 - 1, hard: math.MaxUint64, notify: make(chan struct{})}
}

func (m *Mapping) SetDeltaLimits(soft, hard int64) error {
	if soft <= 0 || hard <= soft {
		return base.ErrInvalidConfig
	}
	m.budget.mu.Lock()
	m.budget.soft, m.budget.hard = uint64(soft), uint64(hard)
	m.budget.mu.Unlock()
	return nil
}

func (m *Mapping) ReserveDelta(ctx context.Context, changes uint64) (api.DeltaReservation, bool, error) {
	bytes, err := base.MulUint64(changes, deltaEntryCharge)
	if err != nil {
		return nil, false, base.ErrBatchTooLarge
	}
	if bytes == 0 {
		return &deltaReservation{budget: m.budget}, false, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		m.budget.mu.Lock()
		if bytes > m.budget.hard {
			m.budget.mu.Unlock()
			return nil, false, base.ErrBatchTooLarge
		}
		used, addErr := base.AddUint64(m.budget.charged, m.budget.reserved)
		if addErr == nil && used <= m.budget.hard-bytes {
			m.budget.reserved += bytes
			soft := used+bytes >= m.budget.soft
			m.budget.mu.Unlock()
			return &deltaReservation{budget: m.budget, bytes: bytes}, soft, nil
		}
		notify := m.budget.notify
		m.budget.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-notify:
		}
	}
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
	budget, bytes := r.budget, r.bytes
	r.mu.Unlock()
	budget.mu.Lock()
	if bytes <= budget.reserved {
		budget.reserved -= bytes
	}
	budget.signalLocked()
	budget.mu.Unlock()
}

func (r *deltaReservation) consume(actual uint64) error {
	if r == nil {
		return base.ErrInvalidConfig
	}
	r.mu.Lock()
	if r.consumed || actual > r.bytes {
		r.mu.Unlock()
		return base.ErrInvalidConfig
	}
	r.consumed = true
	budget, reserved := r.budget, r.bytes
	r.mu.Unlock()
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if reserved > budget.reserved {
		return base.ErrCorrupt
	}
	budget.reserved -= reserved
	charged, err := base.AddUint64(budget.charged, actual)
	if err != nil {
		budget.signalLocked()
		return err
	}
	budget.charged = charged
	budget.signalLocked()
	return nil
}

func (b *deltaBudget) addReplay(bytes uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	charged, err := base.AddUint64(b.charged, bytes)
	if err != nil {
		return err
	}
	b.charged = charged
	return nil
}

func (b *deltaBudget) releaseCharged(bytes uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes > b.charged {
		return base.ErrCorrupt
	}
	b.charged -= bytes
	b.signalLocked()
	return nil
}

func (b *deltaBudget) signalLocked() {
	close(b.notify)
	b.notify = make(chan struct{})
}

func (m *Mapping) DeltaBytes() (charged, reserved uint64) {
	m.budget.mu.Lock()
	defer m.budget.mu.Unlock()
	return m.budget.charged, m.budget.reserved
}
