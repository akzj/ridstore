package v2

import (
	"context"
	"sync"
)

type byteBudget struct {
	mu      sync.Mutex
	max     uint64
	used    uint64
	changed chan struct{}
	closed  bool
}

func newByteBudget(max uint64) *byteBudget {
	return &byteBudget{max: max, changed: make(chan struct{})}
}

func (b *byteBudget) acquire(ctx context.Context, n uint64) error {
	if n > b.max {
		return ErrPayloadTooBig
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if n <= b.max-b.used {
			b.used += n
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (b *byteBudget) release(n uint64) {
	b.mu.Lock()
	if n > b.used {
		b.used = 0
	} else {
		b.used -= n
	}
	if !b.closed {
		close(b.changed)
		b.changed = make(chan struct{})
	}
	b.mu.Unlock()
}

func (b *byteBudget) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.changed)
	}
	b.mu.Unlock()
}

func (b *byteBudget) usage() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}
