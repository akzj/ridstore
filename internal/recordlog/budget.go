package recordlog

import (
	"context"
	"sync"
)

type byteBudget struct {
	mu      sync.Mutex
	limit   uint64
	used    uint64
	changed chan struct{}
	closed  bool
}

func newByteBudget(limit uint64) *byteBudget {
	return &byteBudget{limit: limit, changed: make(chan struct{})}
}

func (b *byteBudget) acquire(ctx context.Context, bytes uint64) error {
	if bytes > b.limit {
		return ErrPayloadTooBig
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if bytes <= b.limit-b.used {
			b.used += bytes
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

func (b *byteBudget) release(bytes uint64) {
	b.mu.Lock()
	if bytes > b.used {
		panic("recordlog: queued byte budget underflow")
	}
	b.used -= bytes
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
