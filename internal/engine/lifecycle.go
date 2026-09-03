package engine

import (
	"context"
	"sync"

	"github.com/akzj/ridstore/internal/base"
)

// storeLifecycle is the sole owner of Store admission and shutdown state.
// Its mutex protects only counters and state transitions; it is never held
// while executing an operation, cancelling work, or waiting for a goroutine.
type storeLifecycle struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu        sync.Mutex
	closing   bool
	active    uint64
	drained   chan struct{}
	drainOnce sync.Once
	done      chan struct{}
	closeOnce sync.Once

	resultMu sync.Mutex
	result   error
}

func newStoreLifecycle() *storeLifecycle {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &storeLifecycle{ctx: ctx, cancel: cancel, drained: make(chan struct{}), done: make(chan struct{})}
}

func (l *storeLifecycle) begin(parent context.Context) (context.Context, func(), error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return nil, nil, base.ErrClosed
	}
	l.active++
	l.mu.Unlock()

	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(l.ctx, func() { cancel(base.ErrClosed) })
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			stop()
			cancel(nil)
			l.end()
		})
	}, nil
}

func (l *storeLifecycle) end() {
	l.mu.Lock()
	if l.active == 0 {
		l.mu.Unlock()
		return
	}
	l.active--
	drained := l.closing && l.active == 0
	l.mu.Unlock()
	if drained {
		l.drainOnce.Do(func() { close(l.drained) })
	}
}

func (l *storeLifecycle) startClose(shutdown func() error) {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closing = true
		drained := l.active == 0
		l.mu.Unlock()
		l.cancel(base.ErrClosed)
		if drained {
			l.drainOnce.Do(func() { close(l.drained) })
		}
		go func() {
			result := shutdown()
			l.resultMu.Lock()
			l.result = result
			l.resultMu.Unlock()
			close(l.done)
		}()
	})
}

func (l *storeLifecycle) wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-l.done:
		l.resultMu.Lock()
		defer l.resultMu.Unlock()
		return l.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *storeLifecycle) Done() <-chan struct{}    { return l.done }
func (l *storeLifecycle) Drained() <-chan struct{} { return l.drained }
func (l *storeLifecycle) Context() context.Context { return l.ctx }
