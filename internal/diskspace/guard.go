package diskspace

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/ridstore/internal/base"
)

// Reader returns bytes currently available to the process on path's
// filesystem. The value is an admission signal, not a reservation.
type Reader func(path string) (uint64, error)

// Guard rejects new space-consuming work below a configured free-space
// watermark. Between filesystem observations it conservatively deducts every
// guarded payload/reserve estimate so newly admitted work cannot outrun the
// refresh interval merely through ridstore's foreground append rate.
// External writers can still consume space concurrently, so callers must
// continue handling ENOSPC from every write and fsync.
type Guard struct {
	refreshMu sync.RWMutex

	path      string
	minFree   uint64
	interval  time.Duration
	available Reader
	now       func() time.Time

	lastCheck   atomic.Int64
	estimate    atomic.Uint64
	lastError   atomic.Pointer[observationError]
	stopped     atomic.Bool
	rejections  atomic.Uint64
	checkErrors atomic.Uint64
}

type Snapshot struct {
	AvailableBytes uint64
	MinFreeBytes   uint64
	Stopped        bool
	Rejections     uint64
	CheckErrors    uint64
}

type observationError struct{ err error }

func NewGuard(path string, minFree uint64, interval time.Duration, available Reader) (*Guard, error) {
	if path == "" || interval <= 0 || interval > time.Minute || available == nil {
		return nil, base.ErrInvalidConfig
	}
	return &Guard{path: path, minFree: minFree, interval: interval, available: available, now: time.Now}, nil
}

// Admit performs an admission check without keeping an in-flight append
// reservation. It is used for zero-byte lifecycle gates.
func (g *Guard) Admit(ctx context.Context, bytes uint64) error {
	if err := g.Reserve(ctx, bytes); err != nil {
		return err
	}
	g.Release()
	return nil
}

// Reserve admits bytes and keeps the refresh read gate held. The caller must
// call Release exactly once after its corresponding append attempt returns.
// This prevents a filesystem refresh from replacing the conservative
// deduction in the admission-to-append window.
func (g *Guard) Reserve(ctx context.Context, bytes uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.refreshIfDue(); err != nil {
		return err
	}
	// Keep a refresh from replacing the observed value between the CAS
	// reservation and the caller's append completion.
	g.refreshMu.RLock()
	if observed := g.lastError.Load(); observed != nil {
		g.refreshMu.RUnlock()
		return observed.err
	}
	required, err := base.AddUint64(g.minFree, bytes)
	if err != nil {
		g.refreshMu.RUnlock()
		return err
	}
	for {
		available := g.estimate.Load()
		if available < required {
			g.stopped.Store(true)
			g.rejections.Add(1)
			g.refreshMu.RUnlock()
			return fmt.Errorf("new write requires %d bytes plus %d-byte free-space watermark with %d available: %w", bytes, g.minFree, available, base.ErrInsufficientSpace)
		}
		if g.estimate.CompareAndSwap(available, available-bytes) {
			g.stopped.Store(false)
			return nil
		}
	}
}

func (g *Guard) Release() { g.refreshMu.RUnlock() }

func (g *Guard) refreshIfDue() error {
	now := g.now().UnixNano()
	if !g.due(now) {
		return nil
	}
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	if !g.due(now) {
		return nil
	}
	available, err := g.available(g.path)
	if err != nil {
		g.stopped.Store(true)
		g.checkErrors.Add(1)
		observed := &observationError{err: fmt.Errorf("observe filesystem free space: %w", err)}
		g.lastError.Store(observed)
		g.lastCheck.Store(now)
		return observed.err
	}
	g.estimate.Store(available)
	g.lastError.Store(nil)
	g.lastCheck.Store(now)
	return nil
}

func (g *Guard) due(now int64) bool {
	last := g.lastCheck.Load()
	return last == 0 || now < last || now-last >= int64(g.interval)
}

func (g *Guard) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{}
	}
	return Snapshot{
		AvailableBytes: g.estimate.Load(),
		MinFreeBytes:   g.minFree,
		Stopped:        g.stopped.Load(),
		Rejections:     g.rejections.Load(),
		CheckErrors:    g.checkErrors.Load(),
	}
}
