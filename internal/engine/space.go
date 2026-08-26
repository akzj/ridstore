package engine

import (
	"context"
	"errors"
	"math"
	"sync"
	"syscall"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
)

type availableBytes func(string) (uint64, error)

type spaceGate struct {
	mu sync.Mutex

	root      string
	minimum   uint64
	interval  time.Duration
	available availableBytes
	now       func() time.Time

	valid      bool
	lastSample time.Time
	free       uint64
	reserved   uint64
}

type spaceReservation struct {
	gate  *spaceGate
	bytes uint64
}

func newSpaceGate(root string, minimum uint64, interval time.Duration, available availableBytes) *spaceGate {
	return &spaceGate{root: root, minimum: minimum, interval: interval, available: available, now: time.Now}
}

func (g *spaceGate) reserve(ctx context.Context, bytes uint64) (*spaceReservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bytes == 0 || g == nil || g.root == "" || g.minimum == 0 || g.interval <= 0 || g.available == nil || g.now == nil {
		return nil, base.ErrInvalidConfig
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if !g.valid || g.reserved == 0 && now.Sub(g.lastSample) >= g.interval {
		if err := g.refreshLocked(now); err != nil {
			return nil, err
		}
	}
	if !fitsBelow(g.reserved, bytes, g.free, g.minimum) && g.reserved == 0 {
		if err := g.refreshLocked(now); err != nil {
			return nil, err
		}
	}
	if !fitsBelow(g.reserved, bytes, g.free, g.minimum) {
		return nil, base.ErrInsufficientSpace
	}
	g.reserved += bytes
	return &spaceReservation{gate: g, bytes: bytes}, nil
}

func (g *spaceGate) refreshLocked(now time.Time) error {
	free, err := g.available(g.root)
	if err != nil {
		g.valid = false
		return errors.Join(base.ErrInsufficientSpace, err)
	}
	g.free, g.lastSample, g.valid = free, now, true
	return nil
}

func fitsBelow(reserved, requested, free, minimum uint64) bool {
	if reserved > math.MaxUint64-requested {
		return false
	}
	used := reserved + requested
	return minimum <= free && used <= free-minimum
}

func (r *spaceReservation) complete(success bool) {
	if r == nil || r.gate == nil || r.bytes == 0 {
		return
	}
	g := r.gate
	g.mu.Lock()
	if r.bytes <= g.reserved {
		g.reserved -= r.bytes
	} else {
		g.reserved = 0
		g.valid = false
	}
	if success {
		if r.bytes <= g.free {
			g.free -= r.bytes
		} else {
			g.free = 0
		}
	} else {
		g.valid = false
	}
	r.bytes = 0
	g.mu.Unlock()
}

type spaceAppender struct {
	next transactionAppender
	gate *spaceGate
}

type transactionAppender interface {
	Append(context.Context, []byte, bool) (recordlog.AppendResult, error)
}

func (a *spaceAppender) Append(ctx context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	if a == nil || a.next == nil || a.gate == nil {
		return recordlog.AppendResult{}, base.ErrInvalidConfig
	}
	typ, err := recordcodec.TypeOf(payload)
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	if typ != recordcodec.RecordTypePut {
		return a.next.Append(ctx, payload, syncWrite)
	}
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	reservation, err := a.gate.reserve(ctx, uint64(physical))
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	result, err := a.next.Append(ctx, payload, syncWrite)
	reservation.complete(err == nil)
	return result, err
}

func filesystemAvailable(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, errors.New("invalid filesystem block size")
	}
	if stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return math.MaxUint64, nil
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
