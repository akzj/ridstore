package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/akzj/ridstore/internal/base"
)

// maintenanceResource names scheduler-owned resources. A job receives its
// complete resource set atomically, so workers never wait while holding a
// partial lease.
type maintenanceResource uint8

const (
	maintenanceHeavyIO maintenanceResource = 1 << iota
	maintenanceMappingWriter
	maintenanceRecoveryProtocol
)

type maintenancePriority uint8

const (
	maintenancePriorityMapping maintenancePriority = iota + 1
	maintenancePrioritySegment
	maintenancePriorityCheckpoint
)

type maintenanceJobSpec struct {
	key           string
	priority      maintenancePriority
	resources     maintenanceResource
	preemptible   bool
	rerunOnActive bool
	run           func(context.Context) error
	onAccepted    func()
}

type maintenanceWaiter struct {
	id     uint64
	result chan error
}

type maintenanceJob struct {
	id             uint64
	sequence       uint64
	spec           maintenanceJobSpec
	waiters        map[uint64]maintenanceWaiter
	background     bool
	nextWaiters    map[uint64]maintenanceWaiter
	nextBackground bool
	rerun          bool
	cancel         context.CancelFunc
	preempted      bool
}

type maintenanceSubmit struct {
	spec       maintenanceJobSpec
	waiter     *maintenanceWaiter
	background bool
	accepted   chan error
}

type maintenanceCancel struct {
	jobID    uint64
	waiterID uint64
}

type maintenanceDone struct {
	jobID uint64
	err   error
}

// MaintenanceScheduler is the single owner of maintenance admission,
// priority, coalescing, cancellation, and resource allocation. Job bodies run
// outside the actor so a Segment worker may synchronously request a Checkpoint.
type MaintenanceScheduler struct {
	submitCh                                             chan maintenanceSubmit
	cancelCh                                             chan maintenanceCancel
	doneCh                                               chan maintenanceDone
	closeCh                                              chan struct{}
	closedCh                                             chan struct{}
	once                                                 sync.Once
	leaseID                                              atomic.Uint64
	requested, coalesced, completed, failed, preemptions atomic.Uint64
	queued, running                                      atomic.Uint64
}

// acquire grants a phase lease through the same actor used by whole jobs. It
// lets a multi-phase worker (notably Segment GC) release heavyIO before asking
// the Checkpoint worker for its own lease.
func (m *MaintenanceScheduler) acquire(ctx context.Context, priority maintenancePriority, resources maintenanceResource) (func(), error) {
	granted := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	var once sync.Once
	spec := maintenanceJobSpec{
		key: "lease/" + formatMaintenanceID(m.leaseID.Add(1)), priority: priority, resources: resources,
		run: func(runCtx context.Context) error {
			close(granted)
			select {
			case <-release:
				return nil
			case <-runCtx.Done():
				return runCtx.Err()
			}
		},
	}
	go func() { finished <- m.submit(ctx, spec) }()
	select {
	case <-granted:
		return func() { once.Do(func() { close(release) }) }, nil
	case err := <-finished:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.closedCh:
		return nil, base.ErrClosed
	}
}

func formatMaintenanceID(id uint64) string {
	const digits = "0123456789abcdef"
	var raw [16]byte
	for i := len(raw) - 1; i >= 0; i-- {
		raw[i] = digits[id&15]
		id >>= 4
	}
	return string(raw[:])
}

func (m *MaintenanceScheduler) uniqueKey(prefix string) string {
	return prefix + "/" + formatMaintenanceID(m.leaseID.Add(1))
}

func newMaintenanceScheduler() *MaintenanceScheduler {
	m := &MaintenanceScheduler{
		submitCh: make(chan maintenanceSubmit),
		cancelCh: make(chan maintenanceCancel),
		doneCh:   make(chan maintenanceDone),
		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *MaintenanceScheduler) submit(ctx context.Context, spec maintenanceJobSpec) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || spec.key == "" || spec.run == nil {
		return base.ErrInvalidConfig
	}
	waiter := maintenanceWaiter{result: make(chan error, 1)}
	accepted := make(chan error, 1)
	request := maintenanceSubmit{spec: spec, waiter: &waiter, accepted: accepted}
	select {
	case m.submitCh <- request:
	case <-m.closedCh:
		return base.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-accepted:
		if err != nil {
			return err
		}
	case <-m.closedCh:
		return base.ErrClosed
	}
	select {
	case err := <-waiter.result:
		return err
	case <-ctx.Done():
		select {
		case m.cancelCh <- maintenanceCancel{jobID: waiter.id >> 32, waiterID: waiter.id & 0xffffffff}:
		case <-m.closedCh:
		}
		return ctx.Err()
	case <-m.closedCh:
		return base.ErrClosed
	}
}

func (m *MaintenanceScheduler) submitBackground(spec maintenanceJobSpec) error {
	if m == nil || spec.key == "" || spec.run == nil {
		return base.ErrInvalidConfig
	}
	accepted := make(chan error, 1)
	select {
	case m.submitCh <- maintenanceSubmit{spec: spec, background: true, accepted: accepted}:
	case <-m.closedCh:
		return base.ErrClosed
	}
	return <-accepted
}

func (m *MaintenanceScheduler) Close() {
	if m == nil {
		return
	}
	m.once.Do(func() { close(m.closeCh) })
	<-m.closedCh
}

func (m *MaintenanceScheduler) run() {
	defer close(m.closedCh)
	var nextJobID, nextWaiterID, sequence uint64
	pending := make([]*maintenanceJob, 0)
	active := make(map[uint64]*maintenanceJob)
	byKey := make(map[string]*maintenanceJob)
	var leased maintenanceResource
	closing := false

	finishWaiters := func(job *maintenanceJob, err error) {
		for _, waiter := range job.waiters {
			waiter.result <- err
		}
	}
	dispatch := func() {
		for {
			best := -1
			for i, job := range pending {
				if job.spec.resources&leased != 0 {
					continue
				}
				if best < 0 || job.spec.priority > pending[best].spec.priority ||
					(job.spec.priority == pending[best].spec.priority && job.sequence < pending[best].sequence) {
					best = i
				}
			}
			if best < 0 {
				m.queued.Store(uint64(len(pending)))
				m.running.Store(uint64(len(active)))
				return
			}
			job := pending[best]
			pending = append(pending[:best], pending[best+1:]...)
			leased |= job.spec.resources
			ctx, cancel := context.WithCancel(context.Background())
			job.cancel = cancel
			active[job.id] = job
			m.queued.Store(uint64(len(pending)))
			m.running.Store(uint64(len(active)))
			go func() { m.doneCh <- maintenanceDone{jobID: job.id, err: job.spec.run(ctx)} }()
		}
	}
	for {
		if closing && len(active) == 0 {
			return
		}
		select {
		case request := <-m.submitCh:
			if closing {
				request.accepted <- base.ErrClosed
				continue
			}
			m.requested.Add(1)
			job := byKey[request.spec.key]
			if job != nil {
				m.coalesced.Add(1)
			}
			running := job != nil && active[job.id] != nil
			if job == nil {
				nextJobID++
				sequence++
				job = &maintenanceJob{id: nextJobID, sequence: sequence, spec: request.spec, waiters: make(map[uint64]maintenanceWaiter)}
				byKey[request.spec.key] = job
				pending = append(pending, job)
				m.queued.Store(uint64(len(pending)))
			}
			if running && request.spec.rerunOnActive {
				job.rerun = true
				job.nextBackground = job.nextBackground || request.background
				if request.waiter != nil {
					nextWaiterID++
					request.waiter.id = job.id<<32 | (nextWaiterID & 0xffffffff)
					if job.nextWaiters == nil {
						job.nextWaiters = make(map[uint64]maintenanceWaiter)
					}
					job.nextWaiters[nextWaiterID&0xffffffff] = *request.waiter
				}
			} else {
				job.background = job.background || request.background
				if request.waiter != nil {
					nextWaiterID++
					request.waiter.id = job.id<<32 | (nextWaiterID & 0xffffffff)
					job.waiters[nextWaiterID&0xffffffff] = *request.waiter
				}
			}
			request.accepted <- nil
			if request.spec.onAccepted != nil {
				request.spec.onAccepted()
			}
			for _, running := range active {
				if request.spec.priority > running.spec.priority && running.spec.preemptible && request.spec.resources&running.spec.resources != 0 {
					if !running.preempted {
						m.preemptions.Add(1)
					}
					running.preempted = true
					running.cancel()
				}
			}
			dispatch()
		case cancellation := <-m.cancelCh:
			job := byKeyJob(pending, active, cancellation.jobID)
			if job == nil {
				continue
			}
			delete(job.waiters, cancellation.waiterID)
			delete(job.nextWaiters, cancellation.waiterID)
			if len(job.waiters) == 0 && len(job.nextWaiters) == 0 && !job.background && !job.nextBackground {
				if _, running := active[job.id]; running {
					job.cancel()
				} else {
					pending = removeMaintenanceJob(pending, job.id)
					m.queued.Store(uint64(len(pending)))
					delete(byKey, job.spec.key)
				}
			}
		case done := <-m.doneCh:
			job := active[done.jobID]
			if job == nil {
				continue
			}
			delete(active, job.id)
			m.running.Store(uint64(len(active)))
			leased &^= job.spec.resources
			job.cancel()
			job.cancel = nil
			if job.preempted && errors.Is(done.err, context.Canceled) && !closing && (job.background || len(job.waiters) != 0 || job.nextBackground || len(job.nextWaiters) != 0) {
				job.preempted = false
				for id, waiter := range job.nextWaiters {
					job.waiters[id] = waiter
				}
				job.nextWaiters = nil
				job.background = job.background || job.nextBackground
				job.nextBackground = false
				job.rerun = false
				sequence++
				job.sequence = sequence
				pending = append(pending, job)
			} else if job.rerun && !closing {
				finishWaiters(job, done.err)
				job.waiters = job.nextWaiters
				if job.waiters == nil {
					job.waiters = make(map[uint64]maintenanceWaiter)
				}
				job.nextWaiters = nil
				job.background = job.nextBackground
				job.nextBackground = false
				job.rerun = false
				sequence++
				job.sequence = sequence
				pending = append(pending, job)
			} else {
				finishWaiters(job, done.err)
				finishWaiters(&maintenanceJob{waiters: job.nextWaiters}, done.err)
				delete(byKey, job.spec.key)
				if done.err == nil {
					m.completed.Add(1)
				} else {
					m.failed.Add(1)
				}
			}
			dispatch()
		case <-m.closeCh:
			closing = true
			for _, job := range pending {
				finishWaiters(job, base.ErrClosed)
				finishWaiters(&maintenanceJob{waiters: job.nextWaiters}, base.ErrClosed)
				delete(byKey, job.spec.key)
			}
			pending = nil
			m.queued.Store(0)
			for _, job := range active {
				job.cancel()
			}
		}
	}
}

type maintenanceSchedulerMetrics struct {
	requested, coalesced, completed, failed, preemptions, queued, running uint64
}

func (m *MaintenanceScheduler) metrics() maintenanceSchedulerMetrics {
	if m == nil {
		return maintenanceSchedulerMetrics{}
	}
	return maintenanceSchedulerMetrics{
		requested: m.requested.Load(), coalesced: m.coalesced.Load(), completed: m.completed.Load(), failed: m.failed.Load(),
		preemptions: m.preemptions.Load(), queued: m.queued.Load(), running: m.running.Load(),
	}
}

func byKeyJob(pending []*maintenanceJob, active map[uint64]*maintenanceJob, id uint64) *maintenanceJob {
	if job := active[id]; job != nil {
		return job
	}
	for _, job := range pending {
		if job.id == id {
			return job
		}
	}
	return nil
}

func removeMaintenanceJob(jobs []*maintenanceJob, id uint64) []*maintenanceJob {
	for i, job := range jobs {
		if job.id == id {
			return append(jobs[:i], jobs[i+1:]...)
		}
	}
	return jobs
}
