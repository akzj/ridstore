package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/recordlog"
)

// maintenanceResource is owned exclusively by the scheduler actor. Workers
// declare the resources for a phase; they cannot acquire resources themselves.
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

// maintenanceRequestKind is the closed set of work understood by the Store's
// maintenance runtime. Payload fields form a tagged union selected by kind.
type maintenanceRequestKind uint8

const (
	maintenanceCheckpointRequest maintenanceRequestKind = iota + 1
	maintenanceSegmentRelocateRequest
	maintenanceSegmentPrepareRequest
	maintenanceSegmentCompactRequest
	maintenanceSegmentNextRequest
	maintenanceMappingGCRequest
	maintenanceMappingSurveyRequest
)

type maintenanceRequest struct {
	kind        maintenanceRequestKind
	source      recordlog.SegmentID
	policy      CompactionPolicy
	generation  uint64
	gcAdmission bool
	force       bool
	periodic    bool
	background  bool
	automatic   bool
}

// maintenanceResult is the typed result union returned to API waiters and
// dependency continuations.
type maintenanceResult struct {
	relocation SegmentRelocationResult
	proof      SegmentRetirementProof
	compaction SegmentCompactionResult
	next       NextSegmentCompactionResult
	found      bool
	usage      *mappingUsage
}

type maintenancePhase uint8

const (
	maintenancePhaseStart maintenancePhase = iota + 1
	maintenancePhaseCopy
	maintenancePhaseProve
	maintenancePhaseRetire
	maintenancePhasePublish
	maintenancePhaseCleanup
)

// maintenanceTransition is the only way a worker changes state. Retain keeps
// a logical resource across a dependency (Segment GC keeps recoveryProtocol
// while yielding Checkpoint); all other phase resources are released first.
type maintenanceTransition struct {
	next       maintenancePhase
	retain     maintenanceResource
	dependency *maintenanceRequest
	result     maintenanceResult
	done       bool
	retryAfter time.Duration
	err        error
}

type maintenanceWorker interface {
	Resources(maintenancePhase) maintenanceResource
	Run(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition
}

type maintenanceWorkerFactory interface {
	NewMaintenanceWorker(maintenanceRequest) (maintenanceWorker, error)
}

type maintenanceWaiter struct {
	id     uint64
	result chan maintenanceCompletion
}

type maintenanceCompletion struct {
	result maintenanceResult
	err    error
}

type maintenanceJob struct {
	id, sequence   uint64
	request        maintenanceRequest
	key            string
	priority       maintenancePriority
	phase          maintenancePhase
	worker         maintenanceWorker
	waiters        map[uint64]maintenanceWaiter
	nextWaiters    map[uint64]maintenanceWaiter
	background     bool
	nextBackground bool
	runAgain       bool
	nextRequest    maintenanceRequest

	dependencyResult maintenanceResult
	dependents       map[uint64]struct{}
	nextDependents   map[uint64]struct{}
	waiting          bool

	held, runningResources maintenanceResource
	cancel                 context.CancelFunc
	preempted              bool
}

type maintenanceSubmit struct {
	request  maintenanceRequest
	waiter   *maintenanceWaiter
	accepted chan error
}

type maintenanceCancel struct{ jobID, waiterID uint64 }
type maintenanceDone struct {
	jobID      uint64
	transition maintenanceTransition
}
type maintenanceRetry struct{ jobID uint64 }
type maintenanceScheduleConfig struct {
	checkpointInterval time.Duration
	maintenance        MaintenanceConfig
	accepted           chan error
}

// MaintenanceScheduler owns request admission, coalescing, worker creation,
// phase transitions, dependencies, resources, cancellation and shutdown.
// Worker code has no reference back to the scheduler.
type MaintenanceScheduler struct {
	ctx                                                  context.Context
	cancel                                               context.CancelFunc
	factory                                              maintenanceWorkerFactory
	submitCh                                             chan maintenanceSubmit
	cancelCh                                             chan maintenanceCancel
	doneCh                                               chan maintenanceDone
	retryCh                                              chan maintenanceRetry
	configureCh                                          chan maintenanceScheduleConfig
	closedCh                                             chan struct{}
	once                                                 sync.Once
	requested, coalesced, completed, failed, preemptions atomic.Uint64
	queued, running                                      atomic.Uint64
}

func newMaintenanceScheduler(parent context.Context, factory maintenanceWorkerFactory) *MaintenanceScheduler {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	m := &MaintenanceScheduler{
		ctx: ctx, cancel: cancel,
		factory:  factory,
		submitCh: make(chan maintenanceSubmit), cancelCh: make(chan maintenanceCancel),
		doneCh: make(chan maintenanceDone), retryCh: make(chan maintenanceRetry), configureCh: make(chan maintenanceScheduleConfig),
		closedCh: make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *MaintenanceScheduler) Configure(checkpointInterval time.Duration, maintenance MaintenanceConfig) error {
	if m == nil || checkpointInterval <= 0 {
		return base.ErrInvalidConfig
	}
	accepted := make(chan error, 1)
	select {
	case m.configureCh <- maintenanceScheduleConfig{checkpointInterval: checkpointInterval, maintenance: maintenance, accepted: accepted}:
	case <-m.closedCh:
		return base.ErrClosed
	}
	return <-accepted
}

func (m *MaintenanceScheduler) Submit(ctx context.Context, request maintenanceRequest) (maintenanceResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return maintenanceResult{}, err
	}
	if m == nil {
		return maintenanceResult{}, base.ErrInvalidConfig
	}
	waiter := maintenanceWaiter{result: make(chan maintenanceCompletion, 1)}
	accepted := make(chan error, 1)
	select {
	case m.submitCh <- maintenanceSubmit{request: request, waiter: &waiter, accepted: accepted}:
	case <-m.closedCh:
		return maintenanceResult{}, base.ErrClosed
	case <-ctx.Done():
		return maintenanceResult{}, ctx.Err()
	}
	if err := <-accepted; err != nil {
		return maintenanceResult{}, err
	}
	select {
	case completion := <-waiter.result:
		return completion.result, completion.err
	case <-ctx.Done():
		select {
		case m.cancelCh <- maintenanceCancel{jobID: waiter.id >> 32, waiterID: waiter.id & 0xffffffff}:
		case <-m.closedCh:
		}
		return maintenanceResult{}, ctx.Err()
	case <-m.closedCh:
		return maintenanceResult{}, base.ErrClosed
	}
}

func (m *MaintenanceScheduler) SubmitBackground(request maintenanceRequest) error {
	if m == nil {
		return base.ErrInvalidConfig
	}
	request.background = true
	accepted := make(chan error, 1)
	select {
	case m.submitCh <- maintenanceSubmit{request: request, accepted: accepted}:
	case <-m.closedCh:
		return base.ErrClosed
	}
	return <-accepted
}

func (m *MaintenanceScheduler) Close() {
	if m == nil {
		return
	}
	m.once.Do(m.cancel)
	<-m.closedCh
}

func (m *MaintenanceScheduler) Done() <-chan struct{} { return m.closedCh }

func maintenanceRequestPolicy(request maintenanceRequest) (string, maintenancePriority, bool, bool, error) {
	switch request.kind {
	case maintenanceCheckpointRequest:
		return "checkpoint", maintenancePriorityCheckpoint, true, false, nil
	case maintenanceMappingGCRequest:
		return "mapping-gc", maintenancePriorityMapping, false, true, nil
	case maintenanceMappingSurveyRequest:
		return "mapping-survey", maintenancePriorityMapping, false, true, nil
	case maintenanceSegmentRelocateRequest, maintenanceSegmentPrepareRequest,
		maintenanceSegmentCompactRequest, maintenanceSegmentNextRequest:
		// Explicit Segment requests are independent. Automatic Segment requests
		// use one stable key and therefore coalesce.
		if request.background {
			return "segment-auto", maintenancePrioritySegment, false, false, nil
		}
		return "", maintenancePrioritySegment, false, false, nil
	default:
		return "", 0, false, false, base.ErrInvalidConfig
	}
}

func (m *MaintenanceScheduler) run() {
	defer close(m.closedCh)
	var nextJobID, nextWaiterID, sequence uint64
	pending := make([]*maintenanceJob, 0)
	active := make(map[uint64]*maintenanceJob)
	jobs := make(map[uint64]*maintenanceJob)
	byKey := make(map[string]*maintenanceJob)
	var leased maintenanceResource
	closing := false
	shutdownC := m.ctx.Done()
	var checkpointTimer, maintenanceTimer *time.Ticker
	var checkpointC, maintenanceC <-chan time.Time
	var automaticPolicy MaintenanceConfig

	finishWaiters := func(waiters map[uint64]maintenanceWaiter, completion maintenanceCompletion) {
		for _, waiter := range waiters {
			waiter.result <- completion
		}
	}
	removeJob := func(job *maintenanceJob) {
		leased &^= job.held
		job.held = 0
		delete(jobs, job.id)
		if job.key != "" && byKey[job.key] == job {
			delete(byKey, job.key)
		}
	}
	var enqueue func(maintenanceRequest, uint64) (*maintenanceJob, error)
	enqueue = func(request maintenanceRequest, dependent uint64) (*maintenanceJob, error) {
		key, priority, rerun, _, err := maintenanceRequestPolicy(request)
		if err != nil || m.factory == nil {
			if err == nil {
				err = base.ErrInvalidConfig
			}
			return nil, err
		}
		job := byKey[key]
		if key == "" {
			job = nil
		}
		if job != nil {
			m.coalesced.Add(1)
		}
		running := job != nil && active[job.id] != nil
		if job == nil {
			worker, workerErr := m.factory.NewMaintenanceWorker(request)
			if workerErr != nil {
				return nil, workerErr
			}
			nextJobID++
			sequence++
			job = &maintenanceJob{id: nextJobID, sequence: sequence, request: request, key: key,
				priority: priority, phase: maintenancePhaseStart, worker: worker,
				waiters: make(map[uint64]maintenanceWaiter), dependents: make(map[uint64]struct{})}
			jobs[job.id] = job
			if key != "" {
				byKey[key] = job
			}
			pending = append(pending, job)
		}
		if running && rerun {
			job.runAgain = true
			job.nextRequest = mergeMaintenanceRequest(job.nextRequest, request)
			job.nextBackground = job.nextBackground || request.background
			if dependent != 0 {
				if job.nextDependents == nil {
					job.nextDependents = make(map[uint64]struct{})
				}
				job.nextDependents[dependent] = struct{}{}
			}
		} else {
			if !running {
				job.request = mergeMaintenanceRequest(job.request, request)
				worker, workerErr := m.factory.NewMaintenanceWorker(job.request)
				if workerErr != nil {
					return nil, workerErr
				}
				job.worker = worker
			}
			job.background = job.background || request.background
			if dependent != 0 {
				job.dependents[dependent] = struct{}{}
			}
		}
		return job, nil
	}

	dispatch := func() {
		for {
			best := -1
			for i, job := range pending {
				need := job.worker.Resources(job.phase) &^ job.held
				if need&leased != 0 {
					continue
				}
				if best < 0 || job.priority > pending[best].priority || (job.priority == pending[best].priority && job.sequence < pending[best].sequence) {
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
			need := job.worker.Resources(job.phase) &^ job.held
			leased |= need
			job.runningResources = need
			ctx, cancel := context.WithCancel(m.ctx)
			ctx = context.WithValue(ctx, maintenanceWorkerContextKey{}, true)
			job.cancel = cancel
			active[job.id] = job
			phase, dependency := job.phase, job.dependencyResult
			m.queued.Store(uint64(len(pending)))
			m.running.Store(uint64(len(active)))
			go func(jobID uint64, worker maintenanceWorker) {
				transition := worker.Run(ctx, phase, dependency)
				select {
				case m.doneCh <- maintenanceDone{jobID: jobID, transition: transition}:
				case <-m.closedCh:
				}
			}(job.id, job.worker)
		}
	}

	complete := func(job *maintenanceJob, completion maintenanceCompletion) {
		finishWaiters(job.waiters, completion)
		for dependentID := range job.dependents {
			if dependent := jobs[dependentID]; dependent != nil && dependent.waiting {
				dependent.waiting = false
				dependent.dependencyResult = completion.result
				if completion.err != nil {
					dependent.dependencyResult = maintenanceResult{}
				}
				// Dependency errors are delivered through a synthetic transition on
				// the resumed worker, which decides whether they are retryable.
				if completion.err != nil {
					dependent.worker = &failedDependencyWorker{next: dependent.worker, err: completion.err}
				}
				sequence++
				dependent.sequence = sequence
				pending = append(pending, dependent)
			}
		}
		if completion.err == nil {
			m.completed.Add(1)
		} else {
			m.failed.Add(1)
		}
	}

	for {
		if closing && len(active) == 0 {
			return
		}
		select {
		case config := <-m.configureCh:
			if checkpointTimer != nil {
				config.accepted <- base.ErrInvalidConfig
				continue
			}
			checkpointTimer = time.NewTicker(config.checkpointInterval)
			checkpointC = checkpointTimer.C
			automaticPolicy = config.maintenance
			if automaticPolicy.Enabled {
				maintenanceTimer = time.NewTicker(automaticPolicy.Interval)
				maintenanceC = maintenanceTimer.C
			}
			config.accepted <- nil
		case <-checkpointC:
			m.requested.Add(1)
			_, _ = enqueue(maintenanceRequest{kind: maintenanceCheckpointRequest, background: true, periodic: true}, 0)
			dispatch()
		case <-maintenanceC:
			if !automaticPolicy.DisableSegmentGC {
				m.requested.Add(1)
				_, _ = enqueue(maintenanceRequest{kind: maintenanceSegmentNextRequest, policy: normalizeCompactionPolicy(automaticPolicy.SegmentPolicy), background: true, automatic: true}, 0)
			}
			if !automaticPolicy.DisableMappingGC {
				m.requested.Add(1)
				_, _ = enqueue(maintenanceRequest{kind: maintenanceMappingGCRequest, background: true, automatic: true}, 0)
			}
			dispatch()
		case submission := <-m.submitCh:
			if closing {
				submission.accepted <- base.ErrClosed
				continue
			}
			m.requested.Add(1)
			job, err := enqueue(submission.request, 0)
			if err == nil && submission.waiter != nil {
				nextWaiterID++
				submission.waiter.id = job.id<<32 | (nextWaiterID & 0xffffffff)
				if active[job.id] != nil && job.runAgain {
					if job.nextWaiters == nil {
						job.nextWaiters = make(map[uint64]maintenanceWaiter)
					}
					job.nextWaiters[nextWaiterID&0xffffffff] = *submission.waiter
				} else {
					job.waiters[nextWaiterID&0xffffffff] = *submission.waiter
				}
			}
			submission.accepted <- err
			if err == nil {
				for _, running := range active {
					_, _, _, preemptible, _ := maintenanceRequestPolicy(running.request)
					need := job.worker.Resources(job.phase)
					if job.priority > running.priority && preemptible && need&running.runningResources != 0 {
						if !running.preempted {
							m.preemptions.Add(1)
						}
						running.preempted = true
						running.cancel()
					}
				}
			}
			dispatch()
		case cancellation := <-m.cancelCh:
			job := jobs[cancellation.jobID]
			if job == nil {
				continue
			}
			delete(job.waiters, cancellation.waiterID)
			delete(job.nextWaiters, cancellation.waiterID)
			if len(job.waiters) == 0 && len(job.nextWaiters) == 0 && !job.background && !job.nextBackground && len(job.dependents) == 0 && len(job.nextDependents) == 0 {
				if active[job.id] != nil {
					job.cancel()
				} else if !job.waiting {
					pending = removeMaintenanceJob(pending, job.id)
					removeJob(job)
				}
			}
		case done := <-m.doneCh:
			job := active[done.jobID]
			if job == nil {
				continue
			}
			delete(active, job.id)
			job.cancel()
			job.cancel = nil
			leased &^= job.runningResources
			job.runningResources = 0
			transition := done.transition
			if transition.retain&^(job.held|job.worker.Resources(job.phase)) != 0 {
				transition.err = errors.Join(base.ErrInvalidConfig, errors.New("worker retained an unowned maintenance resource"))
			}
			leased &^= job.held &^ transition.retain
			job.held = transition.retain
			leased |= job.held
			if job.preempted && errors.Is(transition.err, context.Canceled) && !closing {
				job.preempted = false
				sequence++
				job.sequence = sequence
				pending = append(pending, job)
				dispatch()
				continue
			}
			if transition.err != nil || transition.done {
				completion := maintenanceCompletion{result: transition.result, err: transition.err}
				complete(job, completion)
				if job.runAgain && !closing {
					job.waiters, job.nextWaiters = job.nextWaiters, nil
					if job.waiters == nil {
						job.waiters = make(map[uint64]maintenanceWaiter)
					}
					job.dependents, job.nextDependents = job.nextDependents, nil
					if job.dependents == nil {
						job.dependents = make(map[uint64]struct{})
					}
					job.background, job.nextBackground = job.nextBackground, false
					job.request = mergeMaintenanceRequest(job.request, job.nextRequest)
					job.nextRequest = maintenanceRequest{}
					worker, workerErr := m.factory.NewMaintenanceWorker(job.request)
					if workerErr != nil {
						finishWaiters(job.waiters, maintenanceCompletion{err: workerErr})
						removeJob(job)
						dispatch()
						continue
					}
					job.worker = worker
					job.runAgain = false
					job.phase = maintenancePhaseStart
					job.dependencyResult = maintenanceResult{}
					sequence++
					job.sequence = sequence
					pending = append(pending, job)
				} else {
					removeJob(job)
				}
			} else if transition.dependency != nil {
				job.phase = transition.next
				job.waiting = true
				if _, err := enqueue(*transition.dependency, job.id); err != nil {
					job.waiting = false
					complete(job, maintenanceCompletion{err: err})
					removeJob(job)
				}
			} else {
				job.phase = transition.next
				sequence++
				job.sequence = sequence
				if transition.retryAfter > 0 {
					time.AfterFunc(transition.retryAfter, func() {
						select {
						case m.retryCh <- maintenanceRetry{jobID: job.id}:
						case <-m.closedCh:
						}
					})
				} else {
					pending = append(pending, job)
				}
			}
			dispatch()
		case retry := <-m.retryCh:
			if job := jobs[retry.jobID]; job != nil && active[job.id] == nil && !job.waiting {
				sequence++
				job.sequence = sequence
				pending = append(pending, job)
				dispatch()
			}
		case <-shutdownC:
			shutdownC = nil
			closing = true
			if checkpointTimer != nil {
				checkpointTimer.Stop()
				checkpointC = nil
			}
			if maintenanceTimer != nil {
				maintenanceTimer.Stop()
				maintenanceC = nil
			}
			for _, job := range jobs {
				finishWaiters(job.waiters, maintenanceCompletion{err: base.ErrClosed})
				finishWaiters(job.nextWaiters, maintenanceCompletion{err: base.ErrClosed})
				job.waiters = nil
				job.nextWaiters = nil
				if job.cancel != nil {
					job.cancel()
				}
			}
			pending = nil
			m.queued.Store(0)
		}
	}
}

func mergeMaintenanceRequest(current, incoming maintenanceRequest) maintenanceRequest {
	if current.kind == 0 {
		return incoming
	}
	current.force = current.force || incoming.force
	current.gcAdmission = current.gcAdmission || incoming.gcAdmission
	current.background = current.background || incoming.background
	current.automatic = current.automatic || incoming.automatic
	current.periodic = current.periodic || incoming.periodic
	if incoming.generation > current.generation {
		current.generation = incoming.generation
	}
	return current
}

// failedDependencyWorker turns a dependency completion into the next worker
// invocation without exposing scheduler internals to that worker.
type failedDependencyWorker struct {
	next maintenanceWorker
	err  error
}

func (w *failedDependencyWorker) Resources(phase maintenancePhase) maintenanceResource {
	return w.next.Resources(phase)
}
func (w *failedDependencyWorker) Run(context.Context, maintenancePhase, maintenanceResult) maintenanceTransition {
	return maintenanceTransition{done: true, err: w.err}
}

type maintenanceSchedulerMetrics struct{ requested, coalesced, completed, failed, preemptions, queued, running uint64 }

func (m *MaintenanceScheduler) metrics() maintenanceSchedulerMetrics {
	if m == nil {
		return maintenanceSchedulerMetrics{}
	}
	return maintenanceSchedulerMetrics{requested: m.requested.Load(), coalesced: m.coalesced.Load(), completed: m.completed.Load(), failed: m.failed.Load(), preemptions: m.preemptions.Load(), queued: m.queued.Load(), running: m.running.Load()}
}

func removeMaintenanceJob(jobs []*maintenanceJob, id uint64) []*maintenanceJob {
	for i, job := range jobs {
		if job.id == id {
			return append(jobs[:i], jobs[i+1:]...)
		}
	}
	return jobs
}
