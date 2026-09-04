# Maintenance Scheduler ownership

`MaintenanceScheduler` is an actor, not a mutual-exclusion helper. Store APIs
submit a closed, typed request union. The actor creates the corresponding
worker and is the only component allowed to:

- order and coalesce requests;
- start, cancel, retry, or resume a worker phase;
- grant `heavyIO`, `mappingWriter`, and `recoveryProtocol`;
- schedule a dependency and resume its parent;
- start periodic Checkpoint and automatic GC work;
- drain all workers during `Store.Close`.

The Scheduler context is derived from the Store lifecycle context. Shutdown is
requested by cancellation; `Done()` is the read-only completion signal. The
Scheduler owns its queue and worker cancellation state, so Store shutdown never
mutates those fields directly.

Workers receive `(phase, dependencyResult)` and return a transition. They do
not receive the Scheduler and cannot acquire a resource or synchronously call
another worker.

| Worker | Phase | Resources | Transition |
|---|---|---|---|
| Checkpoint | Start | mappingWriter | Done |
| Segment relocate | Start | heavyIO + recoveryProtocol | Done |
| Segment prepare | Start | heavyIO + recoveryProtocol | retain recoveryProtocol, depend on Checkpoint |
| Segment prepare | Prove | retained recoveryProtocol | retry Checkpoint if stats stale, else Done |
| Segment compact | Start/Copy | heavyIO + recoveryProtocol | build and durably publish immutable outputs, then Publish |
| Segment compact | Publish | heavyIO + recoveryProtocol | publish bounded relocation CAS batches; yield Checkpoint on Delta pressure; otherwise depend on final Checkpoint |
| Segment compact | Prove | retained recoveryProtocol | retry Checkpoint if stats stale, else Retire |
| Segment compact | Retire | heavyIO + retained recoveryProtocol | durable retirement, Done |
| Mapping survey | Start | heavyIO | Done |
| Mapping GC (automatic) | Start/Survey | heavyIO | stop below policy thresholds, otherwise depend on Checkpoint |
| Mapping GC (explicit) | Start | recoveryProtocol | retain recoveryProtocol, depend on Checkpoint |
| Mapping GC | Copy | heavyIO + recoveryProtocol | Publish |
| Mapping GC | Publish | mappingWriter + recoveryProtocol | Cleanup |
| Mapping GC | Cleanup | recoveryProtocol | Done |

The Segment dependency is deliberately asynchronous: `heavyIO` is released
before Checkpoint is enqueued while `recoveryProtocol` remains owned by the
suspended Segment job. Thus Checkpoint cannot wait behind a Segment copy, and
another marker-producing GC cannot enter between copy and retirement.

Publication remains durable-before-visible. Scheduler ownership changes when
a phase may run; it does not weaken Catalog generation validation, Mapping
root validation, mutation draining, reader pins, open-Batch redirects, or
retirement proofs.

## Fairness and backpressure policy

Checkpoint priority is absolute because Delta hard-pressure and GC proof
dependencies require bounded progress. Mapping jobs must not age above
Checkpoint: doing so can delay the operation that releases their own
dependency. Automatic Checkpoint, Segment GC, Mapping GC, and survey requests
use stable coalescing keys, so timer bursts do not grow the queue.

Preemption cancels the running Mapping phase and restarts a fresh worker at
`Start`; it never resumes the canceled phase with private work that cleanup may
already have discarded. Joined cancellation plus cleanup failure is terminal,
not a successful preemption restart.

Explicit Segment requests are intentionally independent and remain attached to
caller contexts; cancellation removes their waiter and cancels otherwise
unowned work. The Scheduler currently does not impose a second arbitrary queue
limit or priority aging policy. Such a limit would require a new public
overload error and could reject the Checkpoint needed to relieve pressure.

`MaintenanceQueueWaitNanos`, `MaintenanceMaxQueueWaitNanos`,
`MaintenanceRunNanos`, `MaintenanceMaxRunNanos`, retries, preemptions, queued,
and running are the admission evidence. Add bounded explicit-request admission
only if soak tests show sustained queue growth or queue wait materially above
phase runtime; add lower-priority aging only if Mapping work starves while the
Checkpoint queue is already converging. Neither condition is assumed from
architecture alone.
