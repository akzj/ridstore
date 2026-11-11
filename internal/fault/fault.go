// Package fault is the single injection point for the durable-boundary fault
// points the storage packages expose to their own tests.
//
// Every package used to declare its own FaultPoint/FaultHook pair and thread a
// hook argument through every function that could stop at a boundary. That
// worked, but it put a test mechanism into exported signatures (Install,
// Load, Remove, Open ...) and duplicated the same plumbing six times.
//
// A fault point is now just a name. Packages call Hit at the boundary; tests
// install one hook for the process and decide which names to fail on. When no
// hook is installed Hit is a nil-returning call, so the production path costs
// one atomic load.
//
// The hook is process-wide on purpose: durable boundaries are shared by every
// store in the process, and a test that injects at one boundary wants the
// whole call chain to see it. Tests that install a hook must not run in
// parallel with others that do.
package fault

import (
	"sync"
	"sync/atomic"
)

// Point names one durable or recovery boundary. Names are namespaced by
// package ("recordlog.segment.before-header-write") so a single hook can
// discriminate between them.
type Point string

// Hook is asked at every reached boundary. Returning a non-nil error makes the
// operation fail with that error, exactly as a real failure at that boundary
// would.
type Hook func(Point) error

// hookState pairs the installed hook with the points it has already failed on.
// A point fails at most once per installation: the operation that trips the
// boundary reports the injected error, and every later attempt at that same
// boundary sees a healthy drive. That is what the removed per-store hooks
// gave tests for free - a fault belonged to the attempt that hit it - while
// still letting one hook inject at several distinct boundaries of a single
// operation.
type hookState struct {
	hook   Hook
	failed sync.Map
}

var installed atomic.Pointer[hookState]

// SetHook installs hook for the process and returns the hook it replaced, so
// a test can restore the previous state with defer SetHook(previous).
// Passing nil clears the hook.
func SetHook(hook Hook) Hook {
	previous := installed.Load()
	var next *hookState
	if hook != nil {
		next = &hookState{hook: hook}
	}
	installed.Store(next)
	if previous == nil {
		return nil
	}
	return previous.hook
}

// Hit reaches a boundary. It returns nil unless a hook is installed.
func Hit(point Point) error {
	state := installed.Load()
	if state == nil {
		return nil
	}
	if _, fired := state.failed.Load(point); fired {
		return nil
	}
	err := state.hook(point)
	if err != nil {
		state.failed.Store(point, struct{}{})
	}
	return err
}

// Enabled reports whether a hook is installed, letting callers skip work that
// only exists to feed the hook.
func Enabled() bool {
	return installed.Load() != nil
}
