package engine

import "sync"

// mutationAdmission protects only the append-before-publish window of
// Put-like mutations. It is intentionally independent from maintenance task
// execution; holders must never perform I/O or scan batches while locked.
type mutationAdmission struct{ mu sync.RWMutex }

func (m *mutationAdmission) readLock()    { m.mu.RLock() }
func (m *mutationAdmission) readUnlock()  { m.mu.RUnlock() }
func (m *mutationAdmission) writeLock()   { m.mu.Lock() }
func (m *mutationAdmission) writeUnlock() { m.mu.Unlock() }
