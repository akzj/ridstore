package catalog

import (
	"fmt"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
)

// Manager is the single in-process owner of Manifest generation and install
// order. Mutators receive the latest file set and may only change their own
// protocol fields.
type Manager struct {
	mu      sync.Mutex
	root    string
	current storeformat.Manifest
}

func New(root string, current storeformat.Manifest) (*Manager, error) {
	if root == "" || current.Generation == 0 {
		return nil, base.ErrInvalidConfig
	}
	return &Manager{root: root, current: clone(current)}, nil
}

func (m *Manager) Snapshot() storeformat.Manifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return clone(m.current)
}

func (m *Manager) Install(expectGeneration uint64, mutate func(*storeformat.Manifest) error) (storeformat.Manifest, error) {
	if mutate == nil {
		return storeformat.Manifest{}, base.ErrInvalidConfig
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectGeneration != 0 && m.current.Generation != expectGeneration {
		return storeformat.Manifest{}, fmt.Errorf("manifest generation changed from %d to %d: %w", expectGeneration, m.current.Generation, base.ErrConflict)
	}
	next := clone(m.current)
	if next.Generation == ^uint64(0) {
		return storeformat.Manifest{}, base.ErrGenerationExhausted
	}
	next.Generation++
	if err := mutate(&next); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := (manifest.Installer{Dir: m.root}).Install(next); err != nil {
		return storeformat.Manifest{}, err
	}
	m.current = clone(next)
	return clone(next), nil
}

func clone(value storeformat.Manifest) storeformat.Manifest {
	value.SealedDataSegments = append([]storeformat.FileSummary(nil), value.SealedDataSegments...)
	value.SealedMappingSegments = append([]storeformat.FileSummary(nil), value.SealedMappingSegments...)
	value.OpenBatchIDsAtCut = append([]base.BatchID(nil), value.OpenBatchIDsAtCut...)
	value.SegmentStats = append([]storeformat.SegmentStatsEntry(nil), value.SegmentStats...)
	return value
}
