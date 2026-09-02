package engine

import (
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/storecatalog"
)

// PublishedState is the immutable logical view used as the common input for
// background COW builders. Hot active-segment/delta state remains outside it.
type PublishedState struct {
	Manifest      storecatalog.Manifest
	Generation    uint64
	MappingRoot   model.MapAddr
	CoveredCommit model.CommitSeq
}

func (s *Store) publishState(manifest storecatalog.Manifest) {
	state := &PublishedState{Manifest: manifest.Clone(), Generation: manifest.Generation, MappingRoot: manifest.MappingRoot, CoveredCommit: manifest.CoveredCommitSeq}
	s.published.Store(state)
}

func (s *Store) PublishedState() *PublishedState {
	if state := s.published.Load(); state != nil {
		copy := *state
		copy.Manifest = state.Manifest.Clone()
		return &copy
	}
	if s.catalog == nil {
		return nil
	}
	manifest := s.catalog.Snapshot()
	s.publishState(manifest)
	return s.PublishedState()
}
