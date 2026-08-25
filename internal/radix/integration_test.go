package radix

import (
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

type mapCatalog struct {
	mu    sync.Mutex
	state mapstore.CatalogSnapshot
}

func (c *mapCatalog) SnapshotMapStore() mapstore.CatalogSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state.Clone()
}

func (c *mapCatalog) InstallMapStoreRotation(expect uint64, sealed mapstore.SegmentRef, active, next model.MapSegmentID) (mapstore.CatalogSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state.Generation != expect || c.state.ActiveSegment != sealed.SegmentID || c.state.NextSegment != active || next != active+1 {
		return mapstore.CatalogSnapshot{}, ErrInvalid
	}
	c.state.Generation++
	c.state.SealedSegments = append(c.state.SealedSegments, sealed)
	c.state.ActiveSegment = active
	c.state.NextSegment = next
	return c.state.Clone(), nil
}

func TestTreePersistsThroughMapStoreReopen(t *testing.T) {
	root := t.TempDir()
	catalog := &mapCatalog{state: mapstore.CatalogSnapshot{
		Generation: 1, StoreID: mapstore.StoreID{1}, SegmentSize: 8192, ActiveSegment: 1, NextSegment: 2,
	}}
	if err := mapstore.CreateInitialSegment(root, catalog.state.StoreID, catalog.state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	physical, err := mapstore.Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := Open(physical, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	addr := testDataAddr(t, 7, 64)
	tree, err = tree.Build(1, []Mutation{{ID: 99, Addr: addr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := physical.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := physical.Close(); err != nil {
		t.Fatal(err)
	}
	catalog.mu.Lock()
	catalog.state.Root = tree.Root()
	catalog.state.Covered = 1
	catalog.mu.Unlock()
	physical, err = mapstore.Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer physical.Close()
	tree, err = Open(physical, tree.Root(), 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := tree.Lookup(99)
	if err != nil || !exists || got != addr {
		t.Fatalf("got=%v exists=%v err=%v", got, exists, err)
	}
}
