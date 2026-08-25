package mapstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type staticCatalog struct{ state CatalogSnapshot }

func (c *staticCatalog) SnapshotMapStore() CatalogSnapshot { return c.state.Clone() }
func (c *staticCatalog) InstallMapStoreRotation(expect uint64, sealed SegmentRef, newActive, next model.MapSegmentID) (CatalogSnapshot, error) {
	if c.state.Generation != expect || c.state.ActiveSegment != sealed.SegmentID || c.state.NextSegment != newActive || next != newActive+1 {
		return CatalogSnapshot{}, ErrInvalid
	}
	c.state.Generation++
	c.state.SealedSegments = append(c.state.SealedSegments, sealed)
	c.state.ActiveSegment = newActive
	c.state.NextSegment = next
	return c.state.Clone(), nil
}

func initialState() CatalogSnapshot {
	return CatalogSnapshot{
		Generation: 1, StoreID: testStoreID(), SegmentSize: 8192,
		ActiveSegment: 1, NextSegment: 2,
	}
}

func TestStoreAppendSyncReadAndOpen(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	var leaf [NodeSlots]uint64
	addr, _ := recordlog.NewVAddr(2, 64, 64)
	leaf[17] = uint64(addr)
	leafAddr, err := store.Append(0, 9, 3, leaf)
	if err != nil {
		t.Fatal(err)
	}
	var rootSlots [NodeSlots]uint64
	rootSlots[0] = uint64(leafAddr)
	rootAddr, err := store.Append(MaxLevel, 0, 3, rootSlots)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	got, err := store.Read(leafAddr)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := got.Lookup(17); !ok || value != uint64(addr) {
		t.Fatalf("value=%d ok=%v", value, ok)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	catalog.state.Root = rootAddr
	catalog.state.Covered = 3
	reopened, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	rootNode, err := reopened.Read(rootAddr)
	if err != nil || rootNode.Level != MaxLevel {
		t.Fatalf("node=%+v err=%v", rootNode, err)
	}
}

func TestStoreRepairsOnlyIncompleteUnpublishedTail(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	var slots [NodeSlots]uint64
	addr, _ := recordlog.NewVAddr(1, 64, 64)
	slots[1] = uint64(addr)
	_, err = store.Append(0, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, mappingDirectory, activeName(1))
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("size after repair=%d want=%d", after.Size(), before.Size())
	}
}

func TestStoreRejectsRootPastIncompleteTail(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, mappingDirectory, activeName(1))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	state.Root, _ = model.NewMapAddr(1, SegmentHeaderSize)
	state.Covered = 1
	if _, err := Open(root, &staticCatalog{state: state}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err=%v", err)
	}
}

func TestStoreRotatesBeforeAssigningNextAddress(t *testing.T) {
	root := t.TempDir()
	state := initialState()
	if err := CreateInitialSegment(root, state.StoreID, state.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &staticCatalog{state: state}
	store, err := Open(root, catalog)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var slots [NodeSlots]uint64
	for index := range slots {
		slots[index], _ = mapValue(index)
	}
	first, err := store.Append(1, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Append(1, 0, 1, slots)
	if err != nil {
		t.Fatal(err)
	}
	if first.SegmentID() != 1 || second.SegmentID() != 2 || catalog.state.ActiveSegment != 2 || len(catalog.state.SealedSegments) != 1 {
		t.Fatalf("first=%v second=%v state=%+v", first, second, catalog.state)
	}
	if _, err := store.Read(first); err != nil {
		t.Fatal(err)
	}
}

func mapValue(index int) (uint64, error) {
	addr, err := model.NewMapAddr(1, SegmentHeaderSize+uint32(index)*8)
	return uint64(addr), err
}
