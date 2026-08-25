package radix

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

type memoryNodeStore struct {
	mu      sync.Mutex
	next    uint32
	seq     uint64
	nodes   map[model.MapAddr]mapstore.Node
	reads   map[model.MapAddr]int
	appends [8]int
}

func TestIncrementalBuildMatchesMapModel(t *testing.T) {
	store := newMemoryNodeStore()
	tree, _ := Open(store, 0, 0, 1<<20)
	random := rand.New(rand.NewSource(7))
	want := make(map[model.ID]recordlog.VAddr)
	for revision := model.CommitSeq(1); revision <= 200; revision++ {
		batch := make(map[model.ID]recordlog.VAddr)
		for len(batch) < 8 {
			id := model.ID(random.Uint64() | 1)
			if random.Intn(4) == 0 {
				batch[id] = 0
			} else {
				batch[id] = testDataAddr(t, recordlog.SegmentID(random.Intn(8)+1), uint32(random.Intn(1000)+1)*64)
			}
		}
		mutations := make([]Mutation, 0, len(batch))
		for id, addr := range batch {
			mutations = append(mutations, Mutation{ID: id, Addr: addr})
			if addr == 0 {
				delete(want, id)
			} else {
				want[id] = addr
			}
		}
		var err error
		tree, err = tree.Build(revision, mutations)
		if err != nil {
			t.Fatalf("revision=%d err=%v", revision, err)
		}
		for id, addr := range want {
			got, exists, err := tree.Lookup(id)
			if err != nil || !exists || got != addr {
				t.Fatalf("revision=%d id=%d got=%v want=%v exists=%v err=%v", revision, id, got, addr, exists, err)
			}
		}
	}
}

func newMemoryNodeStore() *memoryNodeStore {
	return &memoryNodeStore{next: mapstore.SegmentHeaderSize, seq: 1, nodes: make(map[model.MapAddr]mapstore.Node), reads: make(map[model.MapAddr]int)}
}

func (s *memoryNodeStore) Append(level uint8, prefix uint64, covered model.CommitSeq, slots [mapstore.NodeSlots]uint64) (model.MapAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	encoded, err := mapstore.EncodeNode(mapstore.NodeBuild{Level: level, NodeSeq: s.seq, Prefix: prefix, CoveredCommitSeq: covered, Slots: slots})
	if err != nil {
		return 0, err
	}
	node, size, err := mapstore.DecodeNode(encoded, uint32(len(encoded)))
	if err != nil {
		return 0, err
	}
	addr, _ := model.NewMapAddr(1, s.next)
	s.nodes[addr] = node
	s.next += size
	s.seq++
	s.appends[level]++
	return addr, nil
}

func (s *memoryNodeStore) Read(addr model.MapAddr) (mapstore.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads[addr]++
	node, ok := s.nodes[addr]
	if !ok {
		return mapstore.Node{}, ErrCorrupt
	}
	return node, nil
}

func testDataAddr(t *testing.T, segment recordlog.SegmentID, offset uint32) recordlog.VAddr {
	t.Helper()
	addr, err := recordlog.NewVAddr(segment, offset, 64)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestBuildLookupUpdateAndPrune(t *testing.T) {
	store := newMemoryNodeStore()
	tree, err := Open(store, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	a := testDataAddr(t, 1, 64)
	b := testDataAddr(t, 1, 128)
	c := testDataAddr(t, 2, 64)
	tree, err = tree.Build(3, []Mutation{{ID: 1, Addr: a}, {ID: 2, Addr: b}, {ID: model.ID(uint64(1) << 63), Addr: c}})
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[model.ID]recordlog.VAddr{1: a, 2: b, model.ID(uint64(1) << 63): c} {
		got, exists, err := tree.Lookup(id)
		if err != nil || !exists || got != want {
			t.Fatalf("id=%d got=%v exists=%v err=%v", id, got, exists, err)
		}
	}
	before := store.appends
	d := testDataAddr(t, 3, 64)
	tree, err = tree.Build(4, []Mutation{{ID: 1, Addr: d}, {ID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for level := range store.appends {
		if store.appends[level]-before[level] != 1 {
			t.Fatalf("level %d rewrites=%d", level, store.appends[level]-before[level])
		}
	}
	if got, exists, _ := tree.Lookup(1); !exists || got != d {
		t.Fatalf("got=%v exists=%v", got, exists)
	}
	if _, exists, _ := tree.Lookup(2); exists {
		t.Fatal("deleted ID remains visible")
	}
	tree, err = tree.Build(5, []Mutation{{ID: 1}, {ID: model.ID(uint64(1) << 63)}})
	if err != nil {
		t.Fatal(err)
	}
	if tree.Root() != 0 {
		t.Fatalf("root=%v", tree.Root())
	}
}

func TestBuildRejectsDuplicateIDAndSequenceRegression(t *testing.T) {
	store := newMemoryNodeStore()
	tree, _ := Open(store, 0, 0, 1<<20)
	addr := testDataAddr(t, 1, 64)
	if _, err := tree.Build(1, []Mutation{{ID: 1, Addr: addr}, {ID: 1, Addr: addr}}); err == nil {
		t.Fatal("duplicate ID accepted")
	}
	tree, err := tree.Build(1, []Mutation{{ID: 1, Addr: addr}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.Build(1, []Mutation{{ID: 2, Addr: addr}}); err == nil {
		t.Fatal("same covered sequence accepted changes")
	}
}

func TestOpenRequiresCacheCapacityForPinnedRoot(t *testing.T) {
	store := newMemoryNodeStore()
	tree, _ := Open(store, 0, 0, 1<<20)
	addr := testDataAddr(t, 1, 64)
	built, err := tree.Build(1, []Mutation{{ID: 1, Addr: addr}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(store, built.Root(), 1, 1); err == nil {
		t.Fatal("undersized root cache accepted")
	}
}

func TestConcurrentLookupLoadsEachPathNodeOnce(t *testing.T) {
	store := newMemoryNodeStore()
	tree, _ := Open(store, 0, 0, 1<<20)
	addr := testDataAddr(t, 1, 64)
	tree, err := tree.Build(1, []Mutation{{ID: 42, Addr: addr}})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			got, exists, err := tree.Lookup(42)
			if err != nil || !exists || got != addr {
				t.Errorf("got=%v exists=%v err=%v", got, exists, err)
			}
		}()
	}
	wait.Wait()
	store.mu.Lock()
	defer store.mu.Unlock()
	var reads int
	for _, count := range store.reads {
		reads += count
		if count != 1 {
			t.Fatalf("duplicate node loads: %+v", store.reads)
		}
	}
	if reads != 8 {
		t.Fatalf("reads=%d map=%+v", reads, store.reads)
	}
}
