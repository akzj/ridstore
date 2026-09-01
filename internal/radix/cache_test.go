package radix

import (
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

func cacheAddr(t *testing.T, id uint32) model.MapAddr {
	t.Helper()
	addr, err := model.NewMapAddr(1, 64+id*8)
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

func dummyCacheNode() mapstore.Node {
	return mapstore.Node{Values: []uint64{1}}
}

func TestNodeCachePromotesSecondLookupHit(t *testing.T) {
	weight := nodeWeight(dummyCacheNode())
	cache := newNodeCache(weight * 3)
	load := func() (mapstore.Node, error) { return dummyCacheNode(), nil }
	hot := cacheAddr(t, 1)
	if _, err := cache.get(hot, false, true, load); err != nil {
		t.Fatal(err)
	}
	if cache.items[hot].active {
		t.Fatal("first admission should stay inactive")
	}
	if _, err := cache.get(hot, false, true, load); err != nil {
		t.Fatal(err)
	}
	if !cache.items[hot].active {
		t.Fatal("second lookup should promote to active")
	}
	for id := uint32(2); id < 8; id++ {
		addr := cacheAddr(t, id)
		if _, err := cache.get(addr, false, false, load); err != nil {
			t.Fatal(err)
		}
	}
	if cache.items[hot] == nil || !cache.items[hot].active {
		t.Fatal("promoted node was evicted by inactive scan traffic")
	}
}

func TestNodeCacheScanDoesNotPromote(t *testing.T) {
	weight := nodeWeight(dummyCacheNode())
	cache := newNodeCache(weight * 2)
	load := func() (mapstore.Node, error) { return dummyCacheNode(), nil }
	addr := cacheAddr(t, 1)
	if _, err := cache.get(addr, false, false, load); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.get(addr, false, false, load); err != nil {
		t.Fatal(err)
	}
	if cache.items[addr].active {
		t.Fatal("walk/scan hits must not promote")
	}
}

func TestNodeCachePinnedRootSurvivesReclaim(t *testing.T) {
	weight := nodeWeight(dummyCacheNode())
	cache := newNodeCache(weight * 2)
	load := func() (mapstore.Node, error) { return dummyCacheNode(), nil }
	root := cacheAddr(t, 1)
	if _, err := cache.get(root, true, true, load); err != nil {
		t.Fatal(err)
	}
	for id := uint32(2); id < 6; id++ {
		if _, err := cache.get(cacheAddr(t, id), false, false, load); err != nil {
			t.Fatal(err)
		}
	}
	if item := cache.items[root]; item == nil || !item.pinned || !item.active {
		t.Fatal("pinned root must remain on the active list")
	}
}

func TestNodeCacheSingleflight(t *testing.T) {
	weight := nodeWeight(dummyCacheNode())
	cache := newNodeCache(weight)
	addr := cacheAddr(t, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	var loads int
	load := func() (mapstore.Node, error) {
		loads++
		close(started)
		<-release
		return dummyCacheNode(), nil
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := cache.get(addr, false, true, load); err != nil {
			t.Error(err)
		}
	}()
	<-started
	go func() {
		defer wg.Done()
		if _, err := cache.get(addr, false, true, func() (mapstore.Node, error) {
			t.Error("second loader ran")
			return dummyCacheNode(), nil
		}); err != nil {
			t.Error(err)
		}
	}()
	close(release)
	wg.Wait()
	if loads != 1 {
		t.Fatalf("loads=%d", loads)
	}
}
