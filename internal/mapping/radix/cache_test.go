package radix

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestNodeCacheCoalescesConcurrentMiss(t *testing.T) {
	cache := newNodeCache(4096)
	addr, err := base.NewMapAddr(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	want := storeformat.MappingNode{Level: 7, Prefix: 11}
	var loads atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			node, err := cache.get(addr, func() (storeformat.MappingNode, int, error) {
				loads.Add(1)
				time.Sleep(20 * time.Millisecond)
				return want, 128, nil
			})
			if err != nil {
				errorsSeen <- err
				return
			}
			if node.Level != want.Level || node.Prefix != want.Prefix {
				errorsSeen <- errors.New("wrong coalesced node")
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d want=1", got)
	}
}

func TestNodeCacheSharesLoadErrorAndAllowsRetry(t *testing.T) {
	cache := newNodeCache(4096)
	addr, _ := base.NewMapAddr(1, 4096)
	wantErr := errors.New("read failed")
	var loads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 2)
	load := func() (storeformat.MappingNode, int, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-release
		return storeformat.MappingNode{}, 0, wantErr
	}
	go func() { _, err := cache.get(addr, load); result <- err }()
	<-started
	go func() { _, err := cache.get(addr, load); result <- err }()
	time.Sleep(10 * time.Millisecond)
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-result; !errors.Is(err, wantErr) {
			t.Fatalf("error=%v", err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("loads=%d want=1", got)
	}
	if _, err := cache.get(addr, func() (storeformat.MappingNode, int, error) {
		loads.Add(1)
		return storeformat.MappingNode{Level: 7}, 128, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("retry loads=%d want=2", got)
	}
}
