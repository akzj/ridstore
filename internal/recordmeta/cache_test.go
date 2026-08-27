package recordmeta

import (
	"math"
	"sync"
	"testing"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

func TestCacheRejectsUnrepresentableCapacity(t *testing.T) {
	if ValidCapacity(math.MaxUint64) {
		t.Fatal("maximum uint64 capacity must not be representable")
	}
	cache := New(math.MaxUint64)
	if stats := cache.Stats(); stats != (Stats{}) {
		t.Fatalf("invalid capacity created cache: %+v", stats)
	}
}

func TestCacheIsBoundedAndValidatesEntries(t *testing.T) {
	cache := New(64)
	for index := uint32(0); index < 320; index++ {
		addr, err := recordlog.NewVAddr(1, recordlog.SegmentHeaderSize+index*64, 64)
		if err != nil {
			t.Fatal(err)
		}
		cache.Remember(addr, model.ID(index+1), 64)
	}
	stats := cache.Stats()
	if stats.Entries > 64 || stats.Entries == 0 || stats.Evictions == 0 {
		t.Fatalf("stats=%+v", stats)
	}
	invalid, _ := recordlog.NewVAddr(2, recordlog.SegmentHeaderSize, 64)
	cache.Remember(invalid, 0, 64)
	cache.Remember(invalid, 1, 128)
	if _, ok := cache.Lookup(invalid); ok {
		t.Fatal("invalid metadata entered cache")
	}
}

func TestCacheConcurrentRememberAndLookup(t *testing.T) {
	cache := New(256)
	var group sync.WaitGroup
	for worker := uint32(0); worker < 8; worker++ {
		group.Add(1)
		go func(worker uint32) {
			defer group.Done()
			for index := uint32(0); index < 1000; index++ {
				offset := recordlog.SegmentHeaderSize + ((worker*1000+index)%512)*64
				addr, _ := recordlog.NewVAddr(recordlog.SegmentID(worker+1), offset, 64)
				cache.Remember(addr, model.ID(index+1), 64)
				cache.Lookup(addr)
			}
		}(worker)
	}
	group.Wait()
	stats := cache.Stats()
	if stats.Entries > 256 || stats.Hits == 0 || stats.Evictions == 0 {
		t.Fatalf("stats=%+v", stats)
	}
}
