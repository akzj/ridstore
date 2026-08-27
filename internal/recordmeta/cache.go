// Package recordmeta provides a bounded, process-local cache of immutable Put
// record identity. Cache contents are never recovery or GC authority.
package recordmeta

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

const (
	cacheWays   = uint64(4)
	cacheShards = uint64(64)
)

type Metadata struct {
	RecordID     model.ID
	PhysicalSize uint32
}

type Stats struct {
	Hits, Misses, Entries, Evictions uint64
}

type entry struct {
	addr recordlog.VAddr
	meta Metadata
}

type set struct {
	entries [cacheWays]entry
	next    uint8
}

type shard struct {
	mu   sync.RWMutex
	sets []set
}

type Cache struct {
	shards     [cacheShards]shard
	setCount   uint64
	shardCount uint64
	hits       atomic.Uint64
	misses     atomic.Uint64
	entries    atomic.Uint64
	evictions  atomic.Uint64
}

func New(capacity uint64) *Cache {
	c := &Cache{}
	if !ValidCapacity(capacity) || capacity == 0 {
		return c
	}
	sets := capacity / cacheWays
	if capacity%cacheWays != 0 {
		sets++
	}
	c.setCount = sets
	c.shardCount = min(sets, cacheShards)
	for index := uint64(0); index < c.shardCount; index++ {
		count := sets / c.shardCount
		if index < sets%c.shardCount {
			count++
		}
		c.shards[index].sets = make([]set, int(count))
	}
	return c
}

// ValidCapacity reports whether capacity can be represented by the cache's
// native slice indexes. Zero intentionally represents a disabled cache.
func ValidCapacity(capacity uint64) bool {
	sets := capacity / cacheWays
	if capacity%cacheWays != 0 {
		sets++
	}
	return sets <= uint64(math.MaxInt)/uint64(unsafe.Sizeof(set{}))
}

func (c *Cache) Lookup(addr recordlog.VAddr) (Metadata, bool) {
	if c == nil || c.setCount == 0 {
		return Metadata{}, false
	}
	if !addr.Valid() {
		c.misses.Add(1)
		return Metadata{}, false
	}
	hash := mix(uint64(addr))
	shardIndex := hash % c.shardCount
	current := &c.shards[shardIndex]
	setIndex := (hash / c.shardCount) % uint64(len(current.sets))
	current.mu.RLock()
	selected := &current.sets[setIndex]
	for _, candidate := range selected.entries {
		if candidate.addr == addr {
			meta := candidate.meta
			current.mu.RUnlock()
			c.hits.Add(1)
			return meta, true
		}
	}
	current.mu.RUnlock()
	c.misses.Add(1)
	return Metadata{}, false
}

func (c *Cache) Remember(addr recordlog.VAddr, id model.ID, physicalSize uint32) {
	if c == nil || c.setCount == 0 || id == 0 || !addr.Valid() || !addr.MatchesPhysicalSize(physicalSize) {
		return
	}
	hash := mix(uint64(addr))
	shardIndex := hash % c.shardCount
	current := &c.shards[shardIndex]
	setIndex := (hash / c.shardCount) % uint64(len(current.sets))
	current.mu.Lock()
	selected := &current.sets[setIndex]
	for index := range selected.entries {
		if selected.entries[index].addr == addr {
			selected.entries[index].meta = Metadata{RecordID: id, PhysicalSize: physicalSize}
			current.mu.Unlock()
			return
		}
	}
	for index := range selected.entries {
		if selected.entries[index].addr == 0 {
			selected.entries[index] = entry{addr: addr, meta: Metadata{RecordID: id, PhysicalSize: physicalSize}}
			c.entries.Add(1)
			current.mu.Unlock()
			return
		}
	}
	index := selected.next % uint8(cacheWays)
	selected.entries[index] = entry{addr: addr, meta: Metadata{RecordID: id, PhysicalSize: physicalSize}}
	selected.next = (index + 1) % uint8(cacheWays)
	c.evictions.Add(1)
	current.mu.Unlock()
}

func (c *Cache) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	return Stats{Hits: c.hits.Load(), Misses: c.misses.Load(), Entries: c.entries.Load(), Evictions: c.evictions.Load()}
}

func mix(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ value>>31
}
