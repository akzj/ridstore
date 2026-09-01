package radix

import (
	"container/list"
	"sync"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

// Two-list LRU modeled on Linux's inactive/active page cache:
// first admission stays on the inactive list; a promoting hit moves
// the node to active. Reclaim prefers inactive so a tree Walk cannot
// evict the Lookup working set. Root nodes are pinned on active.
type cacheItem struct {
	addr    model.MapAddr
	node    mapstore.Node
	bytes   uint64
	pinned  bool
	active  bool
	element *list.Element
}

type cacheLoad struct {
	ready chan struct{}
	node  mapstore.Node
	err   error
}

type nodeCache struct {
	mu       sync.Mutex
	capacity uint64
	used     uint64
	activeN  uint64
	items    map[model.MapAddr]*cacheItem
	loading  map[model.MapAddr]*cacheLoad
	inactive list.List
	active   list.List
}

func newNodeCache(capacity uint64) *nodeCache {
	return &nodeCache{capacity: capacity, items: make(map[model.MapAddr]*cacheItem), loading: make(map[model.MapAddr]*cacheLoad)}
}

func (c *nodeCache) get(addr model.MapAddr, pin, promote bool, load func() (mapstore.Node, error)) (mapstore.Node, error) {
	c.mu.Lock()
	if item := c.items[addr]; item != nil {
		if pin {
			item.pinned = true
			c.moveLocked(item, true)
		} else if promote {
			c.moveLocked(item, true)
		}
		node := item.node
		c.mu.Unlock()
		return node, nil
	}
	if pending := c.loading[addr]; pending != nil {
		ready := pending.ready
		c.mu.Unlock()
		<-ready
		return pending.node, pending.err
	}
	pending := &cacheLoad{ready: make(chan struct{})}
	c.loading[addr] = pending
	c.mu.Unlock()

	pending.node, pending.err = load()
	c.mu.Lock()
	delete(c.loading, addr)
	if pending.err == nil {
		weight := nodeWeight(pending.node)
		if pin && weight > c.capacity {
			pending.err = ErrInvalid
		} else if weight <= c.capacity {
			item := &cacheItem{addr: addr, node: pending.node, bytes: weight, pinned: pin}
			c.items[addr] = item
			c.used += weight
			c.insertLocked(item, pin)
			c.balanceActiveLocked()
			c.evict()
		}
	}
	close(pending.ready)
	c.mu.Unlock()
	return pending.node, pending.err
}

func (c *nodeCache) insertLocked(item *cacheItem, active bool) {
	if active {
		item.active = true
		item.element = c.active.PushFront(item)
		c.activeN += item.bytes
		return
	}
	item.active = false
	item.element = c.inactive.PushFront(item)
}

func (c *nodeCache) moveLocked(item *cacheItem, active bool) {
	if item.active == active {
		if active {
			c.active.MoveToFront(item.element)
			return
		}
		c.inactive.MoveToFront(item.element)
		return
	}
	if item.active {
		c.active.Remove(item.element)
		c.activeN -= item.bytes
	} else {
		c.inactive.Remove(item.element)
	}
	c.insertLocked(item, active)
}

func (c *nodeCache) balanceActiveLocked() {
	limit := c.capacity - c.capacity/3
	for c.activeN > limit {
		item := c.unpinnedTail(&c.active)
		if item == nil {
			return
		}
		c.moveLocked(item, false)
	}
}

func (c *nodeCache) evict() {
	for c.used > c.capacity {
		item := c.unpinnedTail(&c.inactive)
		if item == nil {
			c.balanceActiveLocked()
			item = c.unpinnedTail(&c.inactive)
		}
		if item == nil {
			return
		}
		c.removeLocked(item)
	}
}

func (c *nodeCache) unpinnedTail(chain *list.List) *cacheItem {
	for element := chain.Back(); element != nil; element = element.Prev() {
		item := element.Value.(*cacheItem)
		if !item.pinned {
			return item
		}
	}
	return nil
}

func (c *nodeCache) removeLocked(item *cacheItem) {
	if item.active {
		c.active.Remove(item.element)
		c.activeN -= item.bytes
	} else {
		c.inactive.Remove(item.element)
	}
	delete(c.items, item.addr)
	c.used -= item.bytes
	item.element = nil
}

func (c *nodeCache) bytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func nodeWeight(node mapstore.Node) uint64 {
	return uint64(mapstore.NodeHeaderSize+mapstore.SparseBitmapBytes) + uint64(len(node.Values))*8 + 64
}
