package radix

import (
	"container/list"
	"sync"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

type cacheItem struct {
	addr   model.MapAddr
	node   mapstore.Node
	bytes  uint64
	pinned bool
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
	items    map[model.MapAddr]*list.Element
	loading  map[model.MapAddr]*cacheLoad
	lru      list.List
}

func newNodeCache(capacity uint64) *nodeCache {
	return &nodeCache{capacity: capacity, items: make(map[model.MapAddr]*list.Element), loading: make(map[model.MapAddr]*cacheLoad)}
}

func (c *nodeCache) get(addr model.MapAddr, pin bool, load func() (mapstore.Node, error)) (mapstore.Node, error) {
	c.mu.Lock()
	if element := c.items[addr]; element != nil {
		item := element.Value.(*cacheItem)
		if pin {
			item.pinned = true
		}
		c.lru.MoveToFront(element)
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
			c.items[addr] = c.lru.PushFront(item)
			c.used += weight
			c.evict()
		}
	}
	close(pending.ready)
	c.mu.Unlock()
	return pending.node, pending.err
}

func (c *nodeCache) evict() {
	for c.used > c.capacity {
		var victim *list.Element
		for element := c.lru.Back(); element != nil; element = element.Prev() {
			if !element.Value.(*cacheItem).pinned {
				victim = element
				break
			}
		}
		if victim == nil {
			return
		}
		item := victim.Value.(*cacheItem)
		delete(c.items, item.addr)
		c.lru.Remove(victim)
		c.used -= item.bytes
	}
}

func (c *nodeCache) bytes() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func nodeWeight(node mapstore.Node) uint64 {
	return uint64(mapstore.NodeHeaderSize+mapstore.SparseBitmapBytes) + uint64(len(node.Values))*8 + 64
}
