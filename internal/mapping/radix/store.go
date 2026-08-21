package radix

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

type nodeStore struct {
	mu          sync.RWMutex
	root        string
	uuid        base.StoreUUID
	segmentSize uint64
	activeID    base.MapSegmentID
	active      *os.File
	activeEnd   uint64
	nextNodeSeq base.NodeSeq
	sealed      map[base.MapSegmentID]sealedMapFile
	closed      bool
}

type sealedMapFile struct {
	file *os.File
	end  uint64
}

func openNodeStore(root string, manifest storeformat.Manifest) (*nodeStore, error) {
	store := &nodeStore{
		root: root, uuid: manifest.StoreUUID, segmentSize: manifest.HardLimits.SegmentSize,
		activeID: manifest.ActiveMapSegmentID, nextNodeSeq: 1, sealed: make(map[base.MapSegmentID]sealedMapFile),
	}
	for _, summary := range manifest.SealedMappingSegments {
		file, err := openMappingFile(root, manifest.StoreUUID, summary, true, manifest.HardLimits.SegmentSize)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		store.sealed[base.MapSegmentID(summary.FileID)] = sealedMapFile{file: file, end: summary.ValidEnd}
		if base.NodeSeq(summary.LastSeq) >= store.nextNodeSeq {
			store.nextNodeSeq = base.NodeSeq(summary.LastSeq) + 1
		}
	}
	activePath := filepath.Join(root, "mapping", activeMapFileName(manifest.ActiveMapSegmentID))
	fd, err := syscall.Open(activePath, syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	store.active = os.NewFile(uintptr(fd), activePath)
	if store.active == nil {
		_ = syscall.Close(fd)
		_ = store.Close()
		return nil, fmt.Errorf("open active mapping segment")
	}
	info, err := store.active.Stat()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	header, err := readMapHeader(store.active)
	if err != nil || header.Kind != storeformat.SegmentKindMapping || header.StoreUUID != manifest.StoreUUID || header.FileID != uint32(manifest.ActiveMapSegmentID) {
		_ = store.Close()
		return nil, errors.Join(err, base.ErrCorrupt)
	}
	validEnd, lastSeq, err := scanNodes(store.active, uint64(info.Size()), manifest.HardLimits.SegmentSize-storeformat.SegmentFooterSize, base.NodeSeq(header.FirstSeq), false)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	if validEnd != uint64(info.Size()) {
		if err := store.active.Truncate(int64(validEnd)); err != nil {
			_ = store.Close()
			return nil, err
		}
		if err := store.active.Sync(); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	store.activeEnd = validEnd
	if lastSeq >= store.nextNodeSeq {
		store.nextNodeSeq = lastSeq + 1
	}
	return store, nil
}

func (s *nodeStore) append(build storeformat.MappingNodeBuild) (base.MapAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, base.ErrClosed
	}
	build.NodeSeq = s.nextNodeSeq
	encoded, err := storeformat.EncodeMappingNode(build)
	if err != nil {
		return 0, err
	}
	if uint64(len(encoded)) > s.segmentSize-storeformat.SegmentFooterSize-s.activeEnd {
		return 0, segment.ErrFull
	}
	offset := s.activeEnd
	if _, err := writeFullAt(s.active, encoded, int64(offset)); err != nil {
		return 0, err
	}
	s.activeEnd += uint64(len(encoded))
	s.nextNodeSeq++
	return base.NewMapAddr(s.activeID, uint32(offset))
}

func (s *nodeStore) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	return s.active.Sync()
}

func (s *nodeStore) read(addr base.MapAddr) (storeformat.MappingNode, int, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return storeformat.MappingNode{}, 0, base.ErrClosed
	}
	var file *os.File
	var end uint64
	if addr.SegmentID() == s.activeID {
		file, end = s.active, s.activeEnd
	} else if sealed, ok := s.sealed[addr.SegmentID()]; ok {
		file, end = sealed.file, sealed.end
	}
	s.mu.RUnlock()
	if file == nil || uint64(addr.Offset())+storeformat.MappingNodeHeaderSize > end {
		return storeformat.MappingNode{}, 0, base.ErrInvalidAddress
	}
	header := make([]byte, storeformat.MappingNodeHeaderSize)
	if _, err := file.ReadAt(header, int64(addr.Offset())); err != nil {
		return storeformat.MappingNode{}, 0, err
	}
	size := uint64(binary.LittleEndian.Uint32(header[12:16]))
	if size < storeformat.MappingNodeHeaderSize || size > end-uint64(addr.Offset()) {
		return storeformat.MappingNode{}, 0, base.ErrCorrupt
	}
	count, err := base.Uint64ToInt(size)
	if err != nil {
		return storeformat.MappingNode{}, 0, err
	}
	encoded := make([]byte, count)
	if _, err := file.ReadAt(encoded, int64(addr.Offset())); err != nil {
		return storeformat.MappingNode{}, 0, err
	}
	return storeformat.DecodeMappingNode(encoded, end-uint64(addr.Offset()))
}

func (s *nodeStore) state() (base.MapSegmentID, base.MapSegmentID, []storeformat.FileSummary) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sealed := make([]storeformat.FileSummary, 0, len(s.sealed))
	for id, item := range s.sealed {
		sealed = append(sealed, storeformat.FileSummary{FileID: uint32(id), ValidEnd: item.end})
	}
	return s.activeID, s.activeID + 1, sealed
}

func (s *nodeStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return base.ErrClosed
	}
	s.closed = true
	active := s.active
	sealed := s.sealed
	s.mu.Unlock()
	var result error
	if active != nil {
		result = errors.Join(result, active.Close())
	}
	for _, item := range sealed {
		result = errors.Join(result, item.file.Close())
	}
	return result
}

func activeMapFileName(id base.MapSegmentID) string { return fmt.Sprintf("MAP-%08d.active", id) }
func sealedMapFileName(id base.MapSegmentID) string { return fmt.Sprintf("MAP-%08d.seg", id) }

func openMappingFile(root string, uuid base.StoreUUID, summary storeformat.FileSummary, sealed bool, segmentSize uint64) (*os.File, error) {
	name := activeMapFileName(base.MapSegmentID(summary.FileID))
	if sealed {
		name = sealedMapFileName(base.MapSegmentID(summary.FileID))
	}
	path := filepath.Join(root, "mapping", name)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	header, err := readMapHeader(file)
	if err != nil || header.Kind != storeformat.SegmentKindMapping || header.StoreUUID != uuid || header.FileID != summary.FileID || header.FirstSeq != summary.FirstSeq {
		return nil, errors.Join(err, file.Close(), base.ErrCorrupt)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if uint64(info.Size()) != summary.ValidEnd+storeformat.SegmentFooterSize || summary.ValidEnd > segmentSize-storeformat.SegmentFooterSize {
		return nil, errors.Join(base.ErrCorrupt, file.Close())
	}
	validEnd, last, err := scanNodes(file, summary.ValidEnd, summary.ValidEnd, base.NodeSeq(summary.FirstSeq), true)
	if err != nil || validEnd != summary.ValidEnd || uint64(last) != summary.LastSeq {
		return nil, errors.Join(err, file.Close(), base.ErrCorrupt)
	}
	footerBytes := make([]byte, storeformat.SegmentFooterSize)
	if _, err := file.ReadAt(footerBytes, int64(summary.ValidEnd)); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	footer, err := storeformat.DecodeMappingSegmentFooter(footerBytes)
	if err != nil || footer.SegmentID != base.MapSegmentID(summary.FileID) || footer.ValidNodeEnd != summary.ValidEnd || uint64(footer.FirstNodeSeq) != summary.FirstSeq || uint64(footer.LastNodeSeq) != summary.LastSeq {
		return nil, errors.Join(err, file.Close(), base.ErrCorrupt)
	}
	return file, nil
}

func readMapHeader(file *os.File) (storeformat.SegmentHeader, error) {
	bytes := make([]byte, storeformat.SegmentHeaderSize)
	if _, err := file.ReadAt(bytes, 0); err != nil {
		return storeformat.SegmentHeader{}, err
	}
	return storeformat.DecodeSegmentHeader(bytes)
}

func scanNodes(file *os.File, physicalEnd, contentLimit uint64, first base.NodeSeq, strict bool) (uint64, base.NodeSeq, error) {
	offset := uint64(storeformat.SegmentHeaderSize)
	var previous base.NodeSeq
	for offset < physicalEnd {
		if physicalEnd-offset < storeformat.MappingNodeHeaderSize {
			if strict {
				return offset, previous, base.ErrCorrupt
			}
			return offset, previous, nil
		}
		header := make([]byte, storeformat.MappingNodeHeaderSize)
		if _, err := file.ReadAt(header, int64(offset)); err != nil {
			return offset, previous, err
		}
		size := uint64(binary.LittleEndian.Uint32(header[12:16]))
		if size < storeformat.MappingNodeHeaderSize || size > contentLimit-offset {
			return offset, previous, base.ErrCorrupt
		}
		if size > physicalEnd-offset {
			if strict {
				return offset, previous, base.ErrCorrupt
			}
			return offset, previous, nil
		}
		count, err := base.Uint64ToInt(size)
		if err != nil {
			return offset, previous, err
		}
		encoded := make([]byte, count)
		if _, err := file.ReadAt(encoded, int64(offset)); err != nil {
			return offset, previous, err
		}
		node, consumed, err := storeformat.DecodeMappingNode(encoded, contentLimit-offset)
		if err != nil || consumed != count || (previous == 0 && node.NodeSeq != first) || (previous != 0 && node.NodeSeq <= previous) {
			return offset, previous, errors.Join(err, base.ErrCorrupt)
		}
		previous = node.NodeSeq
		offset += size
	}
	return offset, previous, nil
}

func writeFullAt(file *os.File, data []byte, offset int64) (int, error) {
	written := 0
	for written < len(data) {
		n, err := file.WriteAt(data[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

type nodeCache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	items    map[base.MapAddr]*list.Element
	lru      *list.List
}

type cacheEntry struct {
	addr base.MapAddr
	node storeformat.MappingNode
	size int64
}

func newNodeCache(capacity int64) *nodeCache {
	return &nodeCache{capacity: capacity, items: make(map[base.MapAddr]*list.Element), lru: list.New()}
}

func (c *nodeCache) get(addr base.MapAddr, load func() (storeformat.MappingNode, int, error)) (storeformat.MappingNode, error) {
	c.mu.Lock()
	if element := c.items[addr]; element != nil {
		c.lru.MoveToFront(element)
		node := element.Value.(cacheEntry).node
		c.mu.Unlock()
		return node, nil
	}
	c.mu.Unlock()
	node, encodedSize, err := load()
	if err != nil {
		return storeformat.MappingNode{}, err
	}
	size := int64(encodedSize) + 128
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.items[addr]; element != nil {
		c.lru.MoveToFront(element)
		return element.Value.(cacheEntry).node, nil
	}
	if size <= c.capacity {
		for c.used+size > c.capacity && c.lru.Len() != 0 {
			last := c.lru.Back()
			entry := last.Value.(cacheEntry)
			delete(c.items, entry.addr)
			c.used -= entry.size
			c.lru.Remove(last)
		}
		entry := cacheEntry{addr: addr, node: node, size: size}
		c.items[addr] = c.lru.PushFront(entry)
		c.used += size
	}
	return node, nil
}
