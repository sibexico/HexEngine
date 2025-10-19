package storage

import (
	"container/list"
	"sync"
)

// LRUNode represents a node in the LRU list
type LRUNode struct {
	frameID uint32
}

// LRUReplacer implements LRU (Least Recently Used) replacement policy
type LRUReplacer struct {
	capacity uint32
	lruList *list.List
	lruMap map[uint32]*list.Element
	mutex sync.Mutex
}

// NewLRUReplacer creates a new LRU replacer
func NewLRUReplacer(capacity uint32) *LRUReplacer {
	return &LRUReplacer{
		capacity: capacity,
		lruList: list.New(),
		lruMap: make(map[uint32]*list.Element),
		mutex: sync.Mutex{},
	}
}

// Victim selects a frame to evict using LRU policy
// Returns the frame ID and true if a victim was found, or 0 and false if no victim available
func (lru *LRUReplacer) Victim() (uint32, bool) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	// LRU victim is at the front of the list (oldest)
	if lru.lruList.Len() == 0 {
		return 0, false
	}

	// Get the oldest element (front of list)
	oldest := lru.lruList.Front()
	if oldest == nil {
		return 0, false
	}

	node := oldest.Value.(*LRUNode)
	frameID := node.frameID

	// Remove from both list and map
	lru.lruList.Remove(oldest)
	delete(lru.lruMap, frameID)

	return frameID, true
}

// Pin removes a frame from the LRU replacer
// Called when a page is pinned (in use, not evictable)
func (lru *LRUReplacer) Pin(frameID uint32) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	// If frame is in the replacer, remove it
	if elem, exists := lru.lruMap[frameID]; exists {
		lru.lruList.Remove(elem)
		delete(lru.lruMap, frameID)
	}
}

// Unpin adds a frame to the LRU replacer
// Called when a page is unpinned (available for eviction)
func (lru *LRUReplacer) Unpin(frameID uint32) {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	// If already in replacer, move to back (most recently used)
	if elem, exists := lru.lruMap[frameID]; exists {
		lru.lruList.MoveToBack(elem)
		return
	}

	// Add new frame to back of list (most recently used)
	node := &LRUNode{frameID: frameID}
	elem := lru.lruList.PushBack(node)
	lru.lruMap[frameID] = elem
}

// Size returns the number of evictable frames
func (lru *LRUReplacer) Size() uint32 {
	lru.mutex.Lock()
	defer lru.mutex.Unlock()

	return uint32(lru.lruList.Len())
}
