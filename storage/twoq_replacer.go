package storage

import (
	"container/list"
	"sync"
)

// TwoQReplacer implements the 2Q cache replacement algorithm
// 2Q is simpler than ARC but more effective than LRU for many workloads
// It maintains two queues:
// - A1 (Am): First-time access queue (FIFO)
// - A2 (A1in): Frequently accessed queue (LRU)
// Pages graduate from A1 to A2 on second access
type TwoQReplacer struct {
	mu sync.RWMutex

	// A1: Recent first-access queue (FIFO) - "probationary"
	a1 *list.List
	a1Map map[uint32]*list.Element
	a1MaxSize int

	// A2: Frequently accessed queue (LRU) - "protected"
	a2 *list.List
	a2Map map[uint32]*list.Element
	a2MaxSize int

	// Ghost list for A1out (evicted from A1)
	// Tracks pages that were evicted from A1 without second access
	a1out *list.List
	a1outMap map[uint32]*list.Element
	a1outMaxSize int

	capacity int
}

// NewTwoQReplacer creates a new 2Q replacer with the given capacity
// Recommended size ratios (from 2Q paper):
// - A1: 25% of capacity (first-time pages)
// - A2: 75% of capacity (frequent pages)
// - A1out: 50% of capacity (ghost entries)
func NewTwoQReplacer(capacity int) *TwoQReplacer {
	if capacity < 4 {
		capacity = 4 // Minimum size
	}

	a1Size := capacity / 4
	if a1Size < 1 {
		a1Size = 1
	}

	a2Size := capacity - a1Size
	a1outSize := capacity / 2

	return &TwoQReplacer{
		a1: list.New(),
		a1Map: make(map[uint32]*list.Element),
		a1MaxSize: a1Size,
		a2: list.New(),
		a2Map: make(map[uint32]*list.Element),
		a2MaxSize: a2Size,
		a1out: list.New(),
		a1outMap: make(map[uint32]*list.Element),
		a1outMaxSize: a1outSize,
		capacity: capacity,
	}
}

// Pin marks a frame as accessed
func (r *TwoQReplacer) Pin(frameID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if page is in A2 (frequent access)
	if elem, exists := r.a2Map[frameID]; exists {
		// Move to front of A2 (LRU)
		r.a2.MoveToFront(elem)
		return
	}

	// Check if page is in A1 (first access)
	if elem, exists := r.a1Map[frameID]; exists {
		// Second access - promote to A2
		r.a1.Remove(elem)
		delete(r.a1Map, frameID)

		// Add to A2
		r.addToA2(frameID)
		return
	}

	// Check if page was recently evicted from A1 (in ghost list)
	if elem, exists := r.a1outMap[frameID]; exists {
		// Page was accessed again after eviction - add directly to A2
		r.a1out.Remove(elem)
		delete(r.a1outMap, frameID)

		r.addToA2(frameID)
		return
	}

	// First-time access - add to A1
	r.addToA1(frameID)
}

// Unpin removes a frame from consideration
func (r *TwoQReplacer) Unpin(frameID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// In 2Q, we don't actually remove entries on unpin
	// The algorithm only tracks access patterns
	// Pages remain in queues until evicted
}

// Victim selects a frame to evict
func (r *TwoQReplacer) Victim() (uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Evict from A1 first (FIFO - least recently added)
	if r.a1.Len() > 0 {
		elem := r.a1.Back()
		frameID := elem.Value.(uint32)

		r.a1.Remove(elem)
		delete(r.a1Map, frameID)

		// Add to ghost list A1out
		r.addToA1out(frameID)

		return frameID, true
	}

	// If A1 is empty, evict from A2 (LRU - least recently used)
	if r.a2.Len() > 0 {
		elem := r.a2.Back()
		frameID := elem.Value.(uint32)

		r.a2.Remove(elem)
		delete(r.a2Map, frameID)

		return frameID, true
	}

	return 0, false
}

// Size returns the number of frames being tracked
func (r *TwoQReplacer) Size() uint32 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return uint32(len(r.a1Map) + len(r.a2Map))
}

// addToA1 adds a frame to the A1 queue (first-time access)
func (r *TwoQReplacer) addToA1(frameID uint32) {
	// Check if A1 is full
	if r.a1.Len() >= r.a1MaxSize {
		// Evict oldest from A1 to A1out
		elem := r.a1.Back()
		evictedID := elem.Value.(uint32)

		r.a1.Remove(elem)
		delete(r.a1Map, evictedID)

		r.addToA1out(evictedID)
	}

	// Add to front of A1
	elem := r.a1.PushFront(frameID)
	r.a1Map[frameID] = elem
}

// addToA2 adds a frame to the A2 queue (frequent access)
func (r *TwoQReplacer) addToA2(frameID uint32) {
	// Check if A2 is full
	if r.a2.Len() >= r.a2MaxSize {
		// Evict LRU from A2
		elem := r.a2.Back()
		evictedID := elem.Value.(uint32)

		r.a2.Remove(elem)
		delete(r.a2Map, evictedID)
	}

	// Add to front of A2
	elem := r.a2.PushFront(frameID)
	r.a2Map[frameID] = elem
}

// addToA1out adds a frame to the ghost list (evicted from A1)
func (r *TwoQReplacer) addToA1out(frameID uint32) {
	// Check if A1out is full
	if r.a1out.Len() >= r.a1outMaxSize {
		// Remove oldest ghost entry
		elem := r.a1out.Back()
		ghostID := elem.Value.(uint32)

		r.a1out.Remove(elem)
		delete(r.a1outMap, ghostID)
	}

	// Add to ghost list
	elem := r.a1out.PushFront(frameID)
	r.a1outMap[frameID] = elem
}

// Remove explicitly removes a frame from all queues
func (r *TwoQReplacer) Remove(frameID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if elem, exists := r.a1Map[frameID]; exists {
		r.a1.Remove(elem)
		delete(r.a1Map, frameID)
	}

	if elem, exists := r.a2Map[frameID]; exists {
		r.a2.Remove(elem)
		delete(r.a2Map, frameID)
	}

	if elem, exists := r.a1outMap[frameID]; exists {
		r.a1out.Remove(elem)
		delete(r.a1outMap, frameID)
	}
}

// GetStats returns statistics about the 2Q cache
func (r *TwoQReplacer) GetStats() TwoQStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return TwoQStats{
		A1Size: r.a1.Len(),
		A1MaxSize: r.a1MaxSize,
		A2Size: r.a2.Len(),
		A2MaxSize: r.a2MaxSize,
		A1outSize: r.a1out.Len(),
		A1outMaxSize: r.a1outMaxSize,
		TotalPages: len(r.a1Map) + len(r.a2Map),
		Capacity: r.capacity,
	}
}

// TwoQStats contains statistics about the 2Q cache state
type TwoQStats struct {
	A1Size int // Current pages in A1 (probationary)
	A1MaxSize int // Max size of A1
	A2Size int // Current pages in A2 (protected)
	A2MaxSize int // Max size of A2
	A1outSize int // Current ghost entries
	A1outMaxSize int // Max ghost entries
	TotalPages int // Total pages tracked
	Capacity int // Total capacity
}
