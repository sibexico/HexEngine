package storage

import (
	"container/list"
	"sync"
)

// ARCReplacer implements the Adaptive Replacement Cache algorithm
// ARC maintains four LRU lists:
// - T1: Recent cache hits (recency)
// - T2: Frequent cache hits (frequency)
// - B1: Ghost entries evicted from T1 (recent evictions)
// - B2: Ghost entries evicted from T2 (frequent evictions)
//
// The algorithm adaptively adjusts the target size (p) between T1 and T2
// based on cache hit patterns to optimize for the current workload.
type ARCReplacer struct {
	capacity int // Total cache capacity (|T1| + |T2| <= c)
	p int // Target size for T1 (adaptive parameter)

	// Cache lists (actual cached pages)
	t1 *list.List // Recent pages (LRU)
	t2 *list.List // Frequent pages (LRU)

	// Ghost lists (track recently evicted pages)
	b1 *list.List // Recently evicted from T1
	b2 *list.List // Recently evicted from T2

	// Fast lookup maps
	t1Map map[uint32]*list.Element
	t2Map map[uint32]*list.Element
	b1Map map[uint32]*list.Element
	b2Map map[uint32]*list.Element

	mutex sync.Mutex
}

// arcEntry represents a cached page in ARC
type arcEntry struct {
	frameID uint32
	pinned bool
}

// NewARCReplacer creates a new ARC cache replacer
func NewARCReplacer(capacity int) *ARCReplacer {
	return &ARCReplacer{
		capacity: capacity,
		p: 0, // Initially favor recency
		t1: list.New(),
		t2: list.New(),
		b1: list.New(),
		b2: list.New(),
		t1Map: make(map[uint32]*list.Element),
		t2Map: make(map[uint32]*list.Element),
		b1Map: make(map[uint32]*list.Element),
		b2Map: make(map[uint32]*list.Element),
	}
}

// Victim selects a page for eviction
func (arc *ARCReplacer) Victim() (uint32, bool) {
	arc.mutex.Lock()
	defer arc.mutex.Unlock()

	// Try to evict from T1 first
	if arc.t1.Len() > 0 {
		for e := arc.t1.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*arcEntry)
			if !entry.pinned {
				frameID := entry.frameID
				arc.t1.Remove(e)
				delete(arc.t1Map, frameID)

				// Move to B1 (ghost list)
				arc.b1Map[frameID] = arc.b1.PushBack(frameID)

				// Maintain B1 size
				if arc.b1.Len() > arc.capacity {
					oldest := arc.b1.Front()
					delete(arc.b1Map, oldest.Value.(uint32))
					arc.b1.Remove(oldest)
				}

				return frameID, true
			}
		}
	}

	// Try to evict from T2
	if arc.t2.Len() > 0 {
		for e := arc.t2.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*arcEntry)
			if !entry.pinned {
				frameID := entry.frameID
				arc.t2.Remove(e)
				delete(arc.t2Map, frameID)

				// Move to B2 (ghost list)
				arc.b2Map[frameID] = arc.b2.PushBack(frameID)

				// Maintain B2 size
				if arc.b2.Len() > arc.capacity {
					oldest := arc.b2.Front()
					delete(arc.b2Map, oldest.Value.(uint32))
					arc.b2.Remove(oldest)
				}

				return frameID, true
			}
		}
	}

	return 0, false
}

// Pin marks a page as in-use
func (arc *ARCReplacer) Pin(frameID uint32) {
	arc.mutex.Lock()
	defer arc.mutex.Unlock()

	// Check T1
	if elem, ok := arc.t1Map[frameID]; ok {
		elem.Value.(*arcEntry).pinned = true
		return
	}

	// Check T2
	if elem, ok := arc.t2Map[frameID]; ok {
		elem.Value.(*arcEntry).pinned = true
		return
	}
}

// Unpin marks a page as evictable and records the access
func (arc *ARCReplacer) Unpin(frameID uint32) {
	arc.mutex.Lock()
	defer arc.mutex.Unlock()

	// Case 1: Page already in T1 (recent cache)
	if elem, ok := arc.t1Map[frameID]; ok {
		entry := elem.Value.(*arcEntry)
		entry.pinned = false

		// On second access, promote from T1 to T2 (frequent)
		arc.t1.Remove(elem)
		delete(arc.t1Map, frameID)
		arc.t2Map[frameID] = arc.t2.PushBack(&arcEntry{frameID: frameID, pinned: false})

		// Maintain cache size
		arc.ensureCapacity()
		return
	}

	// Case 2: Page already in T2 (frequent cache)
	if elem, ok := arc.t2Map[frameID]; ok {
		entry := elem.Value.(*arcEntry)
		entry.pinned = false

		// Move to MRU position in T2
		arc.t2.MoveToBack(elem)
		return
	}

	// Case 3: Page in B1 (ghost - recently evicted from T1)
	if elem, ok := arc.b1Map[frameID]; ok {
		// Cache hit in B1 - adapt p upward (favor recency)
		delta := 1
		if arc.b1.Len() < arc.b2.Len() {
			delta = arc.b2.Len() / arc.b1.Len()
		}
		if delta < 1 {
			delta = 1
		}
		arc.p = min(arc.p+delta, arc.capacity)

		// Remove from B1
		arc.b1.Remove(elem)
		delete(arc.b1Map, frameID)

		// Add directly to T2 (was valuable enough to be remembered)
		arc.t2Map[frameID] = arc.t2.PushBack(&arcEntry{frameID: frameID, pinned: false})

		// Maintain cache size
		arc.ensureCapacity()
		return
	}

	// Case 4: Page in B2 (ghost - recently evicted from T2)
	if elem, ok := arc.b2Map[frameID]; ok {
		// Cache hit in B2 - adapt p downward (favor frequency)
		delta := 1
		if arc.b2.Len() < arc.b1.Len() {
			delta = arc.b1.Len() / arc.b2.Len()
		}
		if delta < 1 {
			delta = 1
		}
		arc.p = max(arc.p-delta, 0)

		// Remove from B2
		arc.b2.Remove(elem)
		delete(arc.b2Map, frameID)

		// Add directly to T2
		arc.t2Map[frameID] = arc.t2.PushBack(&arcEntry{frameID: frameID, pinned: false})

		// Maintain cache size
		arc.ensureCapacity()
		return
	}

	// Case 5: Complete cache miss - add to T1 (first access)
	arc.t1Map[frameID] = arc.t1.PushBack(&arcEntry{frameID: frameID, pinned: false})

	// Maintain cache size
	arc.ensureCapacity()
}

// ensureCapacity maintains cache size constraints
func (arc *ARCReplacer) ensureCapacity() {
	// While cache is over capacity, evict pages
	for arc.t1.Len()+arc.t2.Len() > arc.capacity {
		evicted := false

		// Decide whether to evict from T1 or T2 based on sizes and p
		if arc.t1.Len() > max(1, arc.p) {
			// Evict from T1
			for e := arc.t1.Front(); e != nil; e = e.Next() {
				entry := e.Value.(*arcEntry)
				if !entry.pinned {
					frameID := entry.frameID
					arc.t1.Remove(e)
					delete(arc.t1Map, frameID)

					// Move to B1 ghost list
					arc.b1Map[frameID] = arc.b1.PushBack(frameID)

					// Limit B1 size
					if arc.b1.Len() > arc.capacity {
						oldest := arc.b1.Front()
						delete(arc.b1Map, oldest.Value.(uint32))
						arc.b1.Remove(oldest)
					}

					evicted = true
					break
				}
			}
		} else {
			// Evict from T2
			for e := arc.t2.Front(); e != nil; e = e.Next() {
				entry := e.Value.(*arcEntry)
				if !entry.pinned {
					frameID := entry.frameID
					arc.t2.Remove(e)
					delete(arc.t2Map, frameID)

					// Move to B2 ghost list
					arc.b2Map[frameID] = arc.b2.PushBack(frameID)

					// Limit B2 size
					if arc.b2.Len() > arc.capacity {
						oldest := arc.b2.Front()
						delete(arc.b2Map, oldest.Value.(uint32))
						arc.b2.Remove(oldest)
					}

					evicted = true
					break
				}
			}
		}

		// If we couldn't evict anything (all pinned), break to avoid infinite loop
		if !evicted {
			break
		}
	}
}

// Size returns the number of evictable pages
func (arc *ARCReplacer) Size() uint32 {
	arc.mutex.Lock()
	defer arc.mutex.Unlock()

	count := 0

	// Count unpinned pages in T1
	for e := arc.t1.Front(); e != nil; e = e.Next() {
		if !e.Value.(*arcEntry).pinned {
			count++
		}
	}

	// Count unpinned pages in T2
	for e := arc.t2.Front(); e != nil; e = e.Next() {
		if !e.Value.(*arcEntry).pinned {
			count++
		}
	}

	return uint32(count)
}

// GetStats returns ARC-specific statistics
func (arc *ARCReplacer) GetStats() map[string]int {
	arc.mutex.Lock()
	defer arc.mutex.Unlock()

	return map[string]int{
		"t1_size": arc.t1.Len(),
		"t2_size": arc.t2.Len(),
		"b1_size": arc.b1.Len(),
		"b2_size": arc.b2.Len(),
		"target_p": arc.p,
		"capacity": arc.capacity,
	}
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
