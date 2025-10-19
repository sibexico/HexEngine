package storage

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

// LockFreeSkipList implements a lock-free skip list using CAS operations
// Based on the algorithm by Herlihy, Lev, Luchangco, and Shavit
type LockFreeSkipList struct {
	head     *skipNode
	maxLevel int32
	randSeed uint64
	length   int64 // Approximate length (atomic)
}

// skipNode represents a node in the skip list
type skipNode struct {
	key    []byte
	value  []byte
	level  int32
	next   []atomic.Pointer[skipNode] // Forward pointers at each level
	marked atomic.Bool                // Logical deletion marker
}

// SkipListConfig configures the skip list
type SkipListConfig struct {
	MaxLevel int32   // Maximum number of levels (default: 16)
	P        float64 // Probability for level generation (default: 0.5)
}

// DefaultSkipListConfig returns default configuration
func DefaultSkipListConfig() SkipListConfig {
	return SkipListConfig{
		MaxLevel: 16,
		P:        0.5,
	}
}

// NewLockFreeSkipList creates a new lock-free skip list
func NewLockFreeSkipList(config SkipListConfig) *LockFreeSkipList {
	if config.MaxLevel <= 0 {
		config = DefaultSkipListConfig()
	}

	// Create sentinel head node with max level
	head := &skipNode{
		key:   nil, // Sentinel has no key
		value: nil,
		level: config.MaxLevel,
		next:  make([]atomic.Pointer[skipNode], config.MaxLevel),
	}

	return &LockFreeSkipList{
		head:     head,
		maxLevel: config.MaxLevel,
		randSeed: uint64(rand.Int63()),
	}
}

// randomLevel generates a random level using geometric distribution
func (sl *LockFreeSkipList) randomLevel() int32 {
	level := int32(1)

	// Use XORShift for fast random number generation
	seed := atomic.LoadUint64(&sl.randSeed)
	seed ^= seed << 13
	seed ^= seed >> 7
	seed ^= seed << 17
	atomic.StoreUint64(&sl.randSeed, seed)

	// Geometric distribution: P(level=k) = (1-p)^(k-1) * p
	for level < sl.maxLevel && (seed&0xFF) < 128 { // p = 0.5
		level++
		seed >>= 8
	}

	return level
}

// compareKeys compares two byte slices
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

// find locates the position for a key, returning predecessors and successors at each level
func (sl *LockFreeSkipList) find(key []byte, preds *[]*skipNode, succs *[]*skipNode) int32 {
	var pred, curr, succ *skipNode
	var level int32

retry:
	pred = sl.head

	// Traverse from top level to bottom
	for level = sl.maxLevel - 1; level >= 0; level-- {
		curr = pred.next[level].Load()

		// Move forward at current level
		for curr != nil {
			succ = curr.next[level].Load()

			// Check if node is marked for deletion
			if curr.marked.Load() {
				// Help remove marked node
				if !pred.next[level].CompareAndSwap(curr, succ) {
					goto retry
				}
				curr = succ
			} else {
				cmp := compareKeys(curr.key, key)
				if cmp < 0 {
					pred = curr
					curr = succ
				} else {
					break
				}
			}
		}

		if preds != nil {
			(*preds)[level] = pred
		}
		if succs != nil {
			(*succs)[level] = curr
		}
	}

	// Return level where key was found (or -1 if not found)
	if curr != nil && compareKeys(curr.key, key) == 0 {
		return curr.level
	}
	return -1
}

// Search looks up a key in the skip list
func (sl *LockFreeSkipList) Search(key []byte) ([]byte, bool) {
	preds := make([]*skipNode, sl.maxLevel)
	succs := make([]*skipNode, sl.maxLevel)

	level := sl.find(key, &preds, &succs)

	if level >= 0 && succs[0] != nil {
		node := succs[0]
		if !node.marked.Load() {
			return node.value, true
		}
	}

	return nil, false
}

// Insert adds a key-value pair to the skip list
func (sl *LockFreeSkipList) Insert(key []byte, value []byte) bool {
	preds := make([]*skipNode, sl.maxLevel)
	succs := make([]*skipNode, sl.maxLevel)

	for {
		level := sl.find(key, &preds, &succs)

		// Key already exists
		if level >= 0 {
			node := succs[0]
			if !node.marked.Load() {
				// Update value atomically
				// In a real implementation, we'd use atomic pointer swap
				// For now, just return false to indicate no insertion
				return false
			}
			// Node is marked, retry
			continue
		}

		// Create new node with random level
		newLevel := sl.randomLevel()
		newNode := &skipNode{
			key:   append([]byte(nil), key...),   // Copy key
			value: append([]byte(nil), value...), // Copy value
			level: newLevel,
			next:  make([]atomic.Pointer[skipNode], newLevel),
		}

		// Link new node at level 0 first
		newNode.next[0].Store(succs[0])
		if !preds[0].next[0].CompareAndSwap(succs[0], newNode) {
			continue // Retry on failure
		}

		// Link at higher levels
		for i := int32(1); i < newLevel; i++ {
			for {
				pred := preds[i]
				succ := succs[i]
				newNode.next[i].Store(succ)

				if pred.next[i].CompareAndSwap(succ, newNode) {
					break
				}

				// Retry find if CAS failed
				sl.find(key, &preds, &succs)
			}
		}

		atomic.AddInt64(&sl.length, 1)
		return true
	}
}

// Delete removes a key from the skip list
func (sl *LockFreeSkipList) Delete(key []byte) bool {
	preds := make([]*skipNode, sl.maxLevel)
	succs := make([]*skipNode, sl.maxLevel)

	var node *skipNode

	// Find the node to delete
	for {
		level := sl.find(key, &preds, &succs)

		if level < 0 {
			return false // Key not found
		}

		node = succs[0]
		if node == nil || node.marked.Load() {
			return false // Already deleted
		}

		// Try to mark node for logical deletion
		if node.marked.CompareAndSwap(false, true) {
			break // Successfully marked
		}
		// If CAS failed, retry find (someone else might have deleted it)
	}

	// Physical deletion: try to unlink from level 0
	// Best-effort removal; find() will help clean up marked nodes
	for i := int32(0); i < node.level && i < sl.maxLevel; i++ {
		succ := node.next[i].Load()
		// Try to swing predecessor's pointer
		preds[i].next[i].CompareAndSwap(node, succ)
	}

	atomic.AddInt64(&sl.length, -1)
	return true
}

// Length returns the approximate number of elements
func (sl *LockFreeSkipList) Length() int64 {
	return atomic.LoadInt64(&sl.length)
}

// Range iterates over keys in sorted order within [start, end].
// Snapshot iteration may miss concurrent updates.
func (sl *LockFreeSkipList) Range(start, end []byte, callback func(key, value []byte) bool) {
	// Start from bottom level
	curr := sl.head.next[0].Load()

	// Skip to start key
	for curr != nil && compareKeys(curr.key, start) < 0 {
		curr = curr.next[0].Load()
	}

	// Iterate until end key
	for curr != nil {
		if compareKeys(curr.key, end) > 0 {
			break
		}

		if !curr.marked.Load() {
			if !callback(curr.key, curr.value) {
				break
			}
		}

		curr = curr.next[0].Load()
	}
}

// RangeScan returns all key-value pairs in [start, end]
func (sl *LockFreeSkipList) RangeScan(start, end []byte) []KeyValue {
	results := make([]KeyValue, 0)

	sl.Range(start, end, func(key, value []byte) bool {
		results = append(results, KeyValue{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), value...),
		})
		return true
	})

	return results
}

// KeyValue represents a key-value pair
type KeyValue struct {
	Key   []byte
	Value []byte
}

// Stats returns skip list statistics
func (sl *LockFreeSkipList) Stats() SkipListStats {
	var stats SkipListStats
	stats.Length = atomic.LoadInt64(&sl.length)
	stats.MaxLevel = sl.maxLevel

	// Count nodes at each level
	levelCounts := make([]int32, sl.maxLevel)

	curr := sl.head.next[0].Load()
	for curr != nil {
		if !curr.marked.Load() {
			for i := int32(0); i < curr.level; i++ {
				levelCounts[i]++
			}
		}
		curr = curr.next[0].Load()
	}

	stats.LevelCounts = levelCounts
	return stats
}

// SkipListStats contains skip list statistics
type SkipListStats struct {
	Length      int64
	MaxLevel    int32
	LevelCounts []int32
}

// Clear removes all elements (not lock-free, for testing only)
func (sl *LockFreeSkipList) Clear() {
	// Reset head pointers
	for i := int32(0); i < sl.maxLevel; i++ {
		sl.head.next[i].Store(nil)
	}
	atomic.StoreInt64(&sl.length, 0)
}

// MinKey returns the smallest key (if any)
func (sl *LockFreeSkipList) MinKey() ([]byte, bool) {
	curr := sl.head.next[0].Load()

	for curr != nil {
		if !curr.marked.Load() {
			return curr.key, true
		}
		curr = curr.next[0].Load()
	}

	return nil, false
}

// MaxKey returns the largest key (if any)
func (sl *LockFreeSkipList) MaxKey() ([]byte, bool) {
	var last *skipNode

	curr := sl.head.next[0].Load()
	for curr != nil {
		if !curr.marked.Load() {
			last = curr
		}
		curr = curr.next[0].Load()
	}

	if last != nil {
		return last.key, true
	}
	return nil, false
}

// CompactMemory helps GC by clearing deleted nodes (not lock-free)
func (sl *LockFreeSkipList) CompactMemory() int {
	removed := 0

	// Walk through level 0 and physically remove marked nodes
	for level := int32(0); level < sl.maxLevel; level++ {
		pred := sl.head
		curr := pred.next[level].Load()

		for curr != nil {
			if curr.marked.Load() {
				succ := curr.next[level].Load()
				pred.next[level].Store(succ)
				curr = succ
				if level == 0 {
					removed++
				}
			} else {
				pred = curr
				curr = pred.next[level].Load()
			}
		}
	}

	return removed
}

// validatePointers checks for nil pointer dereferences (for debugging)
func (sl *LockFreeSkipList) validatePointers() bool {
	if sl.head == nil {
		return false
	}

	for level := int32(0); level < sl.maxLevel; level++ {
		curr := sl.head.next[level].Load()
		for curr != nil {
			if curr.next == nil {
				return false
			}
			curr = curr.next[level].Load()
		}
	}

	return true
}

// unsafeGetPointer is a helper to get raw pointer value (for internal use)
func unsafeGetPointer(node *skipNode) uintptr {
	return uintptr(unsafe.Pointer(node))
}
