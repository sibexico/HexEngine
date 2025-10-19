package storage

import (
	"container/list"
	"sync"
)

// ClockProReplacer implements the Clock-Pro cache replacement algorithm
// Clock-Pro is a scan-resistant algorithm that improves upon Clock by tracking
// both recency and frequency. It maintains three "hands" that move through a
// circular list:
//
// 1. Hand_cold: Points to the oldest cold page (candidate for eviction)
// 2. Hand_hot: Points to the oldest hot page (for demoting to cold)
// 3. Hand_test: Points to the oldest test page (for pruning history)
//
// Pages can be in three states:
// - Cold: Recently accessed once (on probation)
// - Hot: Accessed multiple times (protected from eviction)
// - Test: Ghost entry tracking recent evictions
//
// The algorithm adaptively adjusts the sizes of hot and cold regions based
// on the workload, making it effective for both recency and frequency patterns.
type ClockProReplacer struct {
	capacity int // Total cache capacity
	hotMax int // Maximum hot pages (adaptive)
	coldMax int // Maximum cold pages (capacity - hotMax)
	testMax int // Maximum test (ghost) pages

	// Circular list of all pages
	pages *list.List
	pageMap map[uint32]*list.Element

	// The three clock hands
	handCold *list.Element // Points to oldest cold page
	handHot *list.Element // Points to oldest hot page
	handTest *list.Element // Points to oldest test page

	// Statistics
	hotSize int
	coldSize int
	testSize int

	mutex sync.Mutex
}

// clockProEntry represents a page in the Clock-Pro algorithm
type clockProEntry struct {
	frameID uint32
	state clockProState
	refBit bool // Reference bit (accessed recently)
	pinned bool // Is page currently pinned
	testBit bool // Is this a test (ghost) entry
}

// clockProState represents the state of a page in Clock-Pro
type clockProState int

const (
	clockProCold clockProState = iota // Cold page (recent single access)
	clockProHot // Hot page (multiple accesses)
	clockProTest // Test page (ghost entry)
)

// NewClockProReplacer creates a new Clock-Pro cache replacer
func NewClockProReplacer(capacity int) *ClockProReplacer {
	// Initialize with equal hot/cold split, will adapt over time
	hotMax := capacity / 2

	return &ClockProReplacer{
		capacity: capacity,
		hotMax: hotMax,
		coldMax: capacity - hotMax,
		testMax: capacity, // Test entries can equal cache size
		pages: list.New(),
		pageMap: make(map[uint32]*list.Element),
		hotSize: 0,
		coldSize: 0,
		testSize: 0,
	}
}

// Victim selects a page for eviction using the Clock-Pro algorithm
func (cp *ClockProReplacer) Victim() (uint32, bool) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	// Check if we have any evictable pages
	if cp.coldSize == 0 && cp.hotSize == 0 {
		return 0, false
	}

	// Move hand_cold to find an eviction candidate
	attempts := 0
	maxAttempts := cp.pages.Len() + 1

	for cp.coldSize > 0 && attempts < maxAttempts {
		if cp.handCold == nil {
			cp.handCold = cp.pages.Front()
		}
		if cp.handCold == nil {
			break
		}

		entry := cp.handCold.Value.(*clockProEntry)
		next := cp.handCold.Next()
		if next == nil {
			next = cp.pages.Front() // Wrap around
		}

		// Skip hot pages and pinned pages
		if entry.state != clockProCold || entry.pinned {
			cp.handCold = next
			attempts++
			continue
		}

		// Check reference bit
		if entry.refBit {
			// Referenced recently: promote to hot (if space) or clear ref bit
			entry.refBit = false

			if cp.hotSize < cp.hotMax {
				// Promote cold -> hot
				entry.state = clockProHot
				cp.coldSize--
				cp.hotSize++
			}

			cp.handCold = next
			continue
		}

		// Found unpinned cold page with ref bit clear: evict it
		frameID := entry.frameID

		// Convert to test entry (ghost)
		if cp.testSize < cp.testMax {
			entry.state = clockProTest
			entry.testBit = true
			cp.coldSize--
			cp.testSize++
		} else {
			// Remove entirely if test list is full
			cp.pages.Remove(cp.handCold)
			delete(cp.pageMap, frameID)
			cp.coldSize--
		}

		cp.handCold = next
		return frameID, true
	}

	// If no cold pages available, try to demote a hot page
	cp.runHandHot()

	// Try again to find a cold victim
	attempts = 0
	for cp.coldSize > 0 && attempts < maxAttempts {
		if cp.handCold == nil {
			cp.handCold = cp.pages.Front()
		}
		if cp.handCold == nil {
			break
		}

		entry := cp.handCold.Value.(*clockProEntry)
		next := cp.handCold.Next()
		if next == nil {
			next = cp.pages.Front()
		}

		if entry.state != clockProCold || entry.pinned {
			cp.handCold = next
			attempts++
			continue
		}

		if entry.refBit {
			entry.refBit = false
			cp.handCold = next
			continue
		}

		frameID := entry.frameID

		if cp.testSize < cp.testMax {
			entry.state = clockProTest
			entry.testBit = true
			cp.coldSize--
			cp.testSize++
		} else {
			cp.pages.Remove(cp.handCold)
			delete(cp.pageMap, frameID)
			cp.coldSize--
		}

		cp.handCold = next
		return frameID, true
	}

	return 0, false
}

// runHandHot moves hand_hot to demote hot pages to cold
func (cp *ClockProReplacer) runHandHot() {
	if cp.hotSize == 0 {
		return
	}

	// Try to demote at least one hot page
	attempts := 0
	maxAttempts := cp.pages.Len()

	for attempts < maxAttempts && cp.hotSize >= cp.hotMax {
		if cp.handHot == nil {
			cp.handHot = cp.pages.Front()
		}
		if cp.handHot == nil {
			break
		}

		entry := cp.handHot.Value.(*clockProEntry)
		next := cp.handHot.Next()
		if next == nil {
			next = cp.pages.Front()
		}

		if entry.state != clockProHot || entry.pinned {
			cp.handHot = next
			attempts++
			continue
		}

		// Check reference bit
		if entry.refBit {
			// Recently accessed: clear ref bit and keep as hot
			entry.refBit = false
			cp.handHot = next
			attempts++
			continue
		}

		// Demote hot -> cold
		entry.state = clockProCold
		cp.hotSize--
		cp.coldSize++

		cp.handHot = next
		break
	}
}

// runHandTest moves hand_test to prune old test entries
func (cp *ClockProReplacer) runHandTest() {
	if cp.testSize == 0 {
		return
	}

	// Prune test entries until we're under the limit
	for cp.testSize > cp.testMax {
		if cp.handTest == nil {
			cp.handTest = cp.pages.Front()
		}
		if cp.handTest == nil {
			break
		}

		entry := cp.handTest.Value.(*clockProEntry)
		next := cp.handTest.Next()
		if next == nil {
			next = cp.pages.Front()
		}

		if entry.state == clockProTest {
			// Remove test entry
			frameID := entry.frameID
			cp.pages.Remove(cp.handTest)
			delete(cp.pageMap, frameID)
			cp.testSize--
		}

		cp.handTest = next
	}
}

// Pin marks a page as in-use
func (cp *ClockProReplacer) Pin(frameID uint32) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if elem, ok := cp.pageMap[frameID]; ok {
		entry := elem.Value.(*clockProEntry)
		entry.pinned = true
	}
}

// Unpin marks a page as evictable and records the access
func (cp *ClockProReplacer) Unpin(frameID uint32) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	elem, exists := cp.pageMap[frameID]

	if exists {
		entry := elem.Value.(*clockProEntry)
		entry.pinned = false
		entry.refBit = true // Mark as recently accessed

		// If it's a test entry (ghost), it's being re-accessed
		if entry.state == clockProTest {
			// Cache hit on ghost entry: adapt by increasing hot size
			cp.adaptOnTestHit()

			// Convert test -> hot (skip cold probation)
			entry.state = clockProHot
			entry.testBit = false
			cp.testSize--
			cp.hotSize++

			// Ensure we have space
			cp.ensureCapacity()
		}

		return
	}

	// New page: insert as cold
	entry := &clockProEntry{
		frameID: frameID,
		state: clockProCold,
		refBit: false, // Will be set on next access
		pinned: false,
	}

	elem = cp.pages.PushBack(entry)
	cp.pageMap[frameID] = elem
	cp.coldSize++

	// Ensure we have space
	cp.ensureCapacity()
}

// adaptOnTestHit adapts the hot/cold split when a test entry is re-accessed
// This indicates we evicted a page too early, so we increase hot size
func (cp *ClockProReplacer) adaptOnTestHit() {
	// Increase hot region (up to 80% of capacity)
	maxHot := (cp.capacity * 4) / 5
	if cp.hotMax < maxHot {
		cp.hotMax++
		cp.coldMax = cp.capacity - cp.hotMax
	}
}

// ensureCapacity ensures we don't exceed capacity by running victim selection
func (cp *ClockProReplacer) ensureCapacity() {
	// Evict pages if we exceed capacity
	for cp.hotSize+cp.coldSize > cp.capacity {
		// Run hand_hot to demote hot pages if needed
		if cp.hotSize > cp.hotMax {
			cp.runHandHot()
		}

		// Try to evict a cold page
		if cp.coldSize > 0 {
			// Find and evict a cold page
			for e := cp.pages.Front(); e != nil; e = e.Next() {
				entry := e.Value.(*clockProEntry)
				if entry.state == clockProCold && !entry.pinned {
					if !entry.refBit {
						// Convert to test entry
						if cp.testSize < cp.testMax {
							entry.state = clockProTest
							entry.testBit = true
							cp.coldSize--
							cp.testSize++
						} else {
							// Remove entirely
							cp.pages.Remove(e)
							delete(cp.pageMap, entry.frameID)
							cp.coldSize--
						}
						break
					} else {
						entry.refBit = false
					}
				}
			}
		} else {
			// No cold pages, must demote hot
			cp.runHandHot()
		}
	}

	// Prune test entries if needed
	cp.runHandTest()
}

// Size returns the number of evictable pages
func (cp *ClockProReplacer) Size() int {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	count := 0
	for e := cp.pages.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*clockProEntry)
		if !entry.pinned && entry.state != clockProTest {
			count++
		}
	}
	return count
}

// GetStats returns statistics about the Clock-Pro replacer
func (cp *ClockProReplacer) GetStats() ClockProStats {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	return ClockProStats{
		Capacity: cp.capacity,
		HotSize: cp.hotSize,
		HotMax: cp.hotMax,
		ColdSize: cp.coldSize,
		ColdMax: cp.coldMax,
		TestSize: cp.testSize,
		TestMax: cp.testMax,
		TotalPages: cp.hotSize + cp.coldSize,
	}
}

// ClockProStats contains statistics about the Clock-Pro replacer
type ClockProStats struct {
	Capacity int // Total cache capacity
	HotSize int // Current hot pages
	HotMax int // Maximum hot pages (adaptive)
	ColdSize int // Current cold pages
	ColdMax int // Maximum cold pages
	TestSize int // Current test (ghost) entries
	TestMax int // Maximum test entries
	TotalPages int // Total cached pages (hot + cold)
}
