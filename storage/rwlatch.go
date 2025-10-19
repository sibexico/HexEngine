package storage

import (
	"runtime"
	"sync/atomic"
)

// RWLatch is a lock-free reader-writer latch implementation using atomic counters
// It provides better performance than sync.RWMutex for high-contention scenarios
// by avoiding kernel-level locks and using only CPU atomic instructions.
//
// Implementation based on optimistic spinning and atomic compare-and-swap:
// - Readers: Increment reader counter atomically
// - Writers: Wait for readers to drain, then set writer flag
// - Both: Use exponential backoff spinning to reduce contention
//
// Layout of the 64-bit atomic counter:
//
//	Bits 0-30: Reader count (31 bits, max 2^31-1 concurrent readers)
//	Bit 31: Writer flag (1 = writer active/pending)
//	Bits 32-63: Writer waiting count (for fairness)
const (
	readerMask        uint64 = 0x7FFFFFFF         // Bits 0-30: reader count
	writerFlag        uint64 = 0x80000000         // Bit 31: writer active
	writerWaitingMask uint64 = 0xFFFFFFFF00000000 // Bits 32-63: writers waiting
	writerWaitingInc  uint64 = 0x100000000        // Increment for writer waiting
)

// RWLatch provides lock-free reader-writer synchronization
type RWLatch struct {
	state uint64 // Atomic state: [waiters:32][writer:1][readers:31]
}

// NewRWLatch creates a new lock-free RWLatch
func NewRWLatch() *RWLatch {
	return &RWLatch{state: 0}
}

// RLock acquires a read lock
// Multiple readers can hold the latch simultaneously
func (rw *RWLatch) RLock() {
	backoff := 1
	for {
		state := atomic.LoadUint64(&rw.state)

		// Check if writer is active or waiting
		if state&writerFlag != 0 || state&writerWaitingMask != 0 {
			// Writer active or waiting: spin with backoff
			rw.spin(backoff)
			backoff = rw.increaseBackoff(backoff)
			continue
		}

		// Try to increment reader count
		newState := state + 1
		if atomic.CompareAndSwapUint64(&rw.state, state, newState) {
			return
		}

		// CAS failed: retry with backoff
		rw.spin(backoff)
		backoff = rw.increaseBackoff(backoff)
	}
}

// RUnlock releases a read lock
func (rw *RWLatch) RUnlock() {
	for {
		state := atomic.LoadUint64(&rw.state)
		readerCount := state & readerMask

		if readerCount == 0 {
			// Should not happen: trying to unlock without lock
			panic("RWLatch: RUnlock called without corresponding RLock")
		}

		// Decrement reader count
		newState := state - 1
		if atomic.CompareAndSwapUint64(&rw.state, state, newState) {
			return
		}

		// CAS failed: retry (no backoff needed for unlock)
		runtime.Gosched()
	}
}

// Lock acquires a write lock. Only one writer can hold the latch,
// and no readers can be active.
func (rw *RWLatch) Lock() {
	backoff := 1

	// Announce writer waiting
	for {
		state := atomic.LoadUint64(&rw.state)

		// Check if writer is already active
		if state&writerFlag != 0 {
			rw.spin(backoff)
			backoff = rw.increaseBackoff(backoff)
			continue
		}

		// Increment writer waiting count and set writer flag
		newState := (state + writerWaitingInc) | writerFlag
		if atomic.CompareAndSwapUint64(&rw.state, state, newState) {
			break
		}

		rw.spin(backoff)
		backoff = rw.increaseBackoff(backoff)
	}

	// Wait for all readers to drain
	backoff = 1
	for {
		state := atomic.LoadUint64(&rw.state)
		readerCount := state & readerMask

		if readerCount == 0 {
			// No readers: we have exclusive access
			return
		}

		// Readers still active: spin with backoff
		rw.spin(backoff)
		backoff = rw.increaseBackoff(backoff)
	}
}

// Unlock releases a write lock
func (rw *RWLatch) Unlock() {
	for {
		state := atomic.LoadUint64(&rw.state)

		// Check if writer flag is set
		if state&writerFlag == 0 {
			panic("RWLatch: Unlock called without corresponding Lock")
		}

		// Clear writer flag and decrement writer waiting count
		newState := (state &^ writerFlag) - writerWaitingInc
		if atomic.CompareAndSwapUint64(&rw.state, state, newState) {
			return
		}

		// CAS failed: retry
		runtime.Gosched()
	}
}

// TryRLock attempts to acquire a read lock without blocking
// Returns true if successful, false otherwise
func (rw *RWLatch) TryRLock() bool {
	state := atomic.LoadUint64(&rw.state)

	// Check if writer is active or waiting
	if state&writerFlag != 0 || state&writerWaitingMask != 0 {
		return false
	}

	// Try to increment reader count
	newState := state + 1
	return atomic.CompareAndSwapUint64(&rw.state, state, newState)
}

// TryLock attempts to acquire a write lock without blocking
// Returns true if successful, false otherwise
func (rw *RWLatch) TryLock() bool {
	state := atomic.LoadUint64(&rw.state)

	// Check if writer is already active or readers are present
	if state&writerFlag != 0 || state&readerMask != 0 {
		return false
	}

	// Try to set writer flag
	newState := state | writerFlag | writerWaitingInc
	return atomic.CompareAndSwapUint64(&rw.state, state, newState)
}

// GetReaderCount returns the current number of active readers (for testing)
func (rw *RWLatch) GetReaderCount() uint32 {
	state := atomic.LoadUint64(&rw.state)
	return uint32(state & readerMask)
}

// IsWriterActive returns true if a writer currently holds the latch (for testing)
func (rw *RWLatch) IsWriterActive() bool {
	state := atomic.LoadUint64(&rw.state)
	return state&writerFlag != 0
}

// GetWriterWaitingCount returns the number of writers waiting (for testing)
func (rw *RWLatch) GetWriterWaitingCount() uint32 {
	state := atomic.LoadUint64(&rw.state)
	return uint32((state & writerWaitingMask) >> 32)
}

// spin performs a busy-wait with exponential backoff
func (rw *RWLatch) spin(iterations int) {
	for i := 0; i < iterations; i++ {
		runtime.Gosched() // Yield to other goroutines
	}
}

// increaseBackoff increases the backoff duration exponentially
// with a maximum cap to prevent excessive spinning
func (rw *RWLatch) increaseBackoff(current int) int {
	next := current * 2
	if next > 1024 {
		return 1024 // Cap at 1024 iterations
	}
	return next
}

// GetStats returns statistics about the latch state
func (rw *RWLatch) GetStats() RWLatchStats {
	state := atomic.LoadUint64(&rw.state)
	return RWLatchStats{
		ReaderCount:        uint32(state & readerMask),
		WriterActive:       state&writerFlag != 0,
		WriterWaitingCount: uint32((state & writerWaitingMask) >> 32),
	}
}

// RWLatchStats contains statistics about a RWLatch
type RWLatchStats struct {
	ReaderCount        uint32 // Number of active readers
	WriterActive       bool   // Is a writer active
	WriterWaitingCount uint32 // Number of writers waiting
}

// PageLatch wraps a page with a lock-free RWLatch
// This replaces the sync.RWMutex in the Page struct
type PageLatch struct {
	pageId   uint32
	pinCount int32  // Still atomic, managed separately
	isDirty  uint32 // Atomic bool (0=false, 1=true)
	data     *SlottedPage
	latch    *RWLatch
}

// NewPageLatch creates a new page with lock-free latching
func NewPageLatch(pageId uint32) *PageLatch {
	return &PageLatch{
		pageId:   pageId,
		pinCount: 0,
		isDirty:  0,
		data:     NewSlottedPage(),
		latch:    NewRWLatch(),
	}
}

// GetPageId returns the page ID
func (p *PageLatch) GetPageId() uint32 {
	return p.pageId
}

// GetPinCount returns the pin count (atomic read)
func (p *PageLatch) GetPinCount() int32 {
	return atomic.LoadInt32(&p.pinCount)
}

// IsDirty returns whether the page is dirty (atomic read)
func (p *PageLatch) IsDirty() bool {
	return atomic.LoadUint32(&p.isDirty) != 0
}

// SetDirty sets the dirty flag (atomic write)
func (p *PageLatch) SetDirty(dirty bool) {
	var val uint32
	if dirty {
		val = 1
	}
	atomic.StoreUint32(&p.isDirty, val)
}

// Pin increments the pin count (atomic)
func (p *PageLatch) Pin() {
	atomic.AddInt32(&p.pinCount, 1)
}

// Unpin decrements the pin count (atomic)
func (p *PageLatch) Unpin() {
	for {
		count := atomic.LoadInt32(&p.pinCount)
		if count <= 0 {
			return
		}
		if atomic.CompareAndSwapInt32(&p.pinCount, count, count-1) {
			return
		}
	}
}

// GetData returns the slotted page data
func (p *PageLatch) GetData() *SlottedPage {
	return p.data
}

// RLock acquires a read lock on the page
func (p *PageLatch) RLock() {
	p.latch.RLock()
}

// RUnlock releases a read lock on the page
func (p *PageLatch) RUnlock() {
	p.latch.RUnlock()
}

// WLock acquires a write lock on the page
func (p *PageLatch) WLock() {
	p.latch.Lock()
}

// WUnlock releases a write lock on the page
func (p *PageLatch) WUnlock() {
	p.latch.Unlock()
}

// TryRLock attempts to acquire a read lock without blocking
func (p *PageLatch) TryRLock() bool {
	return p.latch.TryRLock()
}

// TryWLock attempts to acquire a write lock without blocking
func (p *PageLatch) TryWLock() bool {
	return p.latch.TryLock()
}

// GetLatchStats returns latch statistics
func (p *PageLatch) GetLatchStats() RWLatchStats {
	return p.latch.GetStats()
}
