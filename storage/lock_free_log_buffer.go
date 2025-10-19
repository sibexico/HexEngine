package storage

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

// LockFreeLogBuffer is a lock-free circular buffer for WAL records
// Uses CAS operations for thread-safe concurrent append without blocking
//
// Design:
// - Fixed-size circular buffer with power-of-2 capacity
// - Two atomic counters: head (read position) and tail (write position)
// - Writers reserve space via atomic tail increment
// - Readers wait for writers to finish via sequence numbers
// - Supports concurrent appends with linearizability
//
// Memory Layout:
// Each slot: [sequence(8) | lsn(8) | size(4) | data(var)]
type LockFreeLogBuffer struct {
	// Buffer storage
	buffer []byte
	slots  []logSlot

	// Atomic counters (must be 64-bit aligned)
	head     atomic.Uint64 // Read position
	tail     atomic.Uint64 // Write position
	capacity uint64        // Buffer capacity (power of 2)
	slotMask uint64        // capacity - 1 for fast modulo

	// Configuration
	slotSize    int  // Size of each log slot
	flushOnFull bool // Auto-flush when buffer is full

	// Statistics
	appends      atomic.Uint64
	reads        atomic.Uint64
	wraps        atomic.Uint64 // Times buffer wrapped around
	contentions  atomic.Uint64 // Failed CAS attempts
	maxOccupancy atomic.Uint64 // Maximum buffer occupancy observed
}

// logSlot represents a single slot in the circular buffer
// Each slot contains one log record with metadata
type logSlot struct {
	sequence atomic.Uint64 // Sequence number for synchronization
	lsn      uint64        // Log sequence number
	size     uint32        // Size of data
	data     []byte        // Log record data
}

const (
	// DefaultSlotSize is the default size for each log slot (16KB)
	DefaultSlotSize = 16 * 1024

	// DefaultLogBufferCapacity is the number of slots (must be power of 2)
	DefaultLogBufferCapacity = 256 // 256 * 16KB = 4MB total

	// MinLogBufferCapacity is the minimum buffer capacity
	MinLogBufferCapacity = 16

	// MaxLogBufferCapacity is the maximum buffer capacity
	MaxLogBufferCapacity = 16384 // 16K slots * 16KB = 256MB
)

// LogBufferConfig contains configuration for the lock-free log buffer
type LogBufferConfig struct {
	SlotSize    int  // Size of each slot in bytes
	Capacity    int  // Number of slots (must be power of 2)
	FlushOnFull bool // Auto-flush when buffer is full
}

// DefaultLogBufferConfig returns default configuration
func DefaultLogBufferConfig() LogBufferConfig {
	return LogBufferConfig{
		SlotSize:    DefaultSlotSize,
		Capacity:    DefaultLogBufferCapacity,
		FlushOnFull: true,
	}
}

// NewLockFreeLogBuffer creates a new lock-free log buffer
func NewLockFreeLogBuffer(config LogBufferConfig) (*LockFreeLogBuffer, error) {
	// Validate and normalize capacity to power of 2
	if config.Capacity < MinLogBufferCapacity {
		config.Capacity = MinLogBufferCapacity
	}
	if config.Capacity > MaxLogBufferCapacity {
		config.Capacity = MaxLogBufferCapacity
	}

	// Round up to next power of 2
	capacity := nextPowerOf2(uint64(config.Capacity))

	// Validate slot size
	if config.SlotSize < 1024 || config.SlotSize > 1024*1024 {
		return nil, fmt.Errorf("slot size must be between 1KB and 1MB")
	}

	lb := &LockFreeLogBuffer{
		buffer:      make([]byte, capacity*uint64(config.SlotSize)),
		slots:       make([]logSlot, capacity),
		capacity:    capacity,
		slotMask:    capacity - 1,
		slotSize:    config.SlotSize,
		flushOnFull: config.FlushOnFull,
	}

	// Initialize slots
	for i := uint64(0); i < capacity; i++ {
		lb.slots[i].sequence.Store(i) // Initially, sequence = slot index
		lb.slots[i].data = make([]byte, 0, config.SlotSize)
	}

	// Initialize counters
	lb.head.Store(0)
	lb.tail.Store(0)

	return lb, nil
}

// Append adds a log record to the buffer using lock-free CAS.
// Returns (slotIndex, sequence, error).
func (lb *LockFreeLogBuffer) Append(lsn uint64, data []byte) (uint64, uint64, error) {
	if len(data) > lb.slotSize-16 { // Reserve space for metadata
		return 0, 0, fmt.Errorf("record size %d exceeds slot size %d", len(data), lb.slotSize-16)
	}

	var attempts uint64
	maxAttempts := uint64(1000) // Prevent infinite loop

	for {
		attempts++
		if attempts > maxAttempts {
			return 0, 0, fmt.Errorf("failed to append after %d attempts", maxAttempts)
		}

		// Reserve a slot by atomically incrementing tail
		tailPos := lb.tail.Load()
		slotIndex := tailPos & lb.slotMask
		sequence := tailPos

		// Check if buffer is full
		headPos := lb.head.Load()
		if tailPos >= headPos+lb.capacity {
			// Buffer is full
			if lb.flushOnFull {
				// Wait briefly for reader to catch up
				time.Sleep(time.Microsecond)
				lb.contentions.Add(1)
				continue
			}
			return 0, 0, fmt.Errorf("buffer full: tail=%d head=%d", tailPos, headPos)
		}

		// Try to reserve this slot
		if !lb.tail.CompareAndSwap(tailPos, tailPos+1) {
			// Another thread won, retry
			lb.contentions.Add(1)
			continue
		}

		// We reserved the slot, now fill it
		slot := &lb.slots[slotIndex]

		// Wait for slot to be available
		// The slot's sequence should be exactly divisible by capacity and equal to our position modulo capacity
		// For a slot to be reusable, its sequence must be >= sequence (meaning it's been read)
		expectedSeq := (sequence / lb.capacity) * lb.capacity
		spinCount := 0
		maxSpins := 10000
		for {
			currentSeq := slot.sequence.Load()
			// Slot is available if its sequence is at least expectedSeq
			if currentSeq >= expectedSeq {
				break
			}
			spinCount++
			if spinCount > maxSpins {
				// Slot not ready, buffer likely full and not draining
				return 0, 0, fmt.Errorf("slot %d not available: expected seq >= %d, got %d (attempts: %d)",
					slotIndex, expectedSeq, currentSeq, attempts)
			}
			// Yield to allow other threads to progress
			time.Sleep(time.Nanosecond * 10)
		}

		// Fill the slot
		slot.lsn = lsn
		slot.size = uint32(len(data))
		slot.data = slot.data[:0]
		slot.data = append(slot.data, data...)

		// Publish the record by setting sequence to mark it as written
		// Reader will wait for sequence to be > their expected value
		slot.sequence.Store(sequence + lb.capacity)

		// Update statistics
		lb.appends.Add(1)

		// Track maximum occupancy
		occupancy := tailPos - headPos + 1
		for {
			current := lb.maxOccupancy.Load()
			if occupancy <= current {
				break
			}
			if lb.maxOccupancy.CompareAndSwap(current, occupancy) {
				break
			}
		}

		// Check if we wrapped around
		if tailPos > 0 && slotIndex == 0 {
			lb.wraps.Add(1)
		}

		return slotIndex, sequence, nil
	}
}

// Read reads the next available log record
// Returns (slotIndex, lsn, data, error)
// Returns nil data when no records available
func (lb *LockFreeLogBuffer) Read() (uint64, uint64, []byte, error) {
	headPos := lb.head.Load()
	tailPos := lb.tail.Load()

	// Check if buffer is empty
	if headPos >= tailPos {
		return 0, 0, nil, nil // No data available
	}

	slotIndex := headPos & lb.slotMask
	slot := &lb.slots[slotIndex]

	// Wait for writer to finish
	// The slot's sequence must be > headPos (writer sets it to headPos + capacity)
	expectedMinSeq := headPos + lb.capacity
	spinCount := 0
	maxSpins := 100000
	for slot.sequence.Load() < expectedMinSeq {
		spinCount++
		if spinCount > maxSpins {
			// Writer hasn't finished yet
			return 0, 0, nil, nil
		}
		time.Sleep(time.Nanosecond * 10)
	}

	// Read the slot
	lsn := slot.lsn
	size := slot.size
	data := make([]byte, size)
	copy(data, slot.data[:size])

	// Mark slot as read by updating sequence to allow reuse
	// Next writer will see sequence >= (next_wrap * capacity)
	slot.sequence.Store(headPos + 2*lb.capacity)

	// Advance head
	lb.head.Store(headPos + 1)

	// Update statistics
	lb.reads.Add(1)

	return slotIndex, lsn, data, nil
}

// ReadBatch reads multiple records in a batch (up to maxRecords)
// Returns slice of (lsn, data) tuples
func (lb *LockFreeLogBuffer) ReadBatch(maxRecords int) ([]LogBufferRecord, error) {
	records := make([]LogBufferRecord, 0, maxRecords)

	for i := 0; i < maxRecords; i++ {
		_, lsn, data, err := lb.Read()
		if err != nil {
			return records, err
		}
		if data == nil {
			break // No more data
		}
		records = append(records, LogBufferRecord{LSN: lsn, Data: data})
	}

	return records, nil
}

// LogBufferRecord represents a single record from the buffer
type LogBufferRecord struct {
	LSN  uint64
	Data []byte
}

// Available returns the number of records available to read
func (lb *LockFreeLogBuffer) Available() uint64 {
	tailPos := lb.tail.Load()
	headPos := lb.head.Load()
	if tailPos > headPos {
		return tailPos - headPos
	}
	return 0
}

// Free returns the number of free slots available for writing
func (lb *LockFreeLogBuffer) Free() uint64 {
	tailPos := lb.tail.Load()
	headPos := lb.head.Load()
	used := tailPos - headPos
	if used < lb.capacity {
		return lb.capacity - used
	}
	return 0
}

// IsFull returns true if the buffer is full
func (lb *LockFreeLogBuffer) IsFull() bool {
	return lb.Free() == 0
}

// IsEmpty returns true if the buffer is empty
func (lb *LockFreeLogBuffer) IsEmpty() bool {
	return lb.Available() == 0
}

// Reset clears the buffer (not thread-safe, use only when no concurrent access)
func (lb *LockFreeLogBuffer) Reset() {
	tail := lb.tail.Load()

	// Reset all slots
	for i := uint64(0); i < lb.capacity; i++ {
		lb.slots[i].sequence.Store(tail + i)
		lb.slots[i].lsn = 0
		lb.slots[i].size = 0
		lb.slots[i].data = lb.slots[i].data[:0]
	}

	// Reset counters
	lb.head.Store(tail)
}

// Stats returns buffer statistics
func (lb *LockFreeLogBuffer) Stats() LogBufferStats {
	return LogBufferStats{
		Capacity:     lb.capacity,
		SlotSize:     uint64(lb.slotSize),
		Available:    lb.Available(),
		Free:         lb.Free(),
		Appends:      lb.appends.Load(),
		Reads:        lb.reads.Load(),
		Wraps:        lb.wraps.Load(),
		Contentions:  lb.contentions.Load(),
		MaxOccupancy: lb.maxOccupancy.Load(),
		Utilization:  float64(lb.Available()) / float64(lb.capacity) * 100,
	}
}

// LogBufferStats contains statistics about the log buffer
type LogBufferStats struct {
	Capacity     uint64  // Total capacity (number of slots)
	SlotSize     uint64  // Size of each slot in bytes
	Available    uint64  // Number of records available to read
	Free         uint64  // Number of free slots
	Appends      uint64  // Total appends
	Reads        uint64  // Total reads
	Wraps        uint64  // Number of times buffer wrapped
	Contentions  uint64  // Number of CAS contentions
	MaxOccupancy uint64  // Maximum occupancy observed
	Utilization  float64 // Current utilization percentage
}

// Serialize serializes a log record for storage
// Format: [lsn(8) | size(4) | data(var)]
func (lb *LockFreeLogBuffer) Serialize(lsn uint64, data []byte) []byte {
	size := 8 + 4 + len(data)
	buf := make([]byte, size)

	binary.LittleEndian.PutUint64(buf[0:8], lsn)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[12:], data)

	return buf
}

// Deserialize deserializes a log record
// Returns (lsn, data, bytesRead, error)
func (lb *LockFreeLogBuffer) Deserialize(buf []byte) (uint64, []byte, int, error) {
	if len(buf) < 12 {
		return 0, nil, 0, fmt.Errorf("buffer too small: %d bytes", len(buf))
	}

	lsn := binary.LittleEndian.Uint64(buf[0:8])
	size := binary.LittleEndian.Uint32(buf[8:12])

	if len(buf) < 12+int(size) {
		return 0, nil, 0, fmt.Errorf("incomplete record: need %d bytes, have %d", 12+size, len(buf))
	}

	data := make([]byte, size)
	copy(data, buf[12:12+size])

	return lsn, data, 12 + int(size), nil
}

// nextPowerOf2 rounds up to the next power of 2
func nextPowerOf2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

// isPowerOf2 checks if n is a power of 2
func isPowerOf2(n uint64) bool {
	return n > 0 && (n&(n-1)) == 0
}
