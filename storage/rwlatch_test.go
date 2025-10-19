package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRWLatchBasic tests basic RWLatch operations
func TestRWLatchBasic(t *testing.T) {
	latch := NewRWLatch()

	// Read lock
	latch.RLock()
	if !latch.IsWriterActive() && latch.GetReaderCount() != 1 {
		t.Errorf("Expected 1 reader, got %d", latch.GetReaderCount())
	}
	latch.RUnlock()

	// Write lock
	latch.Lock()
	if !latch.IsWriterActive() {
		t.Error("Expected writer to be active")
	}
	latch.Unlock()

	if latch.IsWriterActive() {
		t.Error("Expected writer to be inactive after unlock")
	}
}

// TestRWLatchMultipleReaders tests multiple concurrent readers
func TestRWLatchMultipleReaders(t *testing.T) {
	latch := NewRWLatch()

	// Acquire 10 read locks
	for i := 0; i < 10; i++ {
		latch.RLock()
	}

	readerCount := latch.GetReaderCount()
	if readerCount != 10 {
		t.Errorf("Expected 10 readers, got %d", readerCount)
	}

	// Release all read locks
	for i := 0; i < 10; i++ {
		latch.RUnlock()
	}

	if latch.GetReaderCount() != 0 {
		t.Errorf("Expected 0 readers after unlock, got %d", latch.GetReaderCount())
	}
}

// TestRWLatchWriterExclusion tests that writer excludes readers
func TestRWLatchWriterExclusion(t *testing.T) {
	latch := NewRWLatch()

	// Acquire write lock
	latch.Lock()

	// Try to acquire read lock (should not succeed immediately)
	acquired := latch.TryRLock()
	if acquired {
		t.Error("Reader should not acquire lock while writer is active")
	}

	// Release write lock
	latch.Unlock()

	// Now reader should be able to acquire
	latch.RLock()
	if latch.GetReaderCount() != 1 {
		t.Error("Reader should acquire lock after writer releases")
	}
	latch.RUnlock()
}

// TestRWLatchReaderWriterExclusion tests that readers block writer
func TestRWLatchReaderWriterExclusion(t *testing.T) {
	latch := NewRWLatch()

	// Acquire read lock
	latch.RLock()

	// Try to acquire write lock (should not succeed immediately)
	acquired := latch.TryLock()
	if acquired {
		t.Error("Writer should not acquire lock while readers are active")
	}

	// Release read lock
	latch.RUnlock()

	// Now writer should be able to acquire
	latch.Lock()
	if !latch.IsWriterActive() {
		t.Error("Writer should acquire lock after all readers release")
	}
	latch.Unlock()
}

// TestRWLatchConcurrentReaders tests many concurrent readers
func TestRWLatchConcurrentReaders(t *testing.T) {
	latch := NewRWLatch()
	var wg sync.WaitGroup

	numReaders := 100
	var readCount int32

	// Start concurrent readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			latch.RLock()
			atomic.AddInt32(&readCount, 1)
			time.Sleep(time.Microsecond)
			atomic.AddInt32(&readCount, -1)
			latch.RUnlock()
		}()
	}

	wg.Wait()

	// All readers should have completed
	if latch.GetReaderCount() != 0 {
		t.Errorf("Expected 0 readers after completion, got %d", latch.GetReaderCount())
	}

	finalCount := atomic.LoadInt32(&readCount)
	if finalCount != 0 {
		t.Errorf("Expected read count 0, got %d", finalCount)
	}
}

// TestRWLatchReadWriteContention tests readers and writer under contention
func TestRWLatchReadWriteContention(t *testing.T) {
	latch := NewRWLatch()
	var wg sync.WaitGroup

	sharedData := 0
	numReaders := 50
	numWriters := 5
	iterations := 100

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				latch.RLock()
				_ = sharedData // Read operation
				latch.RUnlock()
			}
		}()
	}

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				latch.Lock()
				sharedData++ // Write operation
				latch.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verify write count
	expectedWrites := numWriters * iterations
	if sharedData != expectedWrites {
		t.Errorf("Expected %d writes, got %d", expectedWrites, sharedData)
	}

	// No locks should be held
	if latch.GetReaderCount() != 0 {
		t.Error("Readers still active after completion")
	}
	if latch.IsWriterActive() {
		t.Error("Writer still active after completion")
	}
}

// TestRWLatchFairness tests that writers eventually get access under heavy read load
func TestRWLatchFairness(t *testing.T) {
	latch := NewRWLatch()
	var wg sync.WaitGroup

	writerAcquired := make(chan bool, 1)

	// Start many readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				latch.RLock()
				time.Sleep(time.Microsecond)
				latch.RUnlock()
			}
		}()
	}

	// Start a writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let readers start
		latch.Lock()
		writerAcquired <- true
		latch.Unlock()
	}()

	// Wait for writer to acquire (with timeout)
	select {
	case <-writerAcquired:
		// Success: writer acquired lock
	case <-time.After(5 * time.Second):
		t.Error("Writer failed to acquire lock within timeout (fairness issue)")
	}

	wg.Wait()
}

// TestRWLatchTryLockOperations tests TryRLock and TryLock
func TestRWLatchTryLockOperations(t *testing.T) {
	latch := NewRWLatch()

	// TryRLock should succeed when latch is free
	if !latch.TryRLock() {
		t.Error("TryRLock should succeed on free latch")
	}
	latch.RUnlock()

	// TryLock should succeed when latch is free
	if !latch.TryLock() {
		t.Error("TryLock should succeed on free latch")
	}

	// TryRLock should fail when writer is active
	if latch.TryRLock() {
		t.Error("TryRLock should fail when writer is active")
		latch.RUnlock()
	}

	// TryLock should fail when writer is active
	if latch.TryLock() {
		t.Error("TryLock should fail when writer is active")
		latch.Unlock()
	}

	latch.Unlock()

	// Acquire read lock
	latch.RLock()

	// TryLock should fail when readers are active
	if latch.TryLock() {
		t.Error("TryLock should fail when readers are active")
		latch.Unlock()
	}

	// But TryRLock should succeed
	if !latch.TryRLock() {
		t.Error("TryRLock should succeed when only readers are active")
	}
	latch.RUnlock()
	latch.RUnlock()
}

// TestRWLatchStressTest performs a stress test with many operations
func TestRWLatchStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	latch := NewRWLatch()
	var wg sync.WaitGroup

	sharedData := 0
	numGoroutines := 50
	operationsPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				if id%3 == 0 {
					// Writer
					latch.Lock()
					sharedData++
					latch.Unlock()
				} else {
					// Reader
					latch.RLock()
					_ = sharedData
					latch.RUnlock()
				}
			}
		}(i)
	}

	wg.Wait()

	// Count writers
	numWriters := 0
	for i := 0; i < numGoroutines; i++ {
		if i%3 == 0 {
			numWriters++
		}
	}

	expectedWrites := numWriters * operationsPerGoroutine
	if sharedData != expectedWrites {
		t.Errorf("Expected %d writes, got %d", expectedWrites, sharedData)
	}

	stats := latch.GetStats()
	if stats.ReaderCount != 0 || stats.WriterActive {
		t.Error("Latch should be free after stress test")
	}
}

// TestPageLatchBasic tests basic PageLatch operations
func TestPageLatchBasic(t *testing.T) {
	page := NewPageLatch(1)

	if page.GetPageId() != 1 {
		t.Errorf("Expected page ID 1, got %d", page.GetPageId())
	}

	// Test pin/unpin
	page.Pin()
	if page.GetPinCount() != 1 {
		t.Errorf("Expected pin count 1, got %d", page.GetPinCount())
	}

	page.Unpin()
	if page.GetPinCount() != 0 {
		t.Errorf("Expected pin count 0, got %d", page.GetPinCount())
	}

	// Test dirty flag
	if page.IsDirty() {
		t.Error("New page should not be dirty")
	}

	page.SetDirty(true)
	if !page.IsDirty() {
		t.Error("Page should be dirty after SetDirty(true)")
	}

	page.SetDirty(false)
	if page.IsDirty() {
		t.Error("Page should not be dirty after SetDirty(false)")
	}
}

// TestPageLatchReadWrite tests read/write locking on PageLatch
func TestPageLatchReadWrite(t *testing.T) {
	page := NewPageLatch(1)

	// Read lock
	page.RLock()
	stats := page.GetLatchStats()
	if stats.ReaderCount != 1 {
		t.Errorf("Expected 1 reader, got %d", stats.ReaderCount)
	}
	page.RUnlock()

	// Write lock
	page.WLock()
	stats = page.GetLatchStats()
	if !stats.WriterActive {
		t.Error("Expected writer to be active")
	}
	page.WUnlock()

	// Try operations
	if !page.TryRLock() {
		t.Error("TryRLock should succeed on free page")
	}
	page.RUnlock()

	if !page.TryWLock() {
		t.Error("TryWLock should succeed on free page")
	}
	page.WUnlock()
}

// TestPageLatchConcurrentAccess tests concurrent access to PageLatch
func TestPageLatchConcurrentAccess(t *testing.T) {
	page := NewPageLatch(1)
	var wg sync.WaitGroup

	// Concurrent readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				page.RLock()
				_ = page.GetData()
				page.RUnlock()
			}
		}()
	}

	// Concurrent pin/unpin (atomic operations)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				page.Pin()
				page.Unpin()
			}
		}()
	}

	// Occasional writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				page.WLock()
				page.SetDirty(true)
				page.WUnlock()
			}
		}()
	}

	wg.Wait()

	// Verify final state
	if page.GetPinCount() != 0 {
		t.Errorf("Expected pin count 0, got %d", page.GetPinCount())
	}

	stats := page.GetLatchStats()
	if stats.ReaderCount != 0 || stats.WriterActive {
		t.Error("Page should have no active locks after test")
	}
}

// Benchmark RWLatch operations

func BenchmarkRWLatchRLock(b *testing.B) {
	latch := NewRWLatch()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		latch.RLock()
		latch.RUnlock()
	}
}

func BenchmarkRWLatchLock(b *testing.B) {
	latch := NewRWLatch()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		latch.Lock()
		latch.Unlock()
	}
}

func BenchmarkRWLatchTryRLock(b *testing.B) {
	latch := NewRWLatch()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if latch.TryRLock() {
			latch.RUnlock()
		}
	}
}

func BenchmarkRWLatchTryLock(b *testing.B) {
	latch := NewRWLatch()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if latch.TryLock() {
			latch.Unlock()
		}
	}
}

// Benchmark comparison with sync.RWMutex

func BenchmarkCompareReadLocks(b *testing.B) {
	b.Run("RWLatch", func(b *testing.B) {
		latch := NewRWLatch()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			latch.RLock()
			latch.RUnlock()
		}
	})

	b.Run("RWMutex", func(b *testing.B) {
		var mutex sync.RWMutex
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			mutex.RLock()
			mutex.RUnlock()
		}
	})
}

func BenchmarkCompareWriteLocks(b *testing.B) {
	b.Run("RWLatch", func(b *testing.B) {
		latch := NewRWLatch()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			latch.Lock()
			latch.Unlock()
		}
	})

	b.Run("RWMutex", func(b *testing.B) {
		var mutex sync.RWMutex
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			mutex.Lock()
			mutex.Unlock()
		}
	})
}

func BenchmarkCompareMixedLoad(b *testing.B) {
	b.Run("RWLatch", func(b *testing.B) {
		latch := NewRWLatch()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					latch.Lock()
					latch.Unlock()
				} else {
					latch.RLock()
					latch.RUnlock()
				}
				i++
			}
		})
	})

	b.Run("RWMutex", func(b *testing.B) {
		var mutex sync.RWMutex
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					mutex.Lock()
					mutex.Unlock()
				} else {
					mutex.RLock()
					mutex.RUnlock()
				}
				i++
			}
		})
	})
}

func BenchmarkPageLatchOperations(b *testing.B) {
	page := NewPageLatch(1)

	b.Run("Pin", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page.Pin()
		}
	})

	b.Run("Unpin", func(b *testing.B) {
		// Prepopulate pin count
		for i := 0; i < b.N; i++ {
			page.Pin()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page.Unpin()
		}
	})

	b.Run("RLock", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page.RLock()
			page.RUnlock()
		}
	})

	b.Run("WLock", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			page.WLock()
			page.WUnlock()
		}
	})
}
