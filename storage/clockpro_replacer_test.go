package storage

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestClockProBasic tests basic Clock-Pro operations
func TestClockProBasic(t *testing.T) {
	cp := NewClockProReplacer(3)

	// Unpin some pages
	cp.Unpin(1)
	cp.Unpin(2)
	cp.Unpin(3)

	// All pages should be cold initially
	stats := cp.GetStats()
	if stats.ColdSize != 3 {
		t.Errorf("Expected 3 cold pages, got %d", stats.ColdSize)
	}
	if stats.HotSize != 0 {
		t.Errorf("Expected 0 hot pages, got %d", stats.HotSize)
	}

	// Pin a page
	cp.Pin(2)

	// Should be able to evict unpinned pages
	victim, ok := cp.Victim()
	if !ok {
		t.Fatal("Expected to find a victim")
	}
	if victim != 1 && victim != 3 {
		t.Errorf("Expected victim to be 1 or 3, got %d", victim)
	}
}

// TestClockProColdToHotPromotion tests promotion from cold to hot
func TestClockProColdToHotPromotion(t *testing.T) {
	cp := NewClockProReplacer(4)

	// Add 4 pages (all cold)
	cp.Unpin(1)
	cp.Unpin(2)
	cp.Unpin(3)
	cp.Unpin(4)

	// Access page 1 multiple times to promote to hot
	cp.Pin(1)
	cp.Unpin(1) // Sets ref bit

	// Trigger victim selection to process ref bits
	cp.Pin(1)
	cp.Unpin(1)

	// Run victim to trigger promotion logic
	cp.Victim()

	stats := cp.GetStats()
	if stats.HotSize == 0 {
		t.Error("Expected at least one hot page after multiple accesses")
	}

	t.Logf("Stats after promotion: Hot=%d, Cold=%d, Test=%d",
		stats.HotSize, stats.ColdSize, stats.TestSize)
}

// TestClockProScanResistance tests that Clock-Pro resists sequential scans
func TestClockProScanResistance(t *testing.T) {
	cp := NewClockProReplacer(10)

	// Add working set that gets accessed frequently
	workingSet := []uint32{1, 2, 3, 4, 5}
	for _, pageID := range workingSet {
		cp.Unpin(pageID)
		cp.Pin(pageID)
		cp.Unpin(pageID) // Access twice to make hot
	}

	// Give victim calls a chance to promote to hot
	for i := 0; i < 5; i++ {
		cp.Victim()
	}

	stats := cp.GetStats()
	hotBefore := stats.HotSize

	// Simulate a large sequential scan
	for i := uint32(100); i < 200; i++ {
		cp.Unpin(i)
		cp.Victim() // Evict scan pages quickly
	}

	stats = cp.GetStats()

	// Working set should remain mostly hot
	if stats.HotSize < hotBefore/2 {
		t.Errorf("Sequential scan evicted too many hot pages: before=%d, after=%d",
			hotBefore, stats.HotSize)
	}

	// Original working set should still be accessible (not fully evicted)
	// Check by unpinning (which should find existing entries)
	cp.Pin(1)
	cp.Unpin(1)

	t.Logf("Scan resistance: Hot before=%d, after=%d", hotBefore, stats.HotSize)
}

// TestClockProTestEntryAdaptation tests adaptation when test entries are re-accessed
func TestClockProTestEntryAdaptation(t *testing.T) {
	cp := NewClockProReplacer(5)

	// Fill cache
	for i := uint32(1); i <= 5; i++ {
		cp.Unpin(i)
	}

	stats := cp.GetStats()
	hotMaxBefore := stats.HotMax

	// Evict a page (it becomes a test entry)
	victim, ok := cp.Victim()
	if !ok {
		t.Fatal("Expected to find a victim")
	}

	// Re-access the evicted page (test entry hit)
	cp.Unpin(victim)

	stats = cp.GetStats()

	// Hot max should have increased (adaptation)
	if stats.HotMax <= hotMaxBefore {
		t.Errorf("Expected hot max to increase after test entry hit: before=%d, after=%d",
			hotMaxBefore, stats.HotMax)
	}

	t.Logf("Adaptation: HotMax before=%d, after=%d", hotMaxBefore, stats.HotMax)
}

// TestClockProCapacityEnforcement tests that cache doesn't exceed capacity
func TestClockProCapacityEnforcement(t *testing.T) {
	capacity := 10
	cp := NewClockProReplacer(capacity)

	// Add more pages than capacity
	for i := uint32(1); i <= 20; i++ {
		cp.Unpin(i)
	}

	stats := cp.GetStats()

	// Hot + Cold should not exceed capacity
	if stats.TotalPages > capacity {
		t.Errorf("Cache exceeded capacity: got %d pages, capacity %d",
			stats.TotalPages, capacity)
	}

	t.Logf("Capacity enforcement: %d/%d pages (Hot=%d, Cold=%d, Test=%d)",
		stats.TotalPages, capacity, stats.HotSize, stats.ColdSize, stats.TestSize)
}

// TestClockProPinnedPages tests that pinned pages are not evicted
func TestClockProPinnedPages(t *testing.T) {
	cp := NewClockProReplacer(3)

	// Add and pin all pages
	cp.Unpin(1)
	cp.Unpin(2)
	cp.Unpin(3)

	cp.Pin(1)
	cp.Pin(2)
	cp.Pin(3)

	// Should not be able to evict any page
	_, ok := cp.Victim()
	if ok {
		t.Error("Should not be able to evict when all pages are pinned")
	}

	// Unpin one page
	cp.Unpin(2)

	// Now should be able to evict
	victim, ok := cp.Victim()
	if !ok {
		t.Fatal("Expected to find a victim after unpinning")
	}
	if victim != 2 {
		t.Errorf("Expected to evict page 2, got %d", victim)
	}
}

// TestClockProSize tests the Size() method
func TestClockProSize(t *testing.T) {
	cp := NewClockProReplacer(5)

	// Initially empty
	if size := cp.Size(); size != 0 {
		t.Errorf("Expected size 0, got %d", size)
	}

	// Add 3 unpinned pages
	cp.Unpin(1)
	cp.Unpin(2)
	cp.Unpin(3)

	if size := cp.Size(); size != 3 {
		t.Errorf("Expected size 3, got %d", size)
	}

	// Pin one page
	cp.Pin(2)

	if size := cp.Size(); size != 2 {
		t.Errorf("Expected size 2 after pinning, got %d", size)
	}

	// Evict one page
	cp.Victim()

	// Size should decrease (could be 1 or stay 2 if victim became test entry)
	size := cp.Size()
	if size > 2 {
		t.Errorf("Expected size <= 2 after eviction, got %d", size)
	}
}

// TestClockProConcurrentAccess tests concurrent pin/unpin/victim operations
func TestClockProConcurrentAccess(t *testing.T) {
	cp := NewClockProReplacer(100)

	var wg sync.WaitGroup

	// Writer goroutines: unpin pages
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for j := uint32(0); j < 20; j++ {
				pageID := base*20 + j
				cp.Unpin(pageID)
				time.Sleep(time.Microsecond)
			}
		}(uint32(i))
	}

	// Reader goroutines: find victims
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				cp.Victim()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Pin/unpin goroutines
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(base uint32) {
			defer wg.Done()
			for j := uint32(0); j < 30; j++ {
				pageID := base*30 + j
				cp.Pin(pageID)
				time.Sleep(time.Microsecond)
				cp.Unpin(pageID)
			}
		}(uint32(i))
	}

	wg.Wait()

	stats := cp.GetStats()
	t.Logf("Concurrent test: Hot=%d, Cold=%d, Test=%d, Total=%d",
		stats.HotSize, stats.ColdSize, stats.TestSize, stats.TotalPages)

	// Should not exceed capacity
	if stats.TotalPages > stats.Capacity {
		t.Errorf("Cache exceeded capacity: %d > %d", stats.TotalPages, stats.Capacity)
	}
}

// TestClockProEmptyCache tests operations on an empty cache
func TestClockProEmptyCache(t *testing.T) {
	cp := NewClockProReplacer(10)

	// Victim on empty cache
	_, ok := cp.Victim()
	if ok {
		t.Error("Should not find victim in empty cache")
	}

	// Pin non-existent page (should be no-op)
	cp.Pin(999)

	// Size should be 0
	if size := cp.Size(); size != 0 {
		t.Errorf("Expected size 0 for empty cache, got %d", size)
	}
}

// TestClockProWorkloadMix tests mixed workload patterns
func TestClockProWorkloadMix(t *testing.T) {
	cp := NewClockProReplacer(50)

	// Working set (frequent accesses)
	workingSet := []uint32{1, 2, 3, 4, 5}
	for i := 0; i < 10; i++ {
		for _, pageID := range workingSet {
			cp.Unpin(pageID)
		}
	}

	// Sequential scan (one-time accesses)
	for i := uint32(100); i < 200; i++ {
		cp.Unpin(i)
		if i%2 == 0 {
			cp.Victim()
		}
	}

	// Random accesses
	rand.Seed(42)
	for i := 0; i < 100; i++ {
		pageID := uint32(rand.Intn(100)) + 1
		cp.Unpin(pageID)
		if i%3 == 0 {
			cp.Victim()
		}
	}

	stats := cp.GetStats()

	// Should have adapted to the workload
	if stats.HotSize == 0 {
		t.Error("Expected some hot pages after mixed workload")
	}

	// Should not exceed capacity
	if stats.TotalPages > stats.Capacity {
		t.Errorf("Cache exceeded capacity: %d > %d", stats.TotalPages, stats.Capacity)
	}

	t.Logf("Mixed workload: Hot=%d, Cold=%d, Test=%d",
		stats.HotSize, stats.ColdSize, stats.TestSize)
}

// TestClockProStressTest performs a stress test with many operations
func TestClockProStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	cp := NewClockProReplacer(100)
	rand.Seed(time.Now().UnixNano())

	operations := 10000
	for i := 0; i < operations; i++ {
		op := rand.Intn(4)
		pageID := uint32(rand.Intn(200)) + 1

		switch op {
		case 0: // Unpin
			cp.Unpin(pageID)
		case 1: // Pin
			cp.Pin(pageID)
		case 2: // Victim
			cp.Victim()
		case 3: // Multiple accesses
			cp.Unpin(pageID)
			cp.Pin(pageID)
			cp.Unpin(pageID)
		}

		// Periodic validation
		if i%1000 == 0 {
			stats := cp.GetStats()
			if stats.TotalPages > stats.Capacity {
				t.Fatalf("Capacity violation at op %d: %d > %d",
					i, stats.TotalPages, stats.Capacity)
			}
		}
	}

	stats := cp.GetStats()
	t.Logf("Stress test completed: %d operations, Hot=%d, Cold=%d, Test=%d",
		operations, stats.HotSize, stats.ColdSize, stats.TestSize)
}

// Benchmark Clock-Pro operations

func BenchmarkClockProUnpin(b *testing.B) {
	cp := NewClockProReplacer(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp.Unpin(uint32(i % 1000))
	}
}

func BenchmarkClockProPin(b *testing.B) {
	cp := NewClockProReplacer(1000)

	// Populate cache
	for i := uint32(0); i < 1000; i++ {
		cp.Unpin(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp.Pin(uint32(i % 1000))
	}
}

func BenchmarkClockProVictim(b *testing.B) {
	cp := NewClockProReplacer(1000)

	// Populate cache
	for i := uint32(0); i < 1000; i++ {
		cp.Unpin(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp.Victim()
		// Refill to maintain state
		if i%100 == 0 {
			cp.Unpin(uint32(i % 1000))
		}
	}
}

func BenchmarkClockProMixedWorkload(b *testing.B) {
	cp := NewClockProReplacer(1000)

	// Prepopulate
	for i := uint32(0); i < 1000; i++ {
		cp.Unpin(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := i % 4
		pageID := uint32(i % 1000)

		switch op {
		case 0:
			cp.Unpin(pageID)
		case 1:
			cp.Pin(pageID)
		case 2:
			cp.Victim()
		case 3:
			cp.Unpin(pageID + 1000)
		}
	}
}

// Benchmark comparison with other replacers

func BenchmarkCompareReplacers(b *testing.B) {
	capacity := 1000
	accessPattern := make([]uint32, 10000)
	rand.Seed(42)

	// Generate a realistic access pattern with locality
	for i := range accessPattern {
		if rand.Float64() < 0.7 {
			// 70% working set (0-99)
			accessPattern[i] = uint32(rand.Intn(100))
		} else {
			// 30% random (100-999)
			accessPattern[i] = uint32(rand.Intn(900)) + 100
		}
	}

	b.Run("ClockPro", func(b *testing.B) {
		cp := NewClockProReplacer(capacity)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pageID := accessPattern[i%len(accessPattern)]
			cp.Unpin(pageID)
			if i%10 == 0 {
				cp.Victim()
			}
		}
	})

	b.Run("ARC", func(b *testing.B) {
		arc := NewARCReplacer(capacity)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pageID := accessPattern[i%len(accessPattern)]
			arc.Unpin(pageID)
			if i%10 == 0 {
				arc.Victim()
			}
		}
	})

	b.Run("LRU", func(b *testing.B) {
		lru := NewLRUReplacer(uint32(capacity))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pageID := accessPattern[i%len(accessPattern)]
			lru.Unpin(pageID)
			if i%10 == 0 {
				lru.Victim()
			}
		}
	})

	b.Run("TwoQ", func(b *testing.B) {
		twoq := NewTwoQReplacer(capacity)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pageID := accessPattern[i%len(accessPattern)]
			twoq.Unpin(pageID)
			if i%10 == 0 {
				twoq.Victim()
			}
		}
	})
}

// BenchmarkClockProScanPattern tests performance under sequential scan workload
func BenchmarkClockProScanPattern(b *testing.B) {
	cp := NewClockProReplacer(100)

	// Add working set
	for i := uint32(0); i < 50; i++ {
		cp.Unpin(i)
		cp.Pin(i)
		cp.Unpin(i) // Access twice
	}

	b.ResetTimer()

	// Sequential scan with working set accesses
	for i := 0; i < b.N; i++ {
		if i%10 < 7 {
			// 70% working set
			cp.Unpin(uint32(i % 50))
		} else {
			// 30% scan
			cp.Unpin(uint32(i%1000) + 1000)
			cp.Victim()
		}
	}
}
