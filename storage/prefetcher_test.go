package storage

import (
	"os"
	"testing"
	"time"
)

func TestPrefetcherPatternDetection(t *testing.T) {
	// Create a buffer pool and prefetcher
	testFile := "test_prefetch.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(10, diskManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	// Simulate sequential forward access
	contextID := uint64(1)
	for i := uint32(0); i < 5; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	// Allow prefetch goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Check that pattern was detected
	stats := prefetcher.GetStats()
	if stats.PatternsDetected == 0 {
		t.Error("Expected pattern detection")
	}

	t.Logf("Patterns detected: %d, Pages prefetched: %d",
		stats.PatternsDetected, stats.PagesPrefetched)
}

func TestPrefetcherBackwardPattern(t *testing.T) {
	testFile := "test_prefetch_back.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(10, diskManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	// Simulate sequential backward access
	contextID := uint64(2)
	for i := uint32(20); i > 15; i-- {
		prefetcher.RecordAccess(contextID, i)
	}

	time.Sleep(100 * time.Millisecond)

	stats := prefetcher.GetStats()
	if stats.PatternsDetected == 0 {
		t.Error("Expected backward pattern detection")
	}

	t.Logf("Backward pattern - Patterns: %d, Prefetched: %d",
		stats.PatternsDetected, stats.PagesPrefetched)
}

func TestPrefetcherRandomAccess(t *testing.T) {
	testFile := "test_prefetch_random.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(10, diskManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	// Simulate random access - should not trigger prefetching
	contextID := uint64(3)
	pages := []uint32{5, 12, 3, 18, 7, 21}
	for _, pageID := range pages {
		prefetcher.RecordAccess(contextID, pageID)
	}

	stats := prefetcher.GetStats()
	if stats.PatternsDetected > 0 {
		t.Error("Should not detect patterns in random access")
	}

	t.Logf("Random access - No patterns detected (expected)")
}

func TestPrefetcherDisable(t *testing.T) {
	testFile := "test_prefetch_disable.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(10, diskManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	// Disable prefetching
	prefetcher.SetEnabled(false)

	// Try sequential access
	contextID := uint64(5)
	for i := uint32(0); i < 5; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	stats := prefetcher.GetStats()
	if stats.PatternsDetected > 0 || stats.PagesPrefetched > 0 {
		t.Error("Prefetcher should be disabled")
	}
}

func TestPrefetcherConfiguration(t *testing.T) {
	testFile := "test_prefetch_config.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(10, diskManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	// Configure for more aggressive prefetching
	prefetcher.Configure(2, 4) // Trigger after 2 accesses, prefetch 4 pages

	contextID := uint64(6)
	prefetcher.RecordAccess(contextID, 0)
	prefetcher.RecordAccess(contextID, 1)
	prefetcher.RecordAccess(contextID, 2) // Need more accesses to build confidence >= 0.6

	// Should trigger after confidence threshold reached
	time.Sleep(100 * time.Millisecond)

	stats := prefetcher.GetStats()
	if stats.PatternsDetected == 0 {
		t.Error("Expected pattern with threshold=2 and sufficient confidence")
	}
}

// Benchmark prefetching overhead
func BenchmarkPrefetcherRecordAccess(b *testing.B) {
	testFile := "bench_prefetch.db"
	diskManager, err := NewDiskManager(testFile)
	if err != nil {
		b.Fatalf("Failed to create disk manager: %v", err)
	}
	defer diskManager.Close()
	defer os.Remove(testFile)

	bpm, err := NewBufferPoolManager(100, diskManager)
	if err != nil {
		b.Fatalf("Failed to create buffer pool: %v", err)
	}
	prefetcher := NewPrefetcher(bpm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefetcher.RecordAccess(uint64(i%10), uint32(i%1000))
	}
}
