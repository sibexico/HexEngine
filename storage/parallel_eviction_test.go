package storage

import (
	"os"
	"sync"
	"testing"
	"time"
)

// TestParallelEviction tests that multiple pages can be evicted concurrently
func TestParallelEviction(t *testing.T) {
	dm, err := NewDiskManager("test_parallel_evict.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_parallel_evict.db")

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Fill buffer pool
	pages := make([]*Page, 15)
	for i := 0; i < 15; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		pages[i] = page
		// Write some data
		tuple := NewTuple([]byte("test data"))
		page.GetData().InsertTuple(tuple)
		page.SetDirty(true)
		// Unpin to make evictable
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Evict 5 pages in parallel
	evicted, err := bpm.evictPagesParallel(5)
	if err != nil {
		t.Fatal(err)
	}

	if len(evicted) < 2 {
		t.Errorf("Expected at least 2 evictions, got %d", len(evicted))
	}

	t.Logf("Successfully evicted %d pages in parallel", len(evicted))

	// Verify frames were cleared (not necessarily in free list yet)
	bpm.pagesMutex.RLock()
	clearedCount := 0
	for _, frameId := range evicted {
		if bpm.pages[frameId] == nil {
			clearedCount++
		}
	}
	bpm.pagesMutex.RUnlock()

	if clearedCount != len(evicted) {
		t.Errorf("Expected all %d evicted frames to be cleared, got %d", len(evicted), clearedCount)
	}
}

// TestGetFrameIdBatch tests batch frame allocation
func TestGetFrameIdBatch(t *testing.T) {
	dm, err := NewDiskManager("test_batch_frames.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_batch_frames.db")

	bpm, err := NewBufferPoolManager(20, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate 5 frames at once from free list
	frames1, err := bpm.getFrameIdBatch(5)
	if err != nil {
		t.Fatal(err)
	}

	if len(frames1) != 5 {
		t.Errorf("Expected 5 frames, got %d", len(frames1))
	}

	// Fill buffer pool
	for i := 0; i < 20; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		bpm.UnpinPage(page.GetPageId(), false)
	}

	// Now batch allocation should trigger eviction
	frames2, err := bpm.getFrameIdBatch(3)
	if err != nil {
		t.Fatal(err)
	}

	if len(frames2) < 1 {
		t.Errorf("Expected at least 1 frame from eviction, got %d", len(frames2))
	}

	t.Logf("Batch allocation: first=%d frames, second=%d frames", len(frames1), len(frames2))
}

// TestFlushAllPagesParallel tests parallel flushing of all dirty pages
func TestFlushAllPagesParallel(t *testing.T) {
	dm, err := NewDiskManager("test_parallel_flush.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_parallel_flush.db")

	bpm, err := NewBufferPoolManager(20, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Create and dirty many pages
	pageIDs := make([]uint32, 0, 15)
	for i := 0; i < 15; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		pageIDs = append(pageIDs, page.GetPageId())

		// Write data
		tuple := NewTuple([]byte("dirty data"))
		page.GetData().InsertTuple(tuple)
		page.SetDirty(true)
	}

	// Flush all pages in parallel
	start := time.Now()
	err = bpm.FlushAllPagesParallel(4) // Use 4 workers
	duration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Flushed 15 pages in parallel in %v", duration)

	// Verify all pages are now clean
	for _, pageID := range pageIDs {
		page, exists := bpm.pageTable.Get(pageID)
		if !exists {
			t.Errorf("Page %d not found", pageID)
			continue
		}
		if page.IsDirty() {
			t.Errorf("Page %d still dirty after flush", pageID)
		}
	}
}

// TestFlushAllPagesParallelWithErrors tests error handling in parallel flush
func TestFlushAllPagesParallelWithErrors(t *testing.T) {
	dm, err := NewDiskManager("test_parallel_flush_errors.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_parallel_flush_errors.db")

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Create some dirty pages
	for i := 0; i < 5; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		page.SetDirty(true)
	}

	// Close disk manager to force errors
	dm.Close()

	// Flush should fail gracefully
	err = bpm.FlushAllPagesParallel(2)
	if err == nil {
		t.Error("Expected error when flushing with closed disk manager")
	}

	t.Logf("Correctly handled error: %v", err)
}

// TestParallelEvictionConcurrency tests thread safety of parallel eviction
func TestParallelEvictionConcurrency(t *testing.T) {
	dm, err := NewDiskManager("test_parallel_evict_concurrent.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_parallel_evict_concurrent.db")

	bpm, err := NewBufferPoolManager(50, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Fill buffer pool
	for i := 0; i < 50; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		page.SetDirty(true)
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Concurrent evictions
	var wg sync.WaitGroup
	successCount := make(chan int, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			evicted, err := bpm.evictPagesParallel(5)
			if err == nil {
				successCount <- len(evicted)
			} else {
				// No free pages is expected when multiple goroutines compete
				successCount <- 0
			}
		}(i)
	}

	wg.Wait()
	close(successCount)

	// Count total evictions
	totalEvicted := 0
	for count := range successCount {
		totalEvicted += count
	}

	// Should have evicted at least some pages
	if totalEvicted == 0 {
		t.Error("Expected at least some pages to be evicted concurrently")
	}

	t.Logf("Concurrent eviction test passed: %d pages evicted total", totalEvicted)
}

// TestEvictPagesParallelNoVictims tests behavior when no pages can be evicted
func TestEvictPagesParallelNoVictims(t *testing.T) {
	dm, err := NewDiskManager("test_no_victims.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_no_victims.db")

	// Use LRU replacer because ARC has a known issue where Victim() can return
	// a frame ID even when Size()==0. This is a separate bug to fix in ARC.
	bpm, err := NewBufferPoolManagerWithReplacer(5, dm, "lru")
	if err != nil {
		t.Fatal(err)
	}

	// Create pages, unpin them, then fetch them again to pin properly
	pageIDs := make([]uint32, 0, 5)
	for i := 0; i < 5; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		pageIDs = append(pageIDs, page.GetPageId())
		// Unpin to register with replacer
		bpm.UnpinPage(page.GetPageId(), false)
	}

	// Now fetch all pages again (which pins them)
	for _, pageID := range pageIDs {
		_, err := bpm.FetchPage(pageID)
		if err != nil {
			t.Fatal(err)
		}
		// Don't unpin - keep them pinned
	}

	// Try to evict - should fail since all pages are pinned
	evicted, err := bpm.evictPagesParallel(3)
	t.Logf("Eviction result: evicted=%v, err=%v, replacer_size=%d", evicted, err, bpm.replacer.Size())

	if err == nil {
		t.Error("Expected error when trying to evict pinned pages")
	} else {
		t.Logf("Correctly handled no victims case: %v", err)
	}
}

// TestParallelEvictionPerformance benchmarks parallel vs sequential eviction
func TestParallelEvictionPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	dm, err := NewDiskManager("test_evict_perf.db")
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("test_evict_perf.db")

	bpm, err := NewBufferPoolManager(100, dm)
	if err != nil {
		t.Fatal(err)
	}

	// Fill buffer pool with dirty pages
	for i := 0; i < 100; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		tuple := NewTuple([]byte("performance test data"))
		page.GetData().InsertTuple(tuple)
		page.SetDirty(true)
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Measure parallel eviction
	start := time.Now()
	evicted, err := bpm.evictPagesParallel(20)
	parallelDuration := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Parallel eviction: %d pages in %v (%.2f pages/ms)",
		len(evicted), parallelDuration, float64(len(evicted))/float64(parallelDuration.Milliseconds()))

	// Refill for sequential test
	for i := 0; i < len(evicted); i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		page.SetDirty(true)
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Measure sequential eviction
	start = time.Now()
	seqCount := 0
	for i := 0; i < 20; i++ {
		_, err := bpm.evictPage()
		if err == nil {
			seqCount++
		}
	}
	seqDuration := time.Since(start)

	t.Logf("Sequential eviction: %d pages in %v (%.2f pages/ms)",
		seqCount, seqDuration, float64(seqCount)/float64(seqDuration.Milliseconds()))

	if parallelDuration < seqDuration {
		speedup := float64(seqDuration) / float64(parallelDuration)
		t.Logf("Parallel eviction %.2fx faster than sequential", speedup)
	}
}

// BenchmarkEvictPagesParallel benchmarks parallel page eviction
func BenchmarkEvictPagesParallel(b *testing.B) {
	dm, err := NewDiskManager("bench_parallel_evict.db")
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("bench_parallel_evict.db")

	bpm, err := NewBufferPoolManager(200, dm)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-fill buffer pool
	for i := 0; i < 200; i++ {
		page, _ := bpm.NewPage()
		page.SetDirty(true)
		bpm.UnpinPage(page.GetPageId(), true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Evict and refill
		evicted, err := bpm.evictPagesParallel(10)
		if err == nil {
			// Refill
			for range evicted {
				page, _ := bpm.NewPage()
				if page != nil {
					page.SetDirty(true)
					bpm.UnpinPage(page.GetPageId(), true)
				}
			}
		}
	}
}

// BenchmarkFlushAllPagesParallel benchmarks parallel page flushing
func BenchmarkFlushAllPagesParallel(b *testing.B) {
	dm, err := NewDiskManager("bench_parallel_flush.db")
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()
	defer os.Remove("bench_parallel_flush.db")

	bpm, err := NewBufferPoolManager(100, dm)
	if err != nil {
		b.Fatal(err)
	}

	// Create dirty pages
	for i := 0; i < 100; i++ {
		page, _ := bpm.NewPage()
		tuple := NewTuple([]byte("benchmark data"))
		page.GetData().InsertTuple(tuple)
		page.SetDirty(true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Flush all pages
		err := bpm.FlushAllPagesParallel(4)
		if err != nil {
			b.Fatal(err)
		}

		// Re-dirty for next iteration
		bpm.pagesMutex.RLock()
		for _, page := range bpm.pages {
			if page != nil {
				page.SetDirty(true)
			}
		}
		bpm.pagesMutex.RUnlock()
	}
}
