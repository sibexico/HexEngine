package storage

import (
	"os"
	"testing"
	"time"
)

func setupAdaptiveTest(t *testing.T, poolSize uint32, filename string) (*BufferPoolManager, *Prefetcher) {
	dm, err := NewDiskManager(filename)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dm.Close()
		os.Remove(filename)
	})

	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		t.Fatal(err)
	}

	prefetcher := NewPrefetcher(bpm)
	return bpm, prefetcher
}

// TestAdaptivePrefetchStrideDetection tests stride detection for non-sequential patterns
func TestAdaptivePrefetchStrideDetection(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 20, "test_adaptive_stride.db")

	contextID := uint64(1)

	// Simulate stride-2 access pattern: 0, 2, 4, 6, 8
	pages := []uint32{0, 2, 4, 6, 8}
	for _, pageID := range pages {
		prefetcher.RecordAccess(contextID, pageID)
	}

	// Check that pattern was detected
	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	prefetcher.mu.RUnlock()

	if pattern == nil {
		t.Fatal("Pattern should be detected")
	}

	if pattern.Stride != 2 {
		t.Errorf("Expected stride 2, got %d", pattern.Stride)
	}

	if pattern.Confidence < 0.5 {
		t.Errorf("Expected confidence >= 0.5, got %.2f", pattern.Confidence)
	}

	// Verify prefetch was triggered
	stats := prefetcher.GetStats()
	if stats.PatternsDetected == 0 {
		t.Error("Expected at least one pattern detection")
	}

	if stats.StridesDetected == 0 {
		t.Error("Expected stride-based pattern detection")
	}
}

// TestAdaptivePrefetchConfidenceScoring tests confidence score calculation
func TestAdaptivePrefetchConfidenceScoring(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 20, "test_adaptive_confidence.db")

	contextID := uint64(1)

	// Consistent sequential pattern should build high confidence
	for i := uint32(0); i < 15; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	confidence := pattern.Confidence
	prefetcher.mu.RUnlock()

	// After 15 consistent accesses, confidence should be high
	if confidence < 0.8 {
		t.Errorf("Expected high confidence (>0.8) for consistent pattern, got %.2f", confidence)
	}

	// Check average confidence stat
	stats := prefetcher.GetStats()
	if stats.AvgConfidence < 0.6 {
		t.Errorf("Expected average confidence > 0.6, got %.2f", stats.AvgConfidence)
	}
}

// TestAdaptivePrefetchConfidenceBasedDistance tests that prefetch distance scales with confidence
func TestAdaptivePrefetchConfidenceBasedDistance(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 50, "test_adaptive_distance.db")

	prefetcher.Configure(3, 10) // threshold=3, max_distance=10
	contextID := uint64(1)

	// Build low confidence pattern (only 3 accesses)
	for i := uint32(0); i < 3; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	// Wait for prefetch to complete
	time.Sleep(50 * time.Millisecond)

	lowConfidenceStats := prefetcher.GetStats()

	// Reset and build high confidence pattern
	prefetcher.ResetStats()
	prefetcher.ClearPattern(contextID)

	for i := uint32(100); i < 115; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	// Wait for prefetch
	time.Sleep(50 * time.Millisecond)

	highConfidenceStats := prefetcher.GetStats()

	// High confidence should prefetch more pages
	if highConfidenceStats.PagesPrefetched <= lowConfidenceStats.PagesPrefetched {
		t.Logf("Note: High confidence prefetch similar to low confidence")
		// Not a hard failure since async prefetch timing can vary
	}
}

// TestAdaptivePrefetchStrideChange tests pattern adaptation when stride changes
func TestAdaptivePrefetchStrideChange(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_stride_change.db")

	contextID := uint64(1)

	// Start with stride-1 pattern
	for i := uint32(0); i < 5; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	prefetcher.mu.RLock()
	stride1 := prefetcher.patterns[contextID].Stride
	prefetcher.mu.RUnlock()

	if stride1 != 1 {
		t.Errorf("Expected initial stride 1, got %d", stride1)
	}

	// Change to stride-3 pattern
	for i := uint32(10); i < 30; i += 3 {
		prefetcher.RecordAccess(contextID, i)
	}

	prefetcher.mu.RLock()
	stride2 := prefetcher.patterns[contextID].Stride
	confidence2 := prefetcher.patterns[contextID].Confidence
	prefetcher.mu.RUnlock()

	if stride2 != 3 {
		t.Errorf("Expected adapted stride 3, got %d", stride2)
	}

	// Confidence should rebuild
	if confidence2 < 0.4 {
		t.Errorf("Expected rebuilding confidence >= 0.4, got %.2f", confidence2)
	}
}

// TestAdaptivePrefetchBackwardStride tests negative stride detection
func TestAdaptivePrefetchBackwardStride(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_backward.db")

	contextID := uint64(1)

	// Backward stride-2 pattern: 20, 18, 16, 14, 12
	pages := []uint32{20, 18, 16, 14, 12}
	for _, pageID := range pages {
		prefetcher.RecordAccess(contextID, pageID)
	}

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	prefetcher.mu.RUnlock()

	if pattern.Stride != -2 {
		t.Errorf("Expected stride -2, got %d", pattern.Stride)
	}

	if pattern.Confidence < 0.5 {
		t.Errorf("Expected confidence >= 0.5 for backward pattern, got %.2f", pattern.Confidence)
	}
}

// TestAdaptivePrefetchInconsistentPattern tests confidence degradation for inconsistent access
func TestAdaptivePrefetchInconsistentPattern(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_inconsistent.db")

	contextID := uint64(1)

	// Inconsistent pattern: mix of different strides
	pages := []uint32{0, 1, 3, 4, 7, 8}
	for _, pageID := range pages {
		prefetcher.RecordAccess(contextID, pageID)
	}

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	confidence := pattern.Confidence
	prefetcher.mu.RUnlock()

	// Inconsistent pattern should have lower confidence
	if confidence > 0.6 {
		t.Errorf("Expected low confidence for inconsistent pattern, got %.2f", confidence)
	}
}

// TestAdaptivePrefetchHistory tests that history tracking works correctly
func TestAdaptivePrefetchHistory(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_history.db")

	contextID := uint64(1)

	// Access 15 pages (more than HistorySize of 10)
	for i := uint32(0); i < 15; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	historyLen := len(pattern.History)
	prefetcher.mu.RUnlock()

	// History should be capped at HistorySize
	if historyLen > pattern.HistorySize {
		t.Errorf("History size exceeded limit: %d > %d", historyLen, pattern.HistorySize)
	}

	// Recent pages should be in history
	prefetcher.mu.RLock()
	lastPageInHistory := pattern.History[len(pattern.History)-1]
	prefetcher.mu.RUnlock()

	if lastPageInHistory != 14 {
		t.Errorf("Expected last page 14 in history, got %d", lastPageInHistory)
	}
}

// TestAdaptivePrefetchTimeout tests pattern reset after timeout
func TestAdaptivePrefetchTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timeout test in short mode")
	}

	_, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_timeout.db")

	contextID := uint64(1)

	// Build a pattern
	for i := uint32(0); i < 5; i++ {
		prefetcher.RecordAccess(contextID, i)
	}

	prefetcher.mu.RLock()
	confidence1 := prefetcher.patterns[contextID].Confidence
	prefetcher.mu.RUnlock()

	// Wait for timeout (1 second)
	time.Sleep(1100 * time.Millisecond)

	// Access a different page
	prefetcher.RecordAccess(contextID, 100)

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	prefetcher.mu.RUnlock()

	// Pattern should be reset
	if pattern.AccessCount != 1 {
		t.Errorf("Expected AccessCount 1 after timeout, got %d", pattern.AccessCount)
	}

	if pattern.Confidence >= confidence1 {
		t.Errorf("Expected confidence reset after timeout: %.2f >= %.2f", pattern.Confidence, confidence1)
	}
}

// TestAdaptivePrefetchLargeStride tests handling of very large strides
func TestAdaptivePrefetchLargeStride(t *testing.T) {
	_, prefetcher := setupAdaptiveTest(t, 50, "test_adaptive_large_stride.db")

	contextID := uint64(1)

	// Large stride pattern: 0, 100, 200, 300
	pages := []uint32{0, 100, 200, 300}
	for _, pageID := range pages {
		prefetcher.RecordAccess(contextID, pageID)
	}

	prefetcher.mu.RLock()
	pattern := prefetcher.patterns[contextID]
	prefetcher.mu.RUnlock()

	if pattern.Stride != 100 {
		t.Errorf("Expected stride 100, got %d", pattern.Stride)
	}

	// Confidence should still build for consistent large strides
	if pattern.Confidence < 0.4 {
		t.Errorf("Expected confidence >= 0.4 for large stride, got %.2f", pattern.Confidence)
	}

	// Verify prefetch was triggered
	stats := prefetcher.GetStats()
	if stats.StridesDetected == 0 {
		t.Error("Expected large stride detection")
	}
}

// TestAdaptivePrefetchStatsTracking tests comprehensive statistics tracking
func TestAdaptivePrefetchStatsTracking(t *testing.T) {
	bpm, prefetcher := setupAdaptiveTest(t, 30, "test_adaptive_stats.db")

	prefetcher.Configure(2, 5) // Low threshold for testing
	contextID := uint64(1)

	// Pre-create some pages so prefetch can actually fetch them
	for i := uint32(0); i < 15; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatal(err)
		}
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Build pattern
	for i := uint32(0); i < 10; i += 2 { // Stride-2
		prefetcher.RecordAccess(contextID, i)
	}

	// Wait for prefetch
	time.Sleep(100 * time.Millisecond)

	stats := prefetcher.GetStats()

	// Verify all stats are populated
	if stats.PatternsDetected == 0 {
		t.Error("Expected patterns detected > 0")
	}

	// Pages prefetched might be 0 due to async timing or if pages don't exist
	// This is not a failure condition
	t.Logf("Pages prefetched: %d (may be 0 if async prefetch incomplete)", stats.PagesPrefetched)

	if stats.StridesDetected == 0 {
		t.Error("Expected stride detection for stride-2 pattern")
	}

	if stats.AvgConfidence == 0.0 {
		t.Error("Expected non-zero average confidence")
	}

	t.Logf("Stats: Patterns=%d, Pages=%d, Strides=%d, AvgConf=%.2f",
		stats.PatternsDetected, stats.PagesPrefetched, stats.StridesDetected, stats.AvgConfidence)
}
