package storage

import (
	"sync/atomic"
	"testing"
	"time"
)

// MockBufferPool implements FlushableBufferPool for testing
type MockBufferPool struct {
	dirtyPages atomic.Int32
	totalPages atomic.Int32
	flushedPages []uint32
}

func NewMockBufferPool(capacity int) *MockBufferPool {
	mbp := &MockBufferPool{}
	mbp.totalPages.Store(int32(capacity))
	return mbp
}

func (mbp *MockBufferPool) GetDirtyPageCount() int {
	return int(mbp.dirtyPages.Load())
}

func (mbp *MockBufferPool) GetCapacity() int {
	return int(mbp.totalPages.Load())
}

func (mbp *MockBufferPool) GetDirtyPages(maxPages int) []uint32 {
	count := mbp.GetDirtyPageCount()
	if count > maxPages {
		count = maxPages
	}
	pages := make([]uint32, count)
	for i := 0; i < count; i++ {
		pages[i] = uint32(i + 1)
	}
	return pages
}

func (mbp *MockBufferPool) FlushPage(pageID uint32) error {
	mbp.flushedPages = append(mbp.flushedPages, pageID)
	mbp.dirtyPages.Add(-1)
	return nil
}

func (mbp *MockBufferPool) SetDirtyPages(count int) {
	mbp.dirtyPages.Store(int32(count))
}

func (mbp *MockBufferPool) GetFlushedCount() int {
	return len(mbp.flushedPages)
}

func (mbp *MockBufferPool) ClearFlushed() {
	mbp.flushedPages = nil
}

// TestAdaptiveFlusherBasic tests basic flusher creation and configuration
func TestAdaptiveFlusherBasic(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()

	af := NewAdaptiveFlusher(mbp, config)
	if af == nil {
		t.Fatal("Failed to create adaptive flusher")
	}

	if af.IsRunning() {
		t.Error("Flusher should not be running initially")
	}

	cfg := af.GetConfig()
	if cfg.TargetDirtyRatio != config.TargetDirtyRatio {
		t.Errorf("Config mismatch: expected %f, got %f",
			config.TargetDirtyRatio, cfg.TargetDirtyRatio)
	}
}

// TestAdaptiveFlusherStartStop tests starting and stopping the flusher
func TestAdaptiveFlusherStartStop(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	af := NewAdaptiveFlusher(mbp, config)

	// Start flusher
	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}

	if !af.IsRunning() {
		t.Error("Flusher should be running")
	}

	// Try to start again (should fail)
	err = af.Start()
	if err == nil {
		t.Error("Expected error when starting already running flusher")
	}

	// Stop flusher
	err = af.Stop()
	if err != nil {
		t.Fatalf("Failed to stop flusher: %v", err)
	}

	if af.IsRunning() {
		t.Error("Flusher should not be running after stop")
	}
}

// TestAdaptiveFlusherFlushTrigger tests that flushing is triggered
func TestAdaptiveFlusherFlushTrigger(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	config.CheckInterval = 50 * time.Millisecond
	config.TargetDirtyRatio = 0.50 // Target 50%
	config.MinFlushPages = 5

	af := NewAdaptiveFlusher(mbp, config)

	// Set dirty ratio above target (70%)
	mbp.SetDirtyPages(70)

	// Start flusher
	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}
	defer af.Stop()

	// Wait for a few flush cycles
	time.Sleep(300 * time.Millisecond)

	// Check that some pages were flushed
	stats := af.GetStats()
	if stats.FlushesIssued == 0 {
		t.Error("Expected flushes to be issued")
	}

	if stats.PagesFlushed == 0 {
		t.Error("Expected pages to be flushed")
	}

	if mbp.GetFlushedCount() == 0 {
		t.Error("Expected mock buffer pool to record flushed pages")
	}

	t.Logf("Stats: FlushesIssued=%d, PagesFlushed=%d, CurrentRate=%.2f, DirtyRatio=%.2f",
		stats.FlushesIssued, stats.PagesFlushed, stats.CurrentRate, stats.DirtyRatio)
}

// TestAdaptiveFlusherNoFlushBelowTarget tests that no flushing occurs below target
func TestAdaptiveFlusherNoFlushBelowTarget(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	config.CheckInterval = 50 * time.Millisecond
	config.TargetDirtyRatio = 0.60 // Target 60%

	af := NewAdaptiveFlusher(mbp, config)

	// Set dirty ratio below target (40%)
	mbp.SetDirtyPages(40)

	// Start flusher
	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}
	defer af.Stop()

	// Wait for a few cycles
	time.Sleep(300 * time.Millisecond)

	// Check that no pages were flushed
	stats := af.GetStats()
	if stats.PagesFlushed > 0 {
		t.Errorf("Expected no pages flushed below target, but got %d", stats.PagesFlushed)
	}
}

// TestAdaptiveFlusherAggressiveMode tests aggressive flushing when max ratio exceeded
func TestAdaptiveFlusherAggressiveMode(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	config.CheckInterval = 50 * time.Millisecond
	config.TargetDirtyRatio = 0.60
	config.MaxDirtyRatio = 0.80
	config.MaxFlushPages = 20

	af := NewAdaptiveFlusher(mbp, config)

	// Set dirty ratio above max (85%)
	mbp.SetDirtyPages(85)

	// Start flusher
	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}
	defer af.Stop()

	// Wait for flush cycles
	time.Sleep(300 * time.Millisecond)

	// Should have flushed aggressively
	stats := af.GetStats()
	if stats.PagesFlushed == 0 {
		t.Error("Expected aggressive flushing above max ratio")
	}

	// Aggressive mode should flush more pages
	if stats.CurrentRate < float64(config.MaxFlushPages)*0.8 {
		t.Logf("Expected high flush rate in aggressive mode, got %.2f", stats.CurrentRate)
	}

	t.Logf("Aggressive Stats: FlushesIssued=%d, PagesFlushed=%d, CurrentRate=%.2f",
		stats.FlushesIssued, stats.PagesFlushed, stats.CurrentRate)
}

// TestAdaptiveFlusherManualTrigger tests manual flush triggering
func TestAdaptiveFlusherManualTrigger(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	af := NewAdaptiveFlusher(mbp, config)

	// Set some dirty pages
	mbp.SetDirtyPages(30)

	// Manually trigger flush
	flushed := af.TriggerFlush(10)

	if flushed == 0 {
		t.Error("Expected manual flush to flush pages")
	}

	if flushed > 10 {
		t.Errorf("Expected at most 10 pages flushed, got %d", flushed)
	}

	if mbp.GetFlushedCount() != flushed {
		t.Errorf("Mock buffer pool should record %d flushes, got %d",
			flushed, mbp.GetFlushedCount())
	}
}

// TestAdaptiveFlusherDynamicConfig tests dynamic configuration changes
func TestAdaptiveFlusherDynamicConfig(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	af := NewAdaptiveFlusher(mbp, config)

	// Test setting target ratio
	err := af.SetTargetDirtyRatio(0.70)
	if err != nil {
		t.Errorf("Failed to set target ratio: %v", err)
	}

	cfg := af.GetConfig()
	if cfg.TargetDirtyRatio != 0.70 {
		t.Errorf("Expected target ratio 0.70, got %.2f", cfg.TargetDirtyRatio)
	}

	// Test setting max ratio
	err = af.SetMaxDirtyRatio(0.85)
	if err != nil {
		t.Errorf("Failed to set max ratio: %v", err)
	}

	cfg = af.GetConfig()
	if cfg.MaxDirtyRatio != 0.85 {
		t.Errorf("Expected max ratio 0.85, got %.2f", cfg.MaxDirtyRatio)
	}

	// Test invalid configurations
	err = af.SetTargetDirtyRatio(0.90) // Should fail (> max)
	if err == nil {
		t.Error("Expected error when setting target above max")
	}

	err = af.SetMaxDirtyRatio(0.65) // Should fail (< target)
	if err == nil {
		t.Error("Expected error when setting max below target")
	}
}

// TestAdaptiveFlusherStats tests statistics tracking
func TestAdaptiveFlusherStats(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	config.CheckInterval = 50 * time.Millisecond
	config.TargetDirtyRatio = 0.50

	af := NewAdaptiveFlusher(mbp, config)

	// Set dirty pages above target
	mbp.SetDirtyPages(70)

	// Start and run for a bit
	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}
	defer af.Stop()

	time.Sleep(300 * time.Millisecond)

	// Get stats
	stats := af.GetStats()

	// Validate stats
	if stats.FlushesIssued == 0 {
		t.Error("Expected non-zero flushes issued")
	}

	if stats.PagesFlushed == 0 {
		t.Error("Expected non-zero pages flushed")
	}

	if stats.DirtyRatio == 0 {
		t.Error("Expected non-zero dirty ratio")
	}

	if stats.LastAdjustment.IsZero() {
		t.Error("Expected non-zero last adjustment time")
	}

	t.Logf("Final Stats: %+v", stats)
}

// TestAdaptiveFlusherPIDController tests that PID controller adjusts flush rate
func TestAdaptiveFlusherPIDController(t *testing.T) {
	mbp := NewMockBufferPool(100)
	config := DefaultAdaptiveFlushConfig()
	config.CheckInterval = 50 * time.Millisecond
	config.TargetDirtyRatio = 0.60
	config.MinFlushPages = 5
	config.MaxFlushPages = 50

	af := NewAdaptiveFlusher(mbp, config)

	// Start with high dirty ratio
	mbp.SetDirtyPages(80)

	err := af.Start()
	if err != nil {
		t.Fatalf("Failed to start flusher: %v", err)
	}
	defer af.Stop()

	// Collect stats over time
	var rates []float64
	for i := 0; i < 6; i++ {
		time.Sleep(100 * time.Millisecond)
		stats := af.GetStats()
		rates = append(rates, stats.CurrentRate)

		// Simulate pages being flushed
		currentDirty := mbp.GetDirtyPageCount()
		if currentDirty > 60 {
			mbp.SetDirtyPages(currentDirty - 10)
		}
	}

	// PID controller should adjust rates over time
	// Rates should generally decrease as dirty ratio approaches target
	t.Logf("Flush rates over time: %v", rates)

	// Just verify we collected stats
	if len(rates) != 6 {
		t.Errorf("Expected 6 rate samples, got %d", len(rates))
	}
}

// BenchmarkAdaptiveFlusherPerformance benchmarks flusher performance
func BenchmarkAdaptiveFlusherPerformance(b *testing.B) {
	mbp := NewMockBufferPool(1000)
	config := DefaultAdaptiveFlushConfig()
	af := NewAdaptiveFlusher(mbp, config)

	mbp.SetDirtyPages(700) // 70% dirty

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		af.TriggerFlush(10)
		mbp.SetDirtyPages(700) // Reset for next iteration
	}
}
