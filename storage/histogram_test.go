package storage

import (
	"math"
	"testing"
	"time"
)

// TestHistogramBasic tests basic histogram operations
func TestHistogramBasic(t *testing.T) {
	h := NewHistogram(100)

	// Record some samples
	samples := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for _, s := range samples {
		h.Record(s)
	}

	// Check count
	if h.Count() != 10 {
		t.Errorf("Expected count 10, got %d", h.Count())
	}

	// Check min/max
	if h.Min() != 10 {
		t.Errorf("Expected min 10, got %.2f", h.Min())
	}
	if h.Max() != 100 {
		t.Errorf("Expected max 100, got %.2f", h.Max())
	}

	// Check mean
	mean := h.Mean()
	expectedMean := 55.0
	if math.Abs(mean-expectedMean) > 0.1 {
		t.Errorf("Expected mean %.2f, got %.2f", expectedMean, mean)
	}
}

// TestHistogramPercentiles tests percentile calculations
func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram(1000)

	// Record 1-100
	for i := 1; i <= 100; i++ {
		h.Record(float64(i))
	}

	// Test various percentiles
	tests := []struct {
		percentile float64
		expected float64
		tolerance float64
	}{
		{50, 50.5, 1.0}, // Median
		{95, 95.05, 1.0}, // 95th percentile
		{99, 99.01, 1.0}, // 99th percentile
		{0, 1.0, 0.1}, // Min
		{100, 100.0, 0.1}, // Max
	}

	for _, test := range tests {
		result := h.Percentile(test.percentile)
		if math.Abs(result-test.expected) > test.tolerance {
			t.Errorf("P%.1f: expected %.2f, got %.2f", test.percentile, test.expected, result)
		}
	}
}

// TestHistogramCapacity tests FIFO eviction when at capacity
func TestHistogramCapacity(t *testing.T) {
	h := NewHistogram(5) // Small capacity

	// Add 10 samples (should only keep last 5)
	for i := 1; i <= 10; i++ {
		h.Record(float64(i))
	}

	if h.Count() != 5 {
		t.Errorf("Expected count 5 (at capacity), got %d", h.Count())
	}

	// Should have samples 6-10 (oldest evicted)
	min := h.Min()
	if min < 6.0 {
		t.Errorf("Expected min >= 6 (oldest evicted), got %.2f", min)
	}

	max := h.Max()
	if max != 10.0 {
		t.Errorf("Expected max 10, got %.2f", max)
	}
}

// TestHistogramEmpty tests histogram with no samples
func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram(100)

	if h.Count() != 0 {
		t.Errorf("Expected count 0, got %d", h.Count())
	}

	if h.Min() != 0 {
		t.Errorf("Expected min 0 for empty histogram, got %.2f", h.Min())
	}

	if h.Max() != 0 {
		t.Errorf("Expected max 0 for empty histogram, got %.2f", h.Max())
	}

	if h.Mean() != 0 {
		t.Errorf("Expected mean 0 for empty histogram, got %.2f", h.Mean())
	}

	if h.Percentile(50) != 0 {
		t.Errorf("Expected p50 0 for empty histogram, got %.2f", h.Percentile(50))
	}
}

// TestHistogramSnapshot tests snapshot functionality
func TestHistogramSnapshot(t *testing.T) {
	h := NewHistogram(100)

	// Add samples
	for i := 1; i <= 100; i++ {
		h.Record(float64(i))
	}

	snapshot := h.Snapshot()

	if snapshot.Count != 100 {
		t.Errorf("Expected count 100, got %d", snapshot.Count)
	}

	if snapshot.Min != 1.0 {
		t.Errorf("Expected min 1, got %.2f", snapshot.Min)
	}

	if snapshot.Max != 100.0 {
		t.Errorf("Expected max 100, got %.2f", snapshot.Max)
	}

	// Check mean is around 50.5
	if math.Abs(snapshot.Mean-50.5) > 1.0 {
		t.Errorf("Expected mean ~50.5, got %.2f", snapshot.Mean)
	}

	// Check percentiles are reasonable
	if snapshot.P50 < 45 || snapshot.P50 > 55 {
		t.Errorf("Expected p50 ~50, got %.2f", snapshot.P50)
	}

	if snapshot.P95 < 90 || snapshot.P95 > 100 {
		t.Errorf("Expected p95 ~95, got %.2f", snapshot.P95)
	}

	if snapshot.P99 < 95 || snapshot.P99 > 100 {
		t.Errorf("Expected p99 ~99, got %.2f", snapshot.P99)
	}
}

// TestHistogramReset tests resetting a histogram
func TestHistogramReset(t *testing.T) {
	h := NewHistogram(100)

	// Add samples
	for i := 1; i <= 50; i++ {
		h.Record(float64(i))
	}

	if h.Count() != 50 {
		t.Errorf("Expected count 50, got %d", h.Count())
	}

	// Reset
	h.Reset()

	if h.Count() != 0 {
		t.Errorf("Expected count 0 after reset, got %d", h.Count())
	}

	if h.Mean() != 0 {
		t.Errorf("Expected mean 0 after reset, got %.2f", h.Mean())
	}
}

// TestMetricsLatencyRecording tests recording latencies in metrics
func TestMetricsLatencyRecording(t *testing.T) {
	m := NewMetrics()

	// Record some page fetch latencies
	m.RecordPageFetchLatency(100 * time.Microsecond)
	m.RecordPageFetchLatency(200 * time.Microsecond)
	m.RecordPageFetchLatency(300 * time.Microsecond)

	snapshot := m.GetPageFetchLatency()

	if snapshot.Count != 3 {
		t.Errorf("Expected 3 samples, got %d", snapshot.Count)
	}

	if snapshot.Min != 100 {
		t.Errorf("Expected min 100us, got %.2f", snapshot.Min)
	}

	if snapshot.Max != 300 {
		t.Errorf("Expected max 300us, got %.2f", snapshot.Max)
	}

	expectedMean := 200.0
	if math.Abs(snapshot.Mean-expectedMean) > 1.0 {
		t.Errorf("Expected mean 200us, got %.2f", snapshot.Mean)
	}
}

// TestMetricsMultipleHistograms tests that different operation histograms work independently
func TestMetricsMultipleHistograms(t *testing.T) {
	m := NewMetrics()

	// Record different latencies for different operations
	m.RecordPageFetchLatency(100 * time.Microsecond)
	m.RecordPageFlushLatency(1000 * time.Microsecond)
	m.RecordTxnCommitLatency(500 * time.Microsecond)

	fetchSnapshot := m.GetPageFetchLatency()
	flushSnapshot := m.GetPageFlushLatency()
	commitSnapshot := m.GetTxnCommitLatency()

	if fetchSnapshot.Count != 1 || fetchSnapshot.Mean != 100 {
		t.Errorf("Page fetch histogram incorrect: count=%d, mean=%.2f", fetchSnapshot.Count, fetchSnapshot.Mean)
	}

	if flushSnapshot.Count != 1 || flushSnapshot.Mean != 1000 {
		t.Errorf("Page flush histogram incorrect: count=%d, mean=%.2f", flushSnapshot.Count, flushSnapshot.Mean)
	}

	if commitSnapshot.Count != 1 || commitSnapshot.Mean != 500 {
		t.Errorf("Txn commit histogram incorrect: count=%d, mean=%.2f", commitSnapshot.Count, commitSnapshot.Mean)
	}
}

// TestMetricsHistogramReset tests that reset clears histograms
func TestMetricsHistogramReset(t *testing.T) {
	m := NewMetrics()

	// Record some latencies
	m.RecordPageFetchLatency(100 * time.Microsecond)
	m.RecordPageFetchLatency(200 * time.Microsecond)

	snapshot := m.GetPageFetchLatency()
	if snapshot.Count != 2 {
		t.Errorf("Expected 2 samples before reset, got %d", snapshot.Count)
	}

	// Reset metrics
	m.Reset()

	snapshot = m.GetPageFetchLatency()
	if snapshot.Count != 0 {
		t.Errorf("Expected 0 samples after reset, got %d", snapshot.Count)
	}
}

// TestHistogramConcurrency tests thread-safe histogram operations
func TestHistogramConcurrency(t *testing.T) {
	h := NewHistogram(10000)

	// Concurrent writes
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				h.Record(float64(id*100 + j))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have 1000 samples
	if h.Count() != 1000 {
		t.Errorf("Expected 1000 samples from concurrent writes, got %d", h.Count())
	}

	// Concurrent reads
	readsDone := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			_ = h.Snapshot()
			_ = h.Mean()
			_ = h.Percentile(95)
			readsDone <- true
		}()
	}

	// Wait for all reads
	for i := 0; i < 5; i++ {
		<-readsDone
	}

	t.Log("Concurrent histogram operations completed successfully")
}

// TestHistogramLargeDataset tests histogram with large dataset
func TestHistogramLargeDataset(t *testing.T) {
	h := NewHistogram(10000)

	// Add 10,000 samples
	for i := 1; i <= 10000; i++ {
		h.Record(float64(i))
	}

	snapshot := h.Snapshot()

	if snapshot.Count != 10000 {
		t.Errorf("Expected 10000 samples, got %d", snapshot.Count)
	}

	// Check percentiles
	p50 := snapshot.P50
	expectedP50 := 5000.5
	if math.Abs(p50-expectedP50) > 10 {
		t.Errorf("Expected p50 ~%.2f, got %.2f", expectedP50, p50)
	}

	p95 := snapshot.P95
	expectedP95 := 9500.5
	if math.Abs(p95-expectedP95) > 10 {
		t.Errorf("Expected p95 ~%.2f, got %.2f", expectedP95, p95)
	}

	p99 := snapshot.P99
	expectedP99 := 9900.1
	if math.Abs(p99-expectedP99) > 10 {
		t.Errorf("Expected p99 ~%.2f, got %.2f", expectedP99, p99)
	}

	t.Logf("Large dataset: p50=%.2f, p95=%.2f, p99=%.2f, p999=%.2f",
		snapshot.P50, snapshot.P95, snapshot.P99, snapshot.P999)
}

// BenchmarkHistogramRecord benchmarks recording samples
func BenchmarkHistogramRecord(b *testing.B) {
	h := NewHistogram(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Record(float64(i % 1000))
	}
}

// BenchmarkHistogramPercentile benchmarks percentile calculation
func BenchmarkHistogramPercentile(b *testing.B) {
	h := NewHistogram(10000)

	// Pre-fill with data
	for i := 0; i < 10000; i++ {
		h.Record(float64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Percentile(95)
	}
}

// BenchmarkHistogramSnapshot benchmarks snapshot creation
func BenchmarkHistogramSnapshot(b *testing.B) {
	h := NewHistogram(10000)

	// Pre-fill with data
	for i := 0; i < 10000; i++ {
		h.Record(float64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Snapshot()
	}
}
