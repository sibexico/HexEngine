package storage

import (
	"log/slog"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram tracks latency distribution with percentile support
type Histogram struct {
	samples []float64 // Latencies in microseconds
	mu sync.RWMutex
	maxSize int // Maximum samples to retain
	sorted bool // Track if samples are sorted
}

// NewHistogram creates a new histogram with a max sample size
func NewHistogram(maxSize int) *Histogram {
	if maxSize <= 0 {
		maxSize = 10000 // Default: keep last 10k samples
	}
	return &Histogram{
		samples: make([]float64, 0, maxSize),
		maxSize: maxSize,
		sorted: true,
	}
}

// Record adds a latency sample (in microseconds)
func (h *Histogram) Record(latencyUs float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// If at capacity, remove oldest sample (FIFO)
	if len(h.samples) >= h.maxSize {
		// Shift left (removes oldest)
		copy(h.samples, h.samples[1:])
		h.samples = h.samples[:len(h.samples)-1]
	}

	h.samples = append(h.samples, latencyUs)
	h.sorted = false // Adding new sample invalidates sort order
}

// Percentile calculates the given percentile (0-100)
func (h *Histogram) Percentile(p float64) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.samples) == 0 {
		return 0
	}

	// Sort if needed (lazy sorting)
	if !h.sorted {
		h.mu.RUnlock()
		h.mu.Lock()
		if !h.sorted { // Double-check after acquiring write lock
			sort.Float64s(h.samples)
			h.sorted = true
		}
		h.mu.Unlock()
		h.mu.RLock()
	}

	// Calculate index
	rank := (p / 100.0) * float64(len(h.samples)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))

	if lower == upper {
		return h.samples[lower]
	}

	// Linear interpolation between lower and upper
	weight := rank - float64(lower)
	return h.samples[lower]*(1-weight) + h.samples[upper]*weight
}

// Mean calculates the average latency
func (h *Histogram) Mean() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.samples) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range h.samples {
		sum += v
	}
	return sum / float64(len(h.samples))
}

// Min returns the minimum latency
func (h *Histogram) Min() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.samples) == 0 {
		return 0
	}

	min := h.samples[0]
	for _, v := range h.samples {
		if v < min {
			min = v
		}
	}
	return min
}

// Max returns the maximum latency
func (h *Histogram) Max() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.samples) == 0 {
		return 0
	}

	max := h.samples[0]
	for _, v := range h.samples {
		if v > max {
			max = v
		}
	}
	return max
}

// Count returns the number of samples
func (h *Histogram) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.samples)
}

// Reset clears all samples
func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = h.samples[:0]
	h.sorted = true
}

// Snapshot returns current percentile statistics
type HistogramSnapshot struct {
	Count int
	Min float64
	Max float64
	Mean float64
	P50 float64 // Median
	P95 float64
	P99 float64
	P999 float64
}

// Snapshot captures current histogram statistics
func (h *Histogram) Snapshot() HistogramSnapshot {
	return HistogramSnapshot{
		Count: h.Count(),
		Min: h.Min(),
		Max: h.Max(),
		Mean: h.Mean(),
		P50: h.Percentile(50),
		P95: h.Percentile(95),
		P99: h.Percentile(99),
		P999: h.Percentile(99.9),
	}
}

// Metrics tracks storage engine performance metrics
type Metrics struct {
	// Buffer Pool Metrics
	cacheHits atomic.Uint64
	cacheMisses atomic.Uint64
	pageEvictions atomic.Uint64
	dirtyPageFlushes atomic.Uint64

	// Transaction Metrics
	txnsStarted atomic.Uint64
	txnsCommitted atomic.Uint64
	txnsAborted atomic.Uint64

	// Recovery Metrics
	recoveries atomic.Uint64
	redoOps atomic.Uint64
	undoOps atomic.Uint64

	// Latency Histograms (microseconds)
	pageFetchLatency *Histogram // FetchPage latency
	pageFlushLatency *Histogram // FlushPage latency
	txnCommitLatency *Histogram // Transaction commit latency
	bptreeInsertLatency *Histogram // B+ tree insert latency
	bptreeSearchLatency *Histogram // B+ tree search latency

	// Timing Metrics
	startTime time.Time
	mu sync.RWMutex
}

// NewMetrics creates a new metrics tracker
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
		pageFetchLatency: NewHistogram(10000),
		pageFlushLatency: NewHistogram(10000),
		txnCommitLatency: NewHistogram(10000),
		bptreeInsertLatency: NewHistogram(10000),
		bptreeSearchLatency: NewHistogram(10000),
	}
}

// Buffer Pool Metrics

func (m *Metrics) RecordCacheHit() {
	m.cacheHits.Add(1)
}

func (m *Metrics) RecordCacheMiss() {
	m.cacheMisses.Add(1)
}

func (m *Metrics) RecordPageEviction() {
	m.pageEvictions.Add(1)
}

func (m *Metrics) RecordDirtyPageFlush() {
	m.dirtyPageFlushes.Add(1)
}

// Transaction Metrics

func (m *Metrics) RecordTxnStart() {
	m.txnsStarted.Add(1)
}

func (m *Metrics) RecordTxnCommit() {
	m.txnsCommitted.Add(1)
}

func (m *Metrics) RecordTxnAbort() {
	m.txnsAborted.Add(1)
}

// Recovery Metrics

func (m *Metrics) RecordRecovery() {
	m.recoveries.Add(1)
}

func (m *Metrics) RecordRedoOp() {
	m.redoOps.Add(1)
}

func (m *Metrics) RecordUndoOp() {
	m.undoOps.Add(1)
}

// Latency Recording Methods

// RecordPageFetchLatency records the latency of a page fetch operation
func (m *Metrics) RecordPageFetchLatency(duration time.Duration) {
	m.pageFetchLatency.Record(float64(duration.Microseconds()))
}

// RecordPageFlushLatency records the latency of a page flush operation
func (m *Metrics) RecordPageFlushLatency(duration time.Duration) {
	m.pageFlushLatency.Record(float64(duration.Microseconds()))
}

// RecordTxnCommitLatency records the latency of a transaction commit
func (m *Metrics) RecordTxnCommitLatency(duration time.Duration) {
	m.txnCommitLatency.Record(float64(duration.Microseconds()))
}

// RecordBPTreeInsertLatency records the latency of a B+ tree insert
func (m *Metrics) RecordBPTreeInsertLatency(duration time.Duration) {
	m.bptreeInsertLatency.Record(float64(duration.Microseconds()))
}

// RecordBPTreeSearchLatency records the latency of a B+ tree search
func (m *Metrics) RecordBPTreeSearchLatency(duration time.Duration) {
	m.bptreeSearchLatency.Record(float64(duration.Microseconds()))
}

// Getters

func (m *Metrics) GetCacheHits() uint64 {
	return m.cacheHits.Load()
}

func (m *Metrics) GetCacheMisses() uint64 {
	return m.cacheMisses.Load()
}

func (m *Metrics) GetCacheHitRate() float64 {
	hits := m.cacheHits.Load()
	misses := m.cacheMisses.Load()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

func (m *Metrics) GetPageEvictions() uint64 {
	return m.pageEvictions.Load()
}

func (m *Metrics) GetDirtyPageFlushes() uint64 {
	return m.dirtyPageFlushes.Load()
}

func (m *Metrics) GetTxnsStarted() uint64 {
	return m.txnsStarted.Load()
}

func (m *Metrics) GetTxnsCommitted() uint64 {
	return m.txnsCommitted.Load()
}

func (m *Metrics) GetTxnsAborted() uint64 {
	return m.txnsAborted.Load()
}

func (m *Metrics) GetRecoveries() uint64 {
	return m.recoveries.Load()
}

func (m *Metrics) GetRedoOps() uint64 {
	return m.redoOps.Load()
}

func (m *Metrics) GetUndoOps() uint64 {
	return m.undoOps.Load()
}

func (m *Metrics) GetUptime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return time.Since(m.startTime)
}

// Histogram Getters

// GetPageFetchLatency returns snapshot of page fetch latency distribution
func (m *Metrics) GetPageFetchLatency() HistogramSnapshot {
	return m.pageFetchLatency.Snapshot()
}

// GetPageFlushLatency returns snapshot of page flush latency distribution
func (m *Metrics) GetPageFlushLatency() HistogramSnapshot {
	return m.pageFlushLatency.Snapshot()
}

// GetTxnCommitLatency returns snapshot of transaction commit latency distribution
func (m *Metrics) GetTxnCommitLatency() HistogramSnapshot {
	return m.txnCommitLatency.Snapshot()
}

// GetBPTreeInsertLatency returns snapshot of B+ tree insert latency distribution
func (m *Metrics) GetBPTreeInsertLatency() HistogramSnapshot {
	return m.bptreeInsertLatency.Snapshot()
}

// GetBPTreeSearchLatency returns snapshot of B+ tree search latency distribution
func (m *Metrics) GetBPTreeSearchLatency() HistogramSnapshot {
	return m.bptreeSearchLatency.Snapshot()
}

// LogMetrics logs all metrics using structured logging
func (m *Metrics) LogMetrics(logger *slog.Logger) {
	pageFetch := m.GetPageFetchLatency()
	pageFlush := m.GetPageFlushLatency()
	txnCommit := m.GetTxnCommitLatency()

	logger.Info("Storage Engine Metrics",
		slog.Group("buffer_pool",
			slog.Uint64("cache_hits", m.GetCacheHits()),
			slog.Uint64("cache_misses", m.GetCacheMisses()),
			slog.Float64("cache_hit_rate", m.GetCacheHitRate()),
			slog.Uint64("page_evictions", m.GetPageEvictions()),
			slog.Uint64("dirty_page_flushes", m.GetDirtyPageFlushes()),
		),
		slog.Group("transactions",
			slog.Uint64("started", m.GetTxnsStarted()),
			slog.Uint64("committed", m.GetTxnsCommitted()),
			slog.Uint64("aborted", m.GetTxnsAborted()),
		),
		slog.Group("recovery",
			slog.Uint64("recoveries", m.GetRecoveries()),
			slog.Uint64("redo_ops", m.GetRedoOps()),
			slog.Uint64("undo_ops", m.GetUndoOps()),
		),
		slog.Group("latency_us",
			slog.Group("page_fetch",
				slog.Int("count", pageFetch.Count),
				slog.Float64("mean", pageFetch.Mean),
				slog.Float64("p50", pageFetch.P50),
				slog.Float64("p95", pageFetch.P95),
				slog.Float64("p99", pageFetch.P99),
			),
			slog.Group("page_flush",
				slog.Int("count", pageFlush.Count),
				slog.Float64("mean", pageFlush.Mean),
				slog.Float64("p95", pageFlush.P95),
				slog.Float64("p99", pageFlush.P99),
			),
			slog.Group("txn_commit",
				slog.Int("count", txnCommit.Count),
				slog.Float64("mean", txnCommit.Mean),
				slog.Float64("p95", txnCommit.P95),
				slog.Float64("p99", txnCommit.P99),
			),
		),
		slog.Duration("uptime", m.GetUptime()),
	)
}

// Reset resets all metrics (useful for testing)
func (m *Metrics) Reset() {
	m.cacheHits.Store(0)
	m.cacheMisses.Store(0)
	m.pageEvictions.Store(0)
	m.dirtyPageFlushes.Store(0)
	m.txnsStarted.Store(0)
	m.txnsCommitted.Store(0)
	m.txnsAborted.Store(0)
	m.recoveries.Store(0)
	m.redoOps.Store(0)
	m.undoOps.Store(0)

	// Reset histograms
	m.pageFetchLatency.Reset()
	m.pageFlushLatency.Reset()
	m.txnCommitLatency.Reset()
	m.bptreeInsertLatency.Reset()
	m.bptreeSearchLatency.Reset()

	m.mu.Lock()
	m.startTime = time.Now()
	m.mu.Unlock()
}
