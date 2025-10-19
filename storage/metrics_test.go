package storage

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMetricsCreation(t *testing.T) {
	m := NewMetrics()
	if m == nil {
		t.Fatal("Metrics should not be nil")
	}

	// All counters should start at 0
	if m.GetCacheHits() != 0 {
		t.Errorf("Expected cache hits 0, got %d", m.GetCacheHits())
	}

	if m.GetCacheMisses() != 0 {
		t.Errorf("Expected cache misses 0, got %d", m.GetCacheMisses())
	}
}

func TestCacheMetrics(t *testing.T) {
	m := NewMetrics()

	// Record some hits and misses
	m.RecordCacheHit()
	m.RecordCacheHit()
	m.RecordCacheMiss()

	if m.GetCacheHits() != 2 {
		t.Errorf("Expected 2 cache hits, got %d", m.GetCacheHits())
	}

	if m.GetCacheMisses() != 1 {
		t.Errorf("Expected 1 cache miss, got %d", m.GetCacheMisses())
	}

	hitRate := m.GetCacheHitRate()
	expected := 2.0 / 3.0
	if hitRate < expected-0.01 || hitRate > expected+0.01 {
		t.Errorf("Expected hit rate %.2f, got %.2f", expected, hitRate)
	}
}

func TestPageEvictionMetrics(t *testing.T) {
	m := NewMetrics()

	m.RecordPageEviction()
	m.RecordPageEviction()
	m.RecordDirtyPageFlush()

	if m.GetPageEvictions() != 2 {
		t.Errorf("Expected 2 page evictions, got %d", m.GetPageEvictions())
	}

	if m.GetDirtyPageFlushes() != 1 {
		t.Errorf("Expected 1 dirty page flush, got %d", m.GetDirtyPageFlushes())
	}
}

func TestTransactionMetrics(t *testing.T) {
	m := NewMetrics()

	m.RecordTxnStart()
	m.RecordTxnStart()
	m.RecordTxnStart()
	m.RecordTxnCommit()
	m.RecordTxnCommit()
	m.RecordTxnAbort()

	if m.GetTxnsStarted() != 3 {
		t.Errorf("Expected 3 txns started, got %d", m.GetTxnsStarted())
	}

	if m.GetTxnsCommitted() != 2 {
		t.Errorf("Expected 2 txns committed, got %d", m.GetTxnsCommitted())
	}

	if m.GetTxnsAborted() != 1 {
		t.Errorf("Expected 1 txn aborted, got %d", m.GetTxnsAborted())
	}
}

func TestRecoveryMetrics(t *testing.T) {
	m := NewMetrics()

	m.RecordRecovery()
	m.RecordRedoOp()
	m.RecordRedoOp()
	m.RecordRedoOp()
	m.RecordUndoOp()
	m.RecordUndoOp()

	if m.GetRecoveries() != 1 {
		t.Errorf("Expected 1 recovery, got %d", m.GetRecoveries())
	}

	if m.GetRedoOps() != 3 {
		t.Errorf("Expected 3 redo ops, got %d", m.GetRedoOps())
	}

	if m.GetUndoOps() != 2 {
		t.Errorf("Expected 2 undo ops, got %d", m.GetUndoOps())
	}
}

func TestMetricsUptime(t *testing.T) {
	m := NewMetrics()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	uptime := m.GetUptime()
	if uptime < 10*time.Millisecond {
		t.Errorf("Expected uptime >= 10ms, got %v", uptime)
	}
}

func TestMetricsReset(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.RecordTxnStart()

	// Reset
	m.Reset()

	// Everything should be back to 0
	if m.GetCacheHits() != 0 {
		t.Errorf("Expected cache hits 0 after reset, got %d", m.GetCacheHits())
	}

	if m.GetCacheMisses() != 0 {
		t.Errorf("Expected cache misses 0 after reset, got %d", m.GetCacheMisses())
	}

	if m.GetTxnsStarted() != 0 {
		t.Errorf("Expected txns started 0 after reset, got %d", m.GetTxnsStarted())
	}
}

func TestMetricsLogging(t *testing.T) {
	m := NewMetrics()

	// Record some metrics
	m.RecordCacheHit()
	m.RecordCacheHit()
	m.RecordCacheMiss()
	m.RecordTxnStart()
	m.RecordTxnCommit()

	// Create logger (output to /dev/null for test)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Should not panic
	m.LogMetrics(logger)
}

func TestCacheHitRateEdgeCases(t *testing.T) {
	m := NewMetrics()

	// No hits or misses - should return 0.0
	if m.GetCacheHitRate() != 0.0 {
		t.Errorf("Expected 0.0 hit rate with no operations, got %.2f", m.GetCacheHitRate())
	}

	// Only hits
	m.RecordCacheHit()
	m.RecordCacheHit()

	if m.GetCacheHitRate() != 1.0 {
		t.Errorf("Expected 1.0 hit rate with only hits, got %.2f", m.GetCacheHitRate())
	}

	// Reset and only misses
	m.Reset()
	m.RecordCacheMiss()
	m.RecordCacheMiss()

	if m.GetCacheHitRate() != 0.0 {
		t.Errorf("Expected 0.0 hit rate with only misses, got %.2f", m.GetCacheHitRate())
	}
}
