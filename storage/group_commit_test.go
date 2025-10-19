package storage

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroupCommitBasic(t *testing.T) {
	// Create temp log file
	logFile := "test_group_commit.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	// Create group commit manager with small batch for testing
	gcm := NewGroupCommitManager(lm, 5, 50*time.Millisecond)
	defer gcm.Shutdown()

	// Write some log records
	var lsns []uint64
	for i := 0; i < 10; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i),
		}
		lsn, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// Commit all transactions via group commit
	var wg sync.WaitGroup
	for _, lsn := range lsns {
		wg.Add(1)
		go func(l uint64) {
			defer wg.Done()
			err := gcm.Commit(l)
			if err != nil {
				t.Errorf("Commit failed for LSN %d: %v", l, err)
			}
		}(lsn)
	}

	wg.Wait()

	// Check that all logs are flushed
	flushedLSN := lm.GetFlushedLSN()
	if flushedLSN != lsns[len(lsns)-1] {
		t.Errorf("Expected flushed LSN %d, got %d", lsns[len(lsns)-1], flushedLSN)
	}

	// Verify stats
	stats := gcm.Stats()
	if stats.TotalCommits != 10 {
		t.Errorf("Expected 10 commits, got %d", stats.TotalCommits)
	}

	// Should have batched some commits (fewer batches than commits)
	if stats.TotalBatches >= stats.TotalCommits {
		t.Errorf("Expected batching: commits=%d, batches=%d", stats.TotalCommits, stats.TotalBatches)
	}

	t.Logf("Group commit stats: commits=%d, batches=%d, avg batch size=%.2f",
		stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize)
}

func TestGroupCommitBatching(t *testing.T) {
	logFile := "test_group_commit_batch.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	// Small batch size and short delay
	batchSize := 10
	gcm := NewGroupCommitManager(lm, batchSize, 10*time.Millisecond)
	defer gcm.Shutdown()

	// Create exactly 2 batches worth of commits
	numCommits := batchSize * 2
	var lsns []uint64

	for i := 0; i < numCommits; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i),
		}
		lsn, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// Submit all commits rapidly (should batch)
	var wg sync.WaitGroup
	for _, lsn := range lsns {
		wg.Add(1)
		go func(l uint64) {
			defer wg.Done()
			_ = gcm.Commit(l)
		}(lsn)
	}

	wg.Wait()

	stats := gcm.Stats()

	// Should have exactly 2 or 3 batches (depending on timing)
	if stats.TotalBatches > 3 {
		t.Errorf("Expected at most 3 batches for %d commits with batch size %d, got %d",
			numCommits, batchSize, stats.TotalBatches)
	}

	t.Logf("Batching test: commits=%d, batches=%d, avg batch size=%.2f",
		stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize)
}

func TestGroupCommitTimeout(t *testing.T) {
	logFile := "test_group_commit_timeout.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	// Large batch size, but short timeout
	gcm := NewGroupCommitManager(lm, 1000, 20*time.Millisecond)
	defer gcm.Shutdown()

	// Submit only a few commits (won't fill batch)
	var lsns []uint64
	for i := 0; i < 3; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i),
		}
		lsn, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
		lsns = append(lsns, lsn)
	}

	// Commit with delays between them
	start := time.Now()
	for _, lsn := range lsns {
		time.Sleep(5 * time.Millisecond) // Small delay
		err := gcm.Commit(lsn)
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}
	duration := time.Since(start)

	// Should have flushed via timeout, not batch size
	stats := gcm.Stats()
	if stats.TotalBatches == 0 {
		t.Error("Expected at least one batch")
	}

	// Verify all commits succeeded
	if stats.TotalCommits != 3 {
		t.Errorf("Expected 3 commits, got %d", stats.TotalCommits)
	}

	t.Logf("Timeout test took %v: commits=%d, batches=%d",
		duration, stats.TotalCommits, stats.TotalBatches)
}

func TestGroupCommitConcurrent(t *testing.T) {
	logFile := "test_group_commit_concurrent.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	gcm := NewGroupCommitManager(lm, 50, 10*time.Millisecond)
	defer gcm.Shutdown()

	// Many concurrent transactions
	numTxns := 100
	numGoroutines := 10

	var wg sync.WaitGroup
	var errors atomic.Uint64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < numTxns/numGoroutines; i++ {
				// Append log record
				record := &LogRecord{
					TxnID: uint64(goroutineID*1000 + i),
					Type: LogCommit,
					PageID: uint32(i),
				}
				lsn, err := lm.AppendLog(record)
				if err != nil {
					errors.Add(1)
					continue
				}

				// Commit via group commit
				err = gcm.Commit(lsn)
				if err != nil {
					errors.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("Had %d errors during concurrent commits", errors.Load())
	}

	stats := gcm.Stats()
	if stats.TotalCommits != uint64(numTxns) {
		t.Errorf("Expected %d commits, got %d", numTxns, stats.TotalCommits)
	}

	// Should have significant batching
	if stats.AverageBatchSize < 2.0 {
		t.Errorf("Expected average batch size >= 2.0, got %.2f", stats.AverageBatchSize)
	}

	t.Logf("Concurrent test: commits=%d, batches=%d, avg batch size=%.2f, fsyncs=%d",
		stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize, stats.TotalFsyncs)
}

func TestGroupCommitShutdown(t *testing.T) {
	logFile := "test_group_commit_shutdown.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	gcm := NewGroupCommitManager(lm, 100, 100*time.Millisecond)

	// Wait for worker goroutine to be fully started
	time.Sleep(50 * time.Millisecond)

	// Submit some commits that complete before shutdown
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i),
		}
		lsn, _ := lm.AppendLog(record)
		wg.Add(1)
		go func(l uint64) {
			defer wg.Done()
			_ = gcm.Commit(l)
		}(lsn)
	}
	wg.Wait()

	// Shutdown should flush remaining batches with timeout
	done := make(chan bool)
	go func() {
		gcm.Shutdown()
		done <- true
	}()

	select {
	case <-done:
		// Success - shutdown completed
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown timeout - worker goroutine may be stuck")
	}

	// Further commits should fail
	record := &LogRecord{
		TxnID: 999,
		Type: LogCommit,
		PageID: 999,
	}
	lsn, _ := lm.AppendLog(record)
	err = gcm.Commit(lsn)
	if err == nil {
		t.Error("Expected error after shutdown, got nil")
	}
}

// Benchmark group commit vs individual commits
func BenchmarkGroupCommit(b *testing.B) {
	logFile := "bench_group_commit.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	gcm := NewGroupCommitManager(lm, 100, 10*time.Millisecond)
	defer gcm.Shutdown()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i % 1000),
		}
		lsn, err := lm.AppendLog(record)
		if err != nil {
			b.Fatal(err)
		}

		err = gcm.Commit(lsn)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	stats := gcm.Stats()
	b.ReportMetric(stats.AverageBatchSize, "avg_batch_size")
	b.ReportMetric(float64(stats.TotalFsyncs), "total_fsyncs")
}

func BenchmarkIndividualCommit(b *testing.B) {
	logFile := "bench_individual_commit.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		record := &LogRecord{
			TxnID: uint64(i),
			Type: LogCommit,
			PageID: uint32(i % 1000),
		}
		_, err := lm.AppendLog(record)
		if err != nil {
			b.Fatal(err)
		}

		// Individual flush (old way - no group commit)
		err = lm.Flush()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupCommitParallel(b *testing.B) {
	logFile := "bench_group_commit_parallel.log"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create log manager: %v", err)
	}
	defer lm.Close()

	gcm := NewGroupCommitManager(lm, 100, 5*time.Millisecond)
	defer gcm.Shutdown()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		txnID := uint64(0)
		for pb.Next() {
			record := &LogRecord{
				TxnID: txnID,
				Type: LogCommit,
				PageID: uint32(txnID % 1000),
			}
			txnID++

			lsn, err := lm.AppendLog(record)
			if err != nil {
				b.Fatal(err)
			}

			err = gcm.Commit(lsn)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.StopTimer()
	stats := gcm.Stats()
	b.ReportMetric(stats.AverageBatchSize, "avg_batch_size")
	b.ReportMetric(float64(stats.TotalFsyncs), "total_fsyncs")
	b.Logf("Group commit stats: commits=%d, batches=%d, avg batch=%.2f, fsyncs=%d",
		stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize, stats.TotalFsyncs)
}
