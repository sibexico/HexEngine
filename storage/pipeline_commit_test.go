package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Helper to create test environment
func createTestPipelineCommit(t *testing.T, filename string) (*PipelineCommitManager, *LogManager, *TransactionManager, func()) {
	// Create disk manager
	dm, err := NewDiskManager(filename)
	if err != nil {
		t.Fatalf("Failed to create disk manager: %v", err)
	}

	// Create buffer pool
	_, err = NewBufferPoolManager(10, dm)
	if err != nil {
		dm.Close()
		t.Fatalf("Failed to create buffer pool: %v", err)
	}

	// Create log manager
	logFile := filename + ".log"
	lm, err := NewLogManager(logFile)
	if err != nil {
		dm.Close()
		t.Fatalf("Failed to create log manager: %v", err)
	}

	// Create transaction manager
	tm := NewTransactionManager(lm)

	// Create pipeline commit manager
	pcm := NewPipelineCommitManager(lm, tm, 10, 10*time.Millisecond)

	cleanup := func() {
		pcm.Shutdown()
		lm.Close()
		dm.Close()
		os.Remove(filename)
		os.Remove(logFile)
	}

	return pcm, lm, tm, cleanup
}

// TestPipelineCommitBasic tests basic pipeline functionality
func TestPipelineCommitBasic(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_basic.db")
	defer cleanup()

	// Begin transaction
	txn, _ := tm.Begin()

	// Simulate some work
	record := &LogRecord{
		TxnID: txn.TxnID,
		Type: LogUpdate,
	}
	lsn, err := pcm.logManager.AppendLog(record)
	if err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Commit through pipeline
	err = pcm.Commit(txn.TxnID, lsn)
	if err != nil {
		t.Fatalf("Pipeline commit failed: %v", err)
	}

	// Verify stats
	stats := pcm.GetStats()
	if stats.TotalCommits.Load() != 1 {
		t.Errorf("Expected 1 commit, got %d", stats.TotalCommits.Load())
	}
	if stats.TotalFsyncs.Load() == 0 {
		t.Errorf("Expected at least 1 fsync")
	}
}

// TestPipelineBatching tests that multiple commits are batched
func TestPipelineBatching(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_batching.db")
	defer cleanup()

	numTxns := 20
	txns := make([]*Transaction, numTxns)
	lsns := make([]uint64, numTxns)

	// Create transactions
	for i := 0; i < numTxns; i++ {
		txns[i], _ = tm.Begin()
		record := &LogRecord{
			TxnID: txns[i].TxnID,
			Type: LogUpdate,
		}
		lsn, err := pcm.logManager.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
		lsns[i] = lsn
	}

	// Commit all transactions concurrently
	var wg sync.WaitGroup
	errors := make(chan error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := pcm.Commit(txns[idx].TxnID, lsns[idx])
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Commit error: %v", err)
	}

	// Give pipeline time to complete
	time.Sleep(50 * time.Millisecond)

	// Verify batching occurred
	stats := pcm.GetStats()
	avgBatchSize := pcm.GetAverageBatchSize()

	t.Logf("Batching test: %d commits, %d batches, avg batch size %.2f, %d fsyncs",
		stats.TotalCommits.Load(), stats.TotalBatches.Load(), avgBatchSize, stats.TotalFsyncs.Load())

	if stats.TotalCommits.Load() != uint64(numTxns) {
		t.Errorf("Expected %d commits, got %d", numTxns, stats.TotalCommits.Load())
	}

	// Should have fewer batches than commits (due to batching)
	if stats.TotalBatches.Load() >= uint64(numTxns) {
		t.Errorf("Expected fewer batches than commits, got %d batches for %d commits",
			stats.TotalBatches.Load(), numTxns)
	}

	// Average batch size should be > 1
	if avgBatchSize <= 1.0 {
		t.Errorf("Expected average batch size > 1, got %.2f", avgBatchSize)
	}
}

// TestPipelineTimeout tests that batches are flushed on timeout
func TestPipelineTimeout(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_timeout.db")
	defer cleanup()

	// Commit single transaction
	txn, _ := tm.Begin()
	record := &LogRecord{
		TxnID: txn.TxnID,
		Type: LogUpdate,
	}
	lsn, _ := pcm.logManager.AppendLog(record)

	start := time.Now()
	err := pcm.Commit(txn.TxnID, lsn)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Should complete within reasonable time (timeout + processing)
	if duration > 100*time.Millisecond {
		t.Errorf("Commit took too long: %v", duration)
	}

	t.Logf("Single commit completed in %v", duration)

	// Verify it was committed
	stats := pcm.GetStats()
	if stats.TotalCommits.Load() != 1 {
		t.Errorf("Expected 1 commit, got %d", stats.TotalCommits.Load())
	}
}

// TestPipelineConcurrent tests concurrent commits
func TestPipelineConcurrent(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_concurrent.db")
	defer cleanup()

	numWorkers := 10
	commitsPerWorker := 10

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*commitsPerWorker)

	// Launch workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < commitsPerWorker; i++ {
				txn, _ := tm.Begin()
				record := &LogRecord{
					TxnID: txn.TxnID,
					Type: LogUpdate,
				}
				lsn, err := pcm.logManager.AppendLog(record)
				if err != nil {
					errors <- fmt.Errorf("worker %d: failed to append log: %w", workerID, err)
					continue
				}

				err = pcm.Commit(txn.TxnID, lsn)
				if err != nil {
					errors <- fmt.Errorf("worker %d: commit failed: %w", workerID, err)
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Error: %v", err)
		errorCount++
	}

	expectedCommits := numWorkers * commitsPerWorker
	stats := pcm.GetStats()
	actualCommits := stats.TotalCommits.Load()

	t.Logf("Concurrent test: %d workers × %d commits = %d total, %d batches, avg batch size %.2f, %d fsyncs",
		numWorkers, commitsPerWorker, actualCommits, stats.TotalBatches.Load(),
		pcm.GetAverageBatchSize(), stats.TotalFsyncs.Load())

	// Verify commit count (allowing for some errors)
	if actualCommits != uint64(expectedCommits-errorCount) {
		t.Errorf("Expected %d commits (minus %d errors), got %d",
			expectedCommits, errorCount, actualCommits)
	}
}

// TestPipelineShutdown tests graceful shutdown
func TestPipelineShutdown(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_shutdown.db")
	defer cleanup()

	// Commit a transaction
	txn, _ := tm.Begin()
	record := &LogRecord{
		TxnID: txn.TxnID,
		Type: LogUpdate,
	}
	lsn, _ := pcm.logManager.AppendLog(record)

	err := pcm.Commit(txn.TxnID, lsn)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Shutdown
	pcm.Shutdown()

	// Try to commit after shutdown - should fail
	txn2, _ := tm.Begin()
	record2 := &LogRecord{
		TxnID: txn2.TxnID,
		Type: LogUpdate,
	}
	lsn2, _ := pcm.logManager.AppendLog(record2)

	err = pcm.Commit(txn2.TxnID, lsn2)
	if err == nil {
		t.Error("Expected error after shutdown, got nil")
	}
}

// TestPipelineStats tests statistics collection
func TestPipelineStats(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_stats.db")
	defer cleanup()

	// Commit some transactions
	numCommits := 15
	for i := 0; i < numCommits; i++ {
		txn, _ := tm.Begin()
		record := &LogRecord{
			TxnID: txn.TxnID,
			Type: LogUpdate,
		}
		lsn, _ := pcm.logManager.AppendLog(record)
		err := pcm.Commit(txn.TxnID, lsn)
		if err != nil {
			t.Errorf("Commit %d failed: %v", i, err)
		}
	}

	// Wait for pipeline to drain
	time.Sleep(50 * time.Millisecond)

	// Check stats
	stats := pcm.GetStats()
	avgBatch := pcm.GetAverageBatchSize()
	avgValidation := pcm.GetAverageValidationTime()
	avgWrite := pcm.GetAverageWriteTime()
	avgFsync := pcm.GetAverageFsyncTime()
	totalTime := pcm.GetTotalPipelineTime()
	efficiency := pcm.GetPipelineEfficiency()

	t.Logf("Pipeline stats:")
	t.Logf(" Commits: %d", stats.TotalCommits.Load())
	t.Logf(" Batches: %d", stats.TotalBatches.Load())
	t.Logf(" Fsyncs: %d", stats.TotalFsyncs.Load())
	t.Logf(" Avg batch size: %.2f", avgBatch)
	t.Logf(" Avg validation time: %d ns", avgValidation)
	t.Logf(" Avg write time: %d ns", avgWrite)
	t.Logf(" Avg fsync time: %d ns", avgFsync)
	t.Logf(" Total pipeline time: %d ns", totalTime)
	t.Logf(" Pipeline efficiency: %.2f%%", efficiency*100)

	// Verify reasonable values
	if stats.TotalCommits.Load() != uint64(numCommits) {
		t.Errorf("Expected %d commits, got %d", numCommits, stats.TotalCommits.Load())
	}

	if avgFsync <= 0 {
		t.Error("Expected positive fsync time")
	}

	if totalTime <= 0 {
		t.Error("Expected positive total time")
	}
}

// TestPipelineStageOverlap tests that stages overlap correctly
func TestPipelineStageOverlap(t *testing.T) {
	pcm, _, tm, cleanup := createTestPipelineCommit(t, "test_pipeline_overlap.db")
	defer cleanup()

	// Commit transactions in waves to observe pipelining
	numWaves := 5
	txnsPerWave := 20

	for wave := 0; wave < numWaves; wave++ {
		var wg sync.WaitGroup

		for i := 0; i < txnsPerWave; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				txn, _ := tm.Begin()
				record := &LogRecord{
					TxnID: txn.TxnID,
					Type: LogUpdate,
				}
				lsn, _ := pcm.logManager.AppendLog(record)
				_ = pcm.Commit(txn.TxnID, lsn)
			}()
		}

		wg.Wait()

		// Small delay between waves
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for pipeline to drain
	time.Sleep(50 * time.Millisecond)

	stats := pcm.GetStats()
	efficiency := pcm.GetPipelineEfficiency()

	t.Logf("Overlap test: %d waves × %d txns = %d total",
		numWaves, txnsPerWave, stats.TotalCommits.Load())
	t.Logf(" Batches: %d", stats.TotalBatches.Load())
	t.Logf(" Avg batch: %.2f", pcm.GetAverageBatchSize())
	t.Logf(" Pipeline efficiency: %.2f%%", efficiency*100)

	// With good pipelining, efficiency should be reasonable
	// (though actual values depend on system performance)
	if efficiency < 0 || efficiency > 1 {
		t.Errorf("Pipeline efficiency out of range: %.2f", efficiency)
	}
}

// BenchmarkPipelineCommit benchmarks pipeline commit throughput
func BenchmarkPipelineCommit(b *testing.B) {
	pcm, _, tm, cleanup := createTestPipelineCommit(&testing.T{}, "bench_pipeline.db")
	defer cleanup()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()
		record := &LogRecord{
			TxnID: txn.TxnID,
			Type: LogUpdate,
		}
		lsn, _ := pcm.logManager.AppendLog(record)
		_ = pcm.Commit(txn.TxnID, lsn)
	}

	b.StopTimer()

	stats := pcm.GetStats()
	b.ReportMetric(float64(stats.TotalCommits.Load())/b.Elapsed().Seconds(), "commits/sec")
	b.ReportMetric(pcm.GetAverageBatchSize(), "avg_batch_size")
	b.ReportMetric(float64(pcm.GetAverageFsyncTime())/1000000, "avg_fsync_ms")
}

// BenchmarkPipelineVsGroup compares pipeline vs group commit
func BenchmarkPipelineVsGroup(b *testing.B) {
	b.Run("GroupCommit", func(b *testing.B) {
		dm, _ := NewDiskManager("bench_group.db")
		defer dm.Close()
		defer os.Remove("bench_group.db")

		lm, _ := NewLogManager("bench_group.db.log")
		defer lm.Close()
		defer os.Remove("bench_group.db.log")

		tm := NewTransactionManager(lm)
		gcm := NewGroupCommitManager(lm, 10, 10*time.Millisecond)
		defer gcm.Shutdown()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := tm.Begin()
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogUpdate,
			}
			lsn, _ := lm.AppendLog(record)
			_ = gcm.Commit(lsn)
		}
	})

	b.Run("PipelineCommit", func(b *testing.B) {
		pcm, _, tm, cleanup := createTestPipelineCommit(&testing.T{}, "bench_pipeline.db")
		defer cleanup()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, _ := tm.Begin()
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogUpdate,
			}
			lsn, _ := pcm.logManager.AppendLog(record)
			_ = pcm.Commit(txn.TxnID, lsn)
		}
	})
}

// BenchmarkPipelineConcurrent benchmarks concurrent pipeline commits
func BenchmarkPipelineConcurrent(b *testing.B) {
	pcm, _, tm, cleanup := createTestPipelineCommit(&testing.T{}, "bench_pipeline_concurrent.db")
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, _ := tm.Begin()
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogUpdate,
			}
			lsn, _ := pcm.logManager.AppendLog(record)
			_ = pcm.Commit(txn.TxnID, lsn)
		}
	})

	b.StopTimer()

	stats := pcm.GetStats()
	b.ReportMetric(float64(stats.TotalCommits.Load())/b.Elapsed().Seconds(), "commits/sec")
	b.ReportMetric(pcm.GetAverageBatchSize(), "avg_batch_size")
	b.ReportMetric(pcm.GetPipelineEfficiency()*100, "pipeline_efficiency_%")
}
