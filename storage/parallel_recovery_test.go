package storage

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Helper function to create test log manager
func createTestLogManager(t *testing.T) *LogManager {
	tmpFile := fmt.Sprintf("test_log_%d.log", time.Now().UnixNano())
	lm, err := NewLogManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	t.Cleanup(func() {
		lm.Close()
		os.Remove(tmpFile)
	})
	return lm
}

// TestParallelRecoveryBasic tests basic parallel recovery
func TestParallelRecoveryBasic(t *testing.T) {
	logManager := createTestLogManager(t)
	config := DefaultParallelRecoveryConfig()
	rm := NewParallelRecoveryManager(logManager, config)

	// Create committed transaction logs
	txn1 := uint64(1)

	// Insert
	logManager.AppendLog(&LogRecord{
		LSN: 1,
		TxnID: txn1,
		PageID: 100,
		Type: LogInsert,
		AfterData: []byte("data1"),
	})

	// Update
	logManager.AppendLog(&LogRecord{
		LSN: 2,
		TxnID: txn1,
		PageID: 100,
		Type: LogUpdate,
		BeforeData: []byte("data1"),
		AfterData: []byte("data2"),
	})

	// Commit
	logManager.AppendLog(&LogRecord{
		LSN: 3,
		TxnID: txn1,
		Type: LogCommit,
	})

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Parallel recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 1 {
		t.Errorf("Expected 1 committed txn, got %d", stats.CommittedTxns)
	}
	if stats.RedoOperations != 2 {
		t.Errorf("Expected 2 redo ops, got %d", stats.RedoOperations)
	}
	if stats.UndoOperations != 0 {
		t.Errorf("Expected 0 undo ops, got %d", stats.UndoOperations)
	}
}

// TestParallelRecoveryUncommitted tests recovery with uncommitted transactions
func TestParallelRecoveryUncommitted(t *testing.T) {
	logManager := createTestLogManager(t)
	config := DefaultParallelRecoveryConfig()
	rm := NewParallelRecoveryManager(logManager, config)

	// Create uncommitted transaction
	txn1 := uint64(1)

	logManager.AppendLog(&LogRecord{
		LSN: 1,
		TxnID: txn1,
		PageID: 100,
		Type: LogInsert,
		AfterData: []byte("data1"),
	})

	logManager.AppendLog(&LogRecord{
		LSN: 2,
		TxnID: txn1,
		PageID: 100,
		Type: LogUpdate,
		BeforeData: []byte("data1"),
		AfterData: []byte("data2"),
	})

	// No commit - transaction remains uncommitted

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Parallel recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.UncommittedTxns != 1 {
		t.Errorf("Expected 1 uncommitted txn, got %d", stats.UncommittedTxns)
	}
	if stats.RedoOperations != 0 {
		t.Errorf("Expected 0 redo ops, got %d", stats.RedoOperations)
	}
	if stats.UndoOperations != 2 {
		t.Errorf("Expected 2 undo ops, got %d", stats.UndoOperations)
	}
}

// TestParallelRecoveryMultiplePages tests recovery across multiple pages
func TestParallelRecoveryMultiplePages(t *testing.T) {
	logManager := createTestLogManager(t)
	config := ParallelRecoveryConfig{NumWorkers: 4}
	rm := NewParallelRecoveryManager(logManager, config)

	// Create transactions on different pages
	for txnID := uint64(1); txnID <= 10; txnID++ {
		for pageID := uint32(100); pageID < 110; pageID++ {
			logManager.AppendLog(&LogRecord{
				LSN: txnID*100 + uint64(pageID),
				TxnID: txnID,
				PageID: pageID,
				Type: LogInsert,
				AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
			})
		}

		// Commit transaction
		logManager.AppendLog(&LogRecord{
			LSN: txnID * 1000,
			TxnID: txnID,
			Type: LogCommit,
		})
	}

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Parallel recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 10 {
		t.Errorf("Expected 10 committed txns, got %d", stats.CommittedTxns)
	}
	if stats.RedoOperations != 100 { // 10 txns × 10 pages
		t.Errorf("Expected 100 redo ops, got %d", stats.RedoOperations)
	}
}

// TestParallelRecoveryMixedWorkload tests mixed committed/uncommitted
func TestParallelRecoveryMixedWorkload(t *testing.T) {
	logManager := createTestLogManager(t)
	config := DefaultParallelRecoveryConfig()
	rm := NewParallelRecoveryManager(logManager, config)

	// Committed transactions
	for txnID := uint64(1); txnID <= 5; txnID++ {
		logManager.AppendLog(&LogRecord{
			LSN: txnID * 10,
			TxnID: txnID,
			PageID: uint32(100 + txnID),
			Type: LogInsert,
			AfterData: []byte(fmt.Sprintf("data%d", txnID)),
		})

		logManager.AppendLog(&LogRecord{
			LSN: txnID*10 + 1,
			TxnID: txnID,
			Type: LogCommit,
		})
	}

	// Uncommitted transactions
	for txnID := uint64(6); txnID <= 10; txnID++ {
		logManager.AppendLog(&LogRecord{
			LSN: txnID * 10,
			TxnID: txnID,
			PageID: uint32(100 + txnID),
			Type: LogInsert,
			AfterData: []byte(fmt.Sprintf("data%d", txnID)),
		})
		// No commit
	}

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Parallel recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 5 {
		t.Errorf("Expected 5 committed txns, got %d", stats.CommittedTxns)
	}
	if stats.UncommittedTxns != 5 {
		t.Errorf("Expected 5 uncommitted txns, got %d", stats.UncommittedTxns)
	}
	if stats.RedoOperations != 5 {
		t.Errorf("Expected 5 redo ops, got %d", stats.RedoOperations)
	}
	if stats.UndoOperations != 5 {
		t.Errorf("Expected 5 undo ops, got %d", stats.UndoOperations)
	}
}

// TestPageDependencyGraph tests dependency graph construction
func TestPageDependencyGraph(t *testing.T) {
	graph := NewPageDependencyGraph()

	// Add records for different pages
	graph.AddRecord(&LogRecord{LSN: 1, PageID: 100, Type: LogInsert})
	graph.AddRecord(&LogRecord{LSN: 2, PageID: 100, Type: LogUpdate})
	graph.AddRecord(&LogRecord{LSN: 3, PageID: 101, Type: LogInsert})
	graph.AddRecord(&LogRecord{LSN: 4, PageID: 100, Type: LogDelete})

	// Check page 100 records
	records100 := graph.GetPageRecords(100)
	if len(records100) != 3 {
		t.Errorf("Expected 3 records for page 100, got %d", len(records100))
	}

	// Check page 101 records
	records101 := graph.GetPageRecords(101)
	if len(records101) != 1 {
		t.Errorf("Expected 1 record for page 101, got %d", len(records101))
	}

	// Check all pages
	allPages := graph.GetAllPages()
	if len(allPages) != 2 {
		t.Errorf("Expected 2 pages, got %d", len(allPages))
	}
}

// TestParallelRecoveryWorkerCount tests different worker counts
func TestParallelRecoveryWorkerCount(t *testing.T) {
	workerCounts := []int{1, 2, 4, 8}

	for _, numWorkers := range workerCounts {
		t.Run(fmt.Sprintf("Workers=%d", numWorkers), func(t *testing.T) {
			logManager := createTestLogManager(t)
			config := ParallelRecoveryConfig{NumWorkers: numWorkers}
			rm := NewParallelRecoveryManager(logManager, config)

			// Create workload
			for txnID := uint64(1); txnID <= 20; txnID++ {
				for pageID := uint32(100); pageID < 120; pageID++ {
					logManager.AppendLog(&LogRecord{
						LSN: txnID*1000 + uint64(pageID),
						TxnID: txnID,
						PageID: pageID,
						Type: LogInsert,
						AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
					})
				}

				logManager.AppendLog(&LogRecord{
					LSN: txnID * 10000,
					TxnID: txnID,
					Type: LogCommit,
				})
			}

			// Recover
			err := rm.ParallelRecover()
			if err != nil {
				t.Fatalf("Recovery failed with %d workers: %v", numWorkers, err)
			}

			// Verify results
			stats := rm.GetRecoveryStats()
			if stats.CommittedTxns != 20 {
				t.Errorf("Expected 20 committed txns, got %d", stats.CommittedTxns)
			}
			if stats.RedoOperations != 400 { // 20 txns × 20 pages
				t.Errorf("Expected 400 redo ops, got %d", stats.RedoOperations)
			}
		})
	}
}

// TestParallelRecoveryConcurrency tests concurrent safety
func TestParallelRecoveryConcurrency(t *testing.T) {
	logManager := createTestLogManager(t)
	config := ParallelRecoveryConfig{NumWorkers: 8}
	rm := NewParallelRecoveryManager(logManager, config)

	// Create large workload with many pages
	numTxns := 100
	numPages := 50

	for txnID := uint64(1); txnID <= uint64(numTxns); txnID++ {
		for pageID := uint32(1); pageID <= uint32(numPages); pageID++ {
			logManager.AppendLog(&LogRecord{
				LSN: txnID*10000 + uint64(pageID),
				TxnID: txnID,
				PageID: pageID,
				Type: LogInsert,
				AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
			})
		}

		logManager.AppendLog(&LogRecord{
			LSN: txnID * 100000,
			TxnID: txnID,
			Type: LogCommit,
		})
	}

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Concurrent recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	expectedRedo := numTxns * numPages
	if stats.RedoOperations != expectedRedo {
		t.Errorf("Expected %d redo ops, got %d", expectedRedo, stats.RedoOperations)
	}
}

// TestParallelRecoveryEmpty tests recovery with no logs
func TestParallelRecoveryEmpty(t *testing.T) {
	logManager := createTestLogManager(t)
	config := DefaultParallelRecoveryConfig()
	rm := NewParallelRecoveryManager(logManager, config)

	// Recover with no logs
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Empty recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 0 {
		t.Errorf("Expected 0 committed txns, got %d", stats.CommittedTxns)
	}
	if stats.RedoOperations != 0 {
		t.Errorf("Expected 0 redo ops, got %d", stats.RedoOperations)
	}
}

// TestParallelRecoveryAbortedTransactions tests aborted transactions
func TestParallelRecoveryAbortedTransactions(t *testing.T) {
	logManager := createTestLogManager(t)
	config := DefaultParallelRecoveryConfig()
	rm := NewParallelRecoveryManager(logManager, config)

	// Transaction 1: Committed
	logManager.AppendLog(&LogRecord{LSN: 1, TxnID: 1, PageID: 100, Type: LogInsert})
	logManager.AppendLog(&LogRecord{LSN: 2, TxnID: 1, Type: LogCommit})

	// Transaction 2: Aborted
	logManager.AppendLog(&LogRecord{LSN: 3, TxnID: 2, PageID: 101, Type: LogInsert})
	logManager.AppendLog(&LogRecord{LSN: 4, TxnID: 2, Type: LogAbort})

	// Transaction 3: Uncommitted (crash)
	logManager.AppendLog(&LogRecord{LSN: 5, TxnID: 3, PageID: 102, Type: LogInsert})

	// Recover
	err := rm.ParallelRecover()
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify stats
	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 1 {
		t.Errorf("Expected 1 committed txn, got %d", stats.CommittedTxns)
	}
	if stats.UncommittedTxns != 1 { // Only txn 3
		t.Errorf("Expected 1 uncommitted txn, got %d", stats.UncommittedTxns)
	}
	if stats.RedoOperations != 1 {
		t.Errorf("Expected 1 redo op, got %d", stats.RedoOperations)
	}
	if stats.UndoOperations != 1 {
		t.Errorf("Expected 1 undo op, got %d", stats.UndoOperations)
	}
}

// BenchmarkParallelRecovery benchmarks parallel recovery
func BenchmarkParallelRecovery(b *testing.B) {
	workerCounts := []int{1, 2, 4, 8}

	for _, numWorkers := range workerCounts {
		b.Run(fmt.Sprintf("Workers=%d", numWorkers), func(b *testing.B) {
			// Create workload once
			tmpFile := fmt.Sprintf("bench_log_%d.log", time.Now().UnixNano())
			logManager, err := NewLogManager(tmpFile)
			if err != nil {
				b.Fatalf("Failed to create log manager: %v", err)
			}
			defer func() {
				logManager.Close()
				os.Remove(tmpFile)
			}()

			numTxns := 100
			numPages := 50

			for txnID := uint64(1); txnID <= uint64(numTxns); txnID++ {
				for pageID := uint32(1); pageID <= uint32(numPages); pageID++ {
					logManager.AppendLog(&LogRecord{
						LSN: txnID*10000 + uint64(pageID),
						TxnID: txnID,
						PageID: pageID,
						Type: LogInsert,
						AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
					})
				}

				logManager.AppendLog(&LogRecord{
					LSN: txnID * 100000,
					TxnID: txnID,
					Type: LogCommit,
				})
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				config := ParallelRecoveryConfig{NumWorkers: numWorkers}
				rm := NewParallelRecoveryManager(logManager, config)

				err := rm.ParallelRecover()
				if err != nil {
					b.Fatalf("Recovery failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSequentialRecovery benchmarks original sequential recovery
func BenchmarkSequentialRecovery(b *testing.B) {
	// Create workload once
	tmpFile := fmt.Sprintf("bench_seq_log_%d.log", time.Now().UnixNano())
	logManager, err := NewLogManager(tmpFile)
	if err != nil {
		b.Fatalf("Failed to create log manager: %v", err)
	}
	defer func() {
		logManager.Close()
		os.Remove(tmpFile)
	}()

	numTxns := 100
	numPages := 50

	for txnID := uint64(1); txnID <= uint64(numTxns); txnID++ {
		for pageID := uint32(1); pageID <= uint32(numPages); pageID++ {
			logManager.AppendLog(&LogRecord{
				LSN: txnID*10000 + uint64(pageID),
				TxnID: txnID,
				PageID: pageID,
				Type: LogInsert,
				AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
			})
		}

		logManager.AppendLog(&LogRecord{
			LSN: txnID * 100000,
			TxnID: txnID,
			Type: LogCommit,
		})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rm := NewRecoveryManager(logManager)

		err := rm.Recover()
		if err != nil {
			b.Fatalf("Recovery failed: %v", err)
		}
	}
}

// TestParallelRecoverySpeedup measures parallel speedup
func TestParallelRecoverySpeedup(t *testing.T) {
	// Create large workload
	tmpFile := fmt.Sprintf("speedup_log_%d.log", time.Now().UnixNano())
	logManager, err := NewLogManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}
	defer func() {
		logManager.Close()
		os.Remove(tmpFile)
	}()

	numTxns := 200
	numPages := 100

	for txnID := uint64(1); txnID <= uint64(numTxns); txnID++ {
		for pageID := uint32(1); pageID <= uint32(numPages); pageID++ {
			logManager.AppendLog(&LogRecord{
				LSN: txnID*10000 + uint64(pageID),
				TxnID: txnID,
				PageID: pageID,
				Type: LogInsert,
				AfterData: []byte(fmt.Sprintf("data_%d_%d", txnID, pageID)),
			})
		}

		logManager.AppendLog(&LogRecord{
			LSN: txnID * 100000,
			TxnID: txnID,
			Type: LogCommit,
		})
	}

	// Benchmark sequential
	start := time.Now()
	rm := NewRecoveryManager(logManager)
	if err := rm.Recover(); err != nil {
		t.Fatalf("Sequential recovery failed: %v", err)
	}
	seqTime := time.Since(start)

	// Benchmark parallel with 4 workers
	start = time.Now()
	config := ParallelRecoveryConfig{NumWorkers: 4}
	prm := NewParallelRecoveryManager(logManager, config)
	err = prm.ParallelRecover()
	if err != nil {
		t.Fatalf("Parallel recovery failed: %v", err)
	}
	parTime := time.Since(start)

	speedup := float64(seqTime) / float64(parTime)

	t.Logf("Sequential recovery: %v", seqTime)
	t.Logf("Parallel recovery (4 workers): %v", parTime)
	t.Logf("Speedup: %.2fx", speedup)

	// Expect at least 2× speedup with 4 workers
	if speedup < 2.0 {
		t.Logf("Warning: Speedup %.2fx is less than expected 2.0x", speedup)
	}
}

// TestParallelRecoveryDependencyGraphConcurrency tests concurrent graph access
func TestParallelRecoveryDependencyGraphConcurrency(t *testing.T) {
	graph := NewPageDependencyGraph()

	var wg sync.WaitGroup
	numGoroutines := 10
	recordsPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < recordsPerGoroutine; j++ {
				graph.AddRecord(&LogRecord{
					LSN: uint64(id*1000 + j),
					PageID: uint32(id),
					Type: LogInsert,
				})
			}
		}(i)
	}

	wg.Wait()

	// Verify all records added
	allPages := graph.GetAllPages()
	if len(allPages) != numGoroutines {
		t.Errorf("Expected %d pages, got %d", numGoroutines, len(allPages))
	}

	for i := 0; i < numGoroutines; i++ {
		records := graph.GetPageRecords(uint32(i))
		if len(records) != recordsPerGoroutine {
			t.Errorf("Page %d: expected %d records, got %d", i, recordsPerGoroutine, len(records))
		}
	}
}
