package storage

import (
	"os"
	"sync"
	"testing"
)

// Benchmark transaction throughput with group commit enabled
func BenchmarkTransactionThroughputWithGroupCommit(b *testing.B) {
	// Setup
	dbFile := "bench_txn_gc.db"
	logFile := "bench_txn_gc.log"
	defer os.Remove(dbFile)
	defer os.Remove(logFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatal(err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	tm.enableGroupCommit = true // Explicitly enable
	defer tm.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn, err := tm.Begin()
		if err != nil {
			b.Fatal(err)
		}

		// Simulate some work
		record := &LogRecord{
			TxnID: txn.TxnID,
			Type: LogUpdate,
			PageID: uint32(i % 1000),
		}
		_, err = lm.AppendLog(record)
		if err != nil {
			b.Fatal(err)
		}

		err = tm.Commit(txn.TxnID)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	// Report group commit stats
	if stats := tm.GetGroupCommitStats(); stats != nil {
		b.ReportMetric(stats.AverageBatchSize, "avg_batch_size")
		b.ReportMetric(float64(stats.TotalFsyncs), "total_fsyncs")
		b.Logf("Group commit: %d commits, %d batches (avg %.2f), %d fsyncs",
			stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize, stats.TotalFsyncs)
	}
}

// Benchmark transaction throughput WITHOUT group commit
func BenchmarkTransactionThroughputWithoutGroupCommit(b *testing.B) {
	// Setup
	dbFile := "bench_txn_nogc.db"
	logFile := "bench_txn_nogc.log"
	defer os.Remove(dbFile)
	defer os.Remove(logFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatal(err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	tm.enableGroupCommit = false // Explicitly disable
	defer tm.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn, err := tm.Begin()
		if err != nil {
			b.Fatal(err)
		}

		// Simulate some work
		record := &LogRecord{
			TxnID: txn.TxnID,
			Type: LogUpdate,
			PageID: uint32(i % 1000),
		}
		_, err = lm.AppendLog(record)
		if err != nil {
			b.Fatal(err)
		}

		err = tm.Commit(txn.TxnID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark concurrent transactions with group commit
func BenchmarkConcurrentTransactionsWithGroupCommit(b *testing.B) {
	dbFile := "bench_concurrent_gc.db"
	logFile := "bench_concurrent_gc.log"
	defer os.Remove(dbFile)
	defer os.Remove(logFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatal(err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	tm.enableGroupCommit = true
	defer tm.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		pageID := uint32(0)
		for pb.Next() {
			txn, err := tm.Begin()
			if err != nil {
				b.Error(err)
				continue
			}

			// Simulate work
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogUpdate,
				PageID: pageID % 1000,
			}
			pageID++

			_, err = lm.AppendLog(record)
			if err != nil {
				b.Error(err)
				continue
			}

			err = tm.Commit(txn.TxnID)
			if err != nil {
				b.Error(err)
			}
		}
	})

	b.StopTimer()

	if stats := tm.GetGroupCommitStats(); stats != nil {
		b.ReportMetric(stats.AverageBatchSize, "avg_batch_size")
		b.ReportMetric(float64(stats.TotalFsyncs), "total_fsyncs")
		b.Logf("Concurrent group commit: %d commits, %d batches (avg %.2f), %d fsyncs",
			stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize, stats.TotalFsyncs)
	}
}

// Benchmark concurrent transactions WITHOUT group commit
func BenchmarkConcurrentTransactionsWithoutGroupCommit(b *testing.B) {
	dbFile := "bench_concurrent_nogc.db"
	logFile := "bench_concurrent_nogc.log"
	defer os.Remove(dbFile)
	defer os.Remove(logFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatal(err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	tm.enableGroupCommit = false
	defer tm.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		pageID := uint32(0)
		for pb.Next() {
			txn, err := tm.Begin()
			if err != nil {
				b.Error(err)
				continue
			}

			// Simulate work
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogUpdate,
				PageID: pageID % 1000,
			}
			pageID++

			_, err = lm.AppendLog(record)
			if err != nil {
				b.Error(err)
				continue
			}

			err = tm.Commit(txn.TxnID)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// Test with burst load - many commits submitted rapidly
func BenchmarkBurstLoadWithGroupCommit(b *testing.B) {
	dbFile := "bench_burst_gc.db"
	logFile := "bench_burst_gc.log"
	defer os.Remove(dbFile)
	defer os.Remove(logFile)

	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatal(err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	tm.enableGroupCommit = true
	defer tm.Close()

	b.ResetTimer()

	// Submit transactions in bursts
	batchSize := 50
	for i := 0; i < b.N; i += batchSize {
		var wg sync.WaitGroup
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				txn, err := tm.Begin()
				if err != nil {
					return
				}

				record := &LogRecord{
					TxnID: txn.TxnID,
					Type: LogUpdate,
					PageID: uint32(idx % 1000),
				}
				_, _ = lm.AppendLog(record)
				_ = tm.Commit(txn.TxnID)
			}(j)
		}
		wg.Wait()
	}

	b.StopTimer()

	if stats := tm.GetGroupCommitStats(); stats != nil {
		b.ReportMetric(stats.AverageBatchSize, "avg_batch_size")
		b.ReportMetric(float64(stats.TotalFsyncs), "total_fsyncs")
		b.Logf("Burst load: %d commits, %d batches (avg %.2f), %d fsyncs",
			stats.TotalCommits, stats.TotalBatches, stats.AverageBatchSize, stats.TotalFsyncs)
	}
}
