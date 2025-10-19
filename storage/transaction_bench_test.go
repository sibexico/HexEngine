package storage

import (
	"os"
	"testing"
)

// Setup helper for transaction benchmarks
func setupTransactionManager(b *testing.B) (*TransactionManager, *LogManager, func()) {
	b.Helper()

	walFile := "bench_txn_wal.log"
	lm, err := NewLogManager(walFile)
	if err != nil {
		b.Fatal(err)
	}

	tm := NewTransactionManager(lm)

	cleanup := func() {
		lm.Close()
		os.Remove(walFile)
	}

	return tm, lm, cleanup
}

// Benchmark transaction begin
func BenchmarkTransactionBegin(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Begin()
	}
}

// Benchmark transaction commit
func BenchmarkTransactionCommit(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	// Pre-create transactions
	txns := make([]*Transaction, b.N)
	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()
		txns[i] = txn
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Commit(txns[i].TxnID)
	}
}

// Benchmark transaction abort
func BenchmarkTransactionAbort(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	// Pre-create transactions
	txns := make([]*Transaction, b.N)
	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()
		txns[i] = txn
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Abort(txns[i].TxnID)
	}
}

// Benchmark full transaction lifecycle
func BenchmarkTransactionLifecycle(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()
		if i%2 == 0 {
			tm.Commit(txn.TxnID)
		} else {
			tm.Abort(txn.TxnID)
		}
	}
}

// Benchmark concurrent transactions
func BenchmarkTransactionConcurrent(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, _ := tm.Begin()
			tm.Commit(txn.TxnID)
		}
	})
}

// Benchmark snapshot creation
func BenchmarkTransactionSnapshot(b *testing.B) {
	tm, _, cleanup := setupTransactionManager(b)
	defer cleanup()

	// Create some active transactions
	txnIDs := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		txn, _ := tm.Begin()
		txnIDs[i] = txn.TxnID
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txnID := txnIDs[i%100]
		tm.GetSnapshot(txnID)
	}
}

// Benchmark transaction throughput with writes
func BenchmarkTransactionThroughput(b *testing.B) {
	tm, lm, cleanup := setupTransactionManager(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()

		// Simulate writes
		for j := 0; j < 10; j++ {
			record := &LogRecord{
				TxnID: txn.TxnID,
				Type: LogInsert,
				PageID: uint32(j),
				AfterData: make([]byte, 100),
			}
			lsn, _ := lm.AppendLog(record)
			tm.RecordUndo(txn.TxnID, lsn)
		}

		tm.Commit(txn.TxnID)
	}
}

// Benchmark different transaction sizes
func BenchmarkTransactionSizes(b *testing.B) {
	sizes := []int{1, 10, 100, 1000}

	for _, size := range sizes {
		b.Run(benchName("Ops", size), func(b *testing.B) {
			tm, lm, cleanup := setupTransactionManager(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn, _ := tm.Begin()

				for j := 0; j < size; j++ {
					record := &LogRecord{
						TxnID: txn.TxnID,
						Type: LogInsert,
						PageID: uint32(j),
						AfterData: make([]byte, 50),
					}
					lsn, _ := lm.AppendLog(record)
					tm.RecordUndo(txn.TxnID, lsn)
				}

				tm.Commit(txn.TxnID)
			}
		})
	}
}
