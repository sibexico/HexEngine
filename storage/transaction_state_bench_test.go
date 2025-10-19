package storage

import (
	"os"
	"sync"
	"testing"
)

// BenchmarkAtomicStateGet benchmarks lock-free state reads
func BenchmarkAtomicStateGet(b *testing.B) {
	logFile := "bench_state_get.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)
	txn, _ := tm.Begin()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = txn.GetState()
	}
}

// BenchmarkAtomicStateSet benchmarks lock-free state writes
func BenchmarkAtomicStateSet(b *testing.B) {
	logFile := "bench_state_set.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)
	txn, _ := tm.Begin()

	states := []TxnState{TxnRunning, TxnCommitted, TxnAborted}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn.SetState(states[i%3])
	}
}

// BenchmarkAtomicStateCAS benchmarks compare-and-swap operations
func BenchmarkAtomicStateCAS(b *testing.B) {
	logFile := "bench_state_cas.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)
	txn, _ := tm.Begin()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Alternate between successful and failed CAS
		if i%2 == 0 {
			txn.SetState(TxnRunning)
			txn.CompareAndSwapState(TxnRunning, TxnCommitted)
		} else {
			txn.SetState(TxnCommitted)
			txn.CompareAndSwapState(TxnCommitted, TxnAborted)
		}
	}
}

// BenchmarkConcurrentStateAccess benchmarks concurrent state access
func BenchmarkConcurrentStateAccess(b *testing.B) {
	logFile := "bench_concurrent_state.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)
	txn, _ := tm.Begin()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mix of reads and writes
			if txn.GetState() == TxnRunning {
				txn.SetState(TxnCommitted)
			} else {
				txn.SetState(TxnRunning)
			}
		}
	})
}

// BenchmarkStateTransitionThroughput measures state transition throughput
func BenchmarkStateTransitionThroughput(b *testing.B) {
	logFile := "bench_state_throughput.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Create multiple transactions
	numTxns := 100
	txns := make([]*Transaction, numTxns)
	for i := 0; i < numTxns; i++ {
		txns[i], _ = tm.Begin()
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	numWorkers := 10
	txnsPerWorker := b.N / numWorkers

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < txnsPerWorker; i++ {
				txn := txns[i%numTxns]
				// Simulate state transitions
				txn.SetState(TxnRunning)
				_ = txn.GetState()
				txn.SetState(TxnCommitted)
			}
		}(w)
	}

	wg.Wait()
}

// BenchmarkCASContention benchmarks CAS under high contention
func BenchmarkCASContention(b *testing.B) {
	logFile := "bench_cas_contention.wal"
	defer os.Remove(logFile)

	lm, _ := NewLogManager(logFile)
	defer lm.Close()

	tm := NewTransactionManager(lm)
	txn, _ := tm.Begin()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Keep trying CAS until it succeeds
			for !txn.CompareAndSwapState(TxnRunning, TxnCommitted) {
				// Failed, reset and try again
				txn.SetState(TxnRunning)
			}
			// Succeeded, reset for next iteration
			txn.SetState(TxnRunning)
		}
	})
}
