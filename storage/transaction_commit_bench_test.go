package storage

import (
	"os"
	"testing"
)

// BenchmarkTransactionCommitOptimized benchmarks the optimized commit path
func BenchmarkTransactionCommitOptimized(b *testing.B) {
	logFile := "bench_commit_optimized.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, _ := tm.Begin()
		tm.Commit(txn.TxnID)
	}
}

// BenchmarkTransactionCommitConcurrent benchmarks concurrent commits with reduced lock contention
func BenchmarkTransactionCommitConcurrent(b *testing.B) {
	logFile := "bench_commit_concurrent.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, _ := tm.Begin()
			tm.Commit(txn.TxnID)
		}
	})
}

// BenchmarkTransactionBeginOptimized benchmarks the optimized Begin() with atomic ID allocation
func BenchmarkTransactionBeginOptimized(b *testing.B) {
	logFile := "bench_begin_optimized.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Begin()
	}
}

// BenchmarkTransactionBeginConcurrent benchmarks concurrent transaction starts
func BenchmarkTransactionBeginConcurrent(b *testing.B) {
	logFile := "bench_begin_concurrent.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tm.Begin()
		}
	})
}

// BenchmarkCommitWithGroupCommit benchmarks commit with group commit enabled
func BenchmarkCommitWithGroupCommit(b *testing.B) {
	logFile := "bench_group_commit.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	// Group commit is enabled by default

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, _ := tm.Begin()
			tm.Commit(txn.TxnID)
		}
	})
}

// BenchmarkLockContentionReduction measures lock contention with new design
func BenchmarkLockContentionReduction(b *testing.B) {
	logFile := "bench_lock_contention.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		b.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Run with high concurrency to stress test lock contention
	b.SetParallelism(100)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn, _ := tm.Begin()
			// Just begin - this tests the optimized Begin() path
			tm.Commit(txn.TxnID)
		}
	})
}
