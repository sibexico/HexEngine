package storage

import (
	"os"
	"sync"
	"testing"
	"time"
)

// TestTransactionManager tests basic transaction manager initialization
func TestTransactionManager(t *testing.T) {
	logFile := "test_txn_manager.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)
	if tm == nil {
		t.Fatal("TransactionManager should not be nil")
	}

	if tm.nextTxnID.Load() != 1 {
		t.Errorf("Expected initial nextTxnID to be 1, got %d", tm.nextTxnID.Load())
	}
}

// TestBeginTransaction tests transaction creation
func TestBeginTransaction(t *testing.T) {
	logFile := "test_begin.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Begin first transaction
	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	if txn.TxnID != 1 {
		t.Errorf("Expected TxnID 1, got %d", txn.TxnID)
	}

	if txn.GetState() != TxnRunning {
		t.Errorf("Expected transaction state to be Running")
	}

	if txn.StartTime.IsZero() {
		t.Error("Transaction start time should be set")
	}

	// Begin second transaction
	txn2, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	if txn2.TxnID != 2 {
		t.Errorf("Expected TxnID 2, got %d", txn2.TxnID)
	}

	// Verify active transactions
	activeTxns := tm.GetActiveTxns()
	if len(activeTxns) != 2 {
		t.Errorf("Expected 2 active transactions, got %d", len(activeTxns))
	}
}

// TestCommitTransaction tests transaction commit
func TestCommitTransaction(t *testing.T) {
	logFile := "test_commit.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Begin and commit transaction
	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tm.Commit(txn.TxnID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify transaction is no longer active
	activeTxns := tm.GetActiveTxns()
	if len(activeTxns) != 0 {
		t.Errorf("Expected 0 active transactions after commit, got %d", len(activeTxns))
	}

	// Verify commit log was written
	logs, err := lm.ReadAllLogs()
	if err != nil {
		t.Fatalf("Failed to read logs: %v", err)
	}

	commitFound := false
	for _, log := range logs {
		if log.Type == LogCommit && log.TxnID == txn.TxnID {
			commitFound = true
			break
		}
	}

	if !commitFound {
		t.Error("Commit log record not found")
	}
}

// TestAbortTransaction tests transaction abort
func TestAbortTransaction(t *testing.T) {
	logFile := "test_abort.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Begin and abort transaction
	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tm.Abort(txn.TxnID)
	if err != nil {
		t.Fatalf("Failed to abort transaction: %v", err)
	}

	// Verify transaction is no longer active
	activeTxns := tm.GetActiveTxns()
	if len(activeTxns) != 0 {
		t.Errorf("Expected 0 active transactions after abort, got %d", len(activeTxns))
	}

	// Verify abort log was written
	logs, err := lm.ReadAllLogs()
	if err != nil {
		t.Fatalf("Failed to read logs: %v", err)
	}

	abortFound := false
	for _, log := range logs {
		if log.Type == LogAbort && log.TxnID == txn.TxnID {
			abortFound = true
			break
		}
	}

	if !abortFound {
		t.Error("Abort log record not found")
	}
}

// TestConcurrentTransactions tests multiple concurrent transactions
func TestConcurrentTransactions(t *testing.T) {
	logFile := "test_concurrent_txn.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Start 5 transactions
	txns := make([]*Transaction, 5)
	for i := 0; i < 5; i++ {
		txn, err := tm.Begin()
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}
		txns[i] = txn
	}

	// Verify all are active
	activeTxns := tm.GetActiveTxns()
	if len(activeTxns) != 5 {
		t.Errorf("Expected 5 active transactions, got %d", len(activeTxns))
	}

	// Commit txn 1, 3, 5
	tm.Commit(txns[0].TxnID)
	tm.Commit(txns[2].TxnID)
	tm.Commit(txns[4].TxnID)

	// Abort txn 2, 4
	tm.Abort(txns[1].TxnID)
	tm.Abort(txns[3].TxnID)

	// Verify all are done
	activeTxns = tm.GetActiveTxns()
	if len(activeTxns) != 0 {
		t.Errorf("Expected 0 active transactions, got %d", len(activeTxns))
	}
}

// TestTransactionThreadSafety tests thread safety with concurrent Begin/Commit/Abort
func TestTransactionThreadSafety(t *testing.T) {
	logFile := "test_thread_safety.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	var wg sync.WaitGroup
	numGoroutines := 10
	txnsPerGoroutine := 10

	// Spawn goroutines that begin and commit transactions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < txnsPerGoroutine; j++ {
				txn, err := tm.Begin()
				if err != nil {
					t.Errorf("Failed to begin transaction: %v", err)
					return
				}

				// Simulate some work
				time.Sleep(1 * time.Millisecond)

				// Randomly commit or abort
				if txn.TxnID%2 == 0 {
					tm.Commit(txn.TxnID)
				} else {
					tm.Abort(txn.TxnID)
				}
			}
		}()
	}

	wg.Wait()

	// All transactions should be done
	activeTxns := tm.GetActiveTxns()
	if len(activeTxns) != 0 {
		t.Errorf("Expected 0 active transactions after concurrent test, got %d", len(activeTxns))
	}

	// Verify next transaction ID
	expectedNextID := uint64(numGoroutines*txnsPerGoroutine + 1)
	if tm.nextTxnID.Load() != expectedNextID {
		t.Errorf("Expected nextTxnID %d, got %d", expectedNextID, tm.nextTxnID.Load())
	}
}

// TestGetSnapshot tests snapshot creation for MVCC
func TestGetSnapshot(t *testing.T) {
	logFile := "test_snapshot.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Start 3 transactions
	txn1, _ := tm.Begin() // ID: 1
	txn2, _ := tm.Begin() // ID: 2
	txn3, _ := tm.Begin() // ID: 3

	// Get snapshot for txn2
	snapshot := tm.GetSnapshot(txn2.TxnID)

	if snapshot == nil {
		t.Fatal("Snapshot should not be nil")
	}

	// XmaxSnapshot should be next transaction ID (4)
	if snapshot.XmaxSnapshot != 4 {
		t.Errorf("Expected XmaxSnapshot 4, got %d", snapshot.XmaxSnapshot)
	}

	// XminSnapshot should be oldest active (1)
	if snapshot.XminSnapshot != 1 {
		t.Errorf("Expected XminSnapshot 1, got %d", snapshot.XminSnapshot)
	}

	// ActiveTxns should contain 1, 2, 3
	if len(snapshot.ActiveTxns) != 3 {
		t.Errorf("Expected 3 active transactions in snapshot, got %d", len(snapshot.ActiveTxns))
	}

	// Commit txn1
	tm.Commit(txn1.TxnID)

	// New snapshot should not include txn1
	snapshot2 := tm.GetSnapshot(txn2.TxnID)
	if len(snapshot2.ActiveTxns) != 2 {
		t.Errorf("Expected 2 active transactions after commit, got %d", len(snapshot2.ActiveTxns))
	}

	// Cleanup
	tm.Commit(txn2.TxnID)
	tm.Commit(txn3.TxnID)
}

// TestCommitNonExistentTransaction tests error handling
func TestCommitNonExistentTransaction(t *testing.T) {
	logFile := "test_error.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	// Try to commit non-existent transaction
	err = tm.Commit(999)
	if err == nil {
		t.Error("Expected error when committing non-existent transaction")
	}

	// Try to abort non-existent transaction
	err = tm.Abort(999)
	if err == nil {
		t.Error("Expected error when aborting non-existent transaction")
	}
}
