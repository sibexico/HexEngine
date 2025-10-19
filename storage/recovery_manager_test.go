package storage

import (
	"os"
	"testing"
)

// TestRecoveryManager tests basic recovery manager initialization
func TestRecoveryManager(t *testing.T) {
	logFile := "test_recovery_mgr.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	rm := NewRecoveryManager(lm)
	if rm == nil {
		t.Fatal("RecoveryManager should not be nil")
	}
}

// TestAnalysisPass tests the analysis phase of recovery
func TestAnalysisPass(t *testing.T) {
	logFile := "test_analysis.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Write test logs
	// Txn 1: Insert and Commit
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, PageID: 10, AfterData: []byte("data1")})
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})

	// Txn 2: Insert but no commit (crash)
	lm.AppendLog(&LogRecord{TxnID: 2, Type: LogInsert, PageID: 20, AfterData: []byte("data2")})

	// Txn 3: Update and Commit
	lm.AppendLog(&LogRecord{TxnID: 3, Type: LogUpdate, PageID: 30, BeforeData: []byte("old"), AfterData: []byte("new")})
	lm.AppendLog(&LogRecord{TxnID: 3, Type: LogCommit})

	lm.Flush()

	// Run analysis
	rm := NewRecoveryManager(lm)
	committedTxns, uncommittedTxns, err := rm.AnalysisPass()
	if err != nil {
		t.Fatalf("Analysis pass failed: %v", err)
	}

	// Verify committed transactions
	if len(committedTxns) != 2 {
		t.Errorf("Expected 2 committed transactions, got %d", len(committedTxns))
	}

	if !committedTxns[1] {
		t.Error("Transaction 1 should be committed")
	}

	if !committedTxns[3] {
		t.Error("Transaction 3 should be committed")
	}

	// Verify uncommitted transactions
	if len(uncommittedTxns) != 1 {
		t.Errorf("Expected 1 uncommitted transaction, got %d", len(uncommittedTxns))
	}

	if !uncommittedTxns[2] {
		t.Error("Transaction 2 should be uncommitted")
	}
}

// TestRedoPass tests the redo phase of recovery
func TestRedoPass(t *testing.T) {
	logFile := "test_redo.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Write test logs
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, PageID: 10, AfterData: []byte("data1")})
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogUpdate, PageID: 10, BeforeData: []byte("data1"), AfterData: []byte("updated1")})
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})
	lm.Flush()

	// Run recovery
	rm := NewRecoveryManager(lm)
	committedTxns := map[uint64]bool{1: true}

	redoCount, err := rm.RedoPass(committedTxns)
	if err != nil {
		t.Fatalf("Redo pass failed: %v", err)
	}

	// Should redo 2 operations (insert and update)
	if redoCount != 2 {
		t.Errorf("Expected 2 redo operations, got %d", redoCount)
	}
}

// TestUndoPass tests the undo phase of recovery
func TestUndoPass(t *testing.T) {
	logFile := "test_undo.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Write test logs - uncommitted transaction
	lm.AppendLog(&LogRecord{TxnID: 2, Type: LogInsert, PageID: 20, AfterData: []byte("data2")})
	lm.AppendLog(&LogRecord{TxnID: 2, Type: LogUpdate, PageID: 20, BeforeData: []byte("data2"), AfterData: []byte("updated2")})
	// No commit - transaction should be undone
	lm.Flush()

	// Run recovery
	rm := NewRecoveryManager(lm)
	uncommittedTxns := map[uint64]bool{2: true}

	undoCount, err := rm.UndoPass(uncommittedTxns)
	if err != nil {
		t.Fatalf("Undo pass failed: %v", err)
	}

	// Should undo 2 operations (in reverse order)
	if undoCount != 2 {
		t.Errorf("Expected 2 undo operations, got %d", undoCount)
	}
}

// TestFullRecovery tests complete ARIES recovery process
func TestFullRecovery(t *testing.T) {
	logFile := "test_full_recovery.wal"
	defer os.Remove(logFile)

	// Simulate normal operations
	{
		lm, err := NewLogManager(logFile)
		if err != nil {
			t.Fatalf("Failed to create LogManager: %v", err)
		}

		// Txn 1: Complete (should be redone)
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, PageID: 10, AfterData: []byte("data1")})
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})

		// Txn 2: Incomplete (should be undone)
		lm.AppendLog(&LogRecord{TxnID: 2, Type: LogInsert, PageID: 20, AfterData: []byte("data2")})

		// Txn 3: Complete (should be redone)
		lm.AppendLog(&LogRecord{TxnID: 3, Type: LogUpdate, PageID: 30, BeforeData: []byte("old"), AfterData: []byte("new")})
		lm.AppendLog(&LogRecord{TxnID: 3, Type: LogCommit})

		// Txn 4: Incomplete (should be undone)
		lm.AppendLog(&LogRecord{TxnID: 4, Type: LogDelete, PageID: 40, BeforeData: []byte("deleted")})

		lm.Flush()
		lm.Close()
	}

	// Simulate crash and recovery
	{
		lm, err := NewLogManager(logFile)
		if err != nil {
			t.Fatalf("Failed to reopen LogManager: %v", err)
		}
		defer lm.Close()

		rm := NewRecoveryManager(lm)
		err = rm.Recover()
		if err != nil {
			t.Fatalf("Recovery failed: %v", err)
		}

		// Verify recovery results
		stats := rm.GetRecoveryStats()

		if stats.CommittedTxns != 2 {
			t.Errorf("Expected 2 committed transactions, got %d", stats.CommittedTxns)
		}

		if stats.UncommittedTxns != 2 {
			t.Errorf("Expected 2 uncommitted transactions, got %d", stats.UncommittedTxns)
		}

		if stats.RedoOperations != 2 {
			t.Errorf("Expected 2 redo operations, got %d", stats.RedoOperations)
		}

		if stats.UndoOperations != 2 {
			t.Errorf("Expected 2 undo operations, got %d", stats.UndoOperations)
		}
	}
}

// TestRecoveryWithCheckpoint tests recovery with checkpoint records
func TestRecoveryWithCheckpoint(t *testing.T) {
	logFile := "test_checkpoint.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Old transactions (before checkpoint)
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, PageID: 10, AfterData: []byte("old1")})
	lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})

	// Checkpoint
	lm.AppendLog(&LogRecord{TxnID: 0, Type: LogCheckpoint})

	// New transactions (after checkpoint)
	lm.AppendLog(&LogRecord{TxnID: 2, Type: LogInsert, PageID: 20, AfterData: []byte("new2")})
	lm.AppendLog(&LogRecord{TxnID: 2, Type: LogCommit})

	lm.Flush()

	// Recovery should process all records (checkpoint is informational)
	rm := NewRecoveryManager(lm)
	err = rm.Recover()
	if err != nil {
		t.Fatalf("Recovery with checkpoint failed: %v", err)
	}

	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 2 {
		t.Errorf("Expected 2 committed transactions, got %d", stats.CommittedTxns)
	}
}

// TestEmptyLogRecovery tests recovery on empty log file
func TestEmptyLogRecovery(t *testing.T) {
	logFile := "test_empty_recovery.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// No logs written
	lm.Flush()

	// Recovery should succeed with no operations
	rm := NewRecoveryManager(lm)
	err = rm.Recover()
	if err != nil {
		t.Fatalf("Recovery on empty log failed: %v", err)
	}

	stats := rm.GetRecoveryStats()
	if stats.CommittedTxns != 0 || stats.UncommittedTxns != 0 {
		t.Error("Empty log should result in 0 transactions")
	}
}
