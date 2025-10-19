package storage

import (
	"os"
	"sync"
	"testing"
)

// TestAtomicStateTransitions tests lock-free state transitions
func TestAtomicStateTransitions(t *testing.T) {
	logFile := "test_atomic_state.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Test GetState
	if txn.GetState() != TxnRunning {
		t.Errorf("Expected state TxnRunning, got %v", txn.GetState())
	}

	// Test SetState
	txn.SetState(TxnCommitted)
	if txn.GetState() != TxnCommitted {
		t.Errorf("Expected state TxnCommitted after SetState, got %v", txn.GetState())
	}

	// Test CompareAndSwapState - successful swap
	if !txn.CompareAndSwapState(TxnCommitted, TxnAborted) {
		t.Error("CompareAndSwapState should succeed when current state matches")
	}
	if txn.GetState() != TxnAborted {
		t.Errorf("Expected state TxnAborted after CAS, got %v", txn.GetState())
	}

	// Test CompareAndSwapState - failed swap
	if txn.CompareAndSwapState(TxnRunning, TxnCommitted) {
		t.Error("CompareAndSwapState should fail when current state doesn't match")
	}
	if txn.GetState() != TxnAborted {
		t.Errorf("State should remain TxnAborted after failed CAS, got %v", txn.GetState())
	}
}

// TestConcurrentStateAccess tests concurrent state reads/writes
func TestConcurrentStateAccess(t *testing.T) {
	logFile := "test_concurrent_state.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Launch multiple goroutines that read state concurrently
	var wg sync.WaitGroup
	numReaders := 100

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			// Just reading state - should never panic or deadlock
			for j := 0; j < 100; j++ {
				_ = txn.GetState()
			}
		}()
	}

	// One goroutine updates state
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			txn.SetState(TxnRunning)
			txn.SetState(TxnCommitted)
			txn.SetState(TxnAborted)
		}
	}()

	wg.Wait()

	// Test should complete without data races
}

// TestCompareAndSwapRace tests CAS under race conditions
func TestCompareAndSwapRace(t *testing.T) {
	logFile := "test_cas_race.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Multiple goroutines try to transition from Running to Committed
	var wg sync.WaitGroup
	numGoroutines := 10
	successCount := 0
	var mu sync.Mutex

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if txn.CompareAndSwapState(TxnRunning, TxnCommitted) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one goroutine should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful CAS, got %d", successCount)
	}

	if txn.GetState() != TxnCommitted {
		t.Errorf("Expected final state TxnCommitted, got %v", txn.GetState())
	}
}

// TestStateTransitionDuringCommit tests that commit properly transitions state
func TestStateTransitionDuringCommit(t *testing.T) {
	logFile := "test_state_commit.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	initialState := txn.GetState()
	if initialState != TxnRunning {
		t.Errorf("Expected initial state TxnRunning, got %v", initialState)
	}

	// Commit transaction
	err = tm.Commit(txn.TxnID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	finalState := txn.GetState()
	if finalState != TxnCommitted {
		t.Errorf("Expected final state TxnCommitted, got %v", finalState)
	}
}

// TestStateTransitionDuringAbort tests that abort properly transitions state
func TestStateTransitionDuringAbort(t *testing.T) {
	logFile := "test_state_abort.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	tm := NewTransactionManager(lm)

	txn, err := tm.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	initialState := txn.GetState()
	if initialState != TxnRunning {
		t.Errorf("Expected initial state TxnRunning, got %v", initialState)
	}

	// Abort transaction
	err = tm.Abort(txn.TxnID)
	if err != nil {
		t.Fatalf("Failed to abort transaction: %v", err)
	}

	finalState := txn.GetState()
	if finalState != TxnAborted {
		t.Errorf("Expected final state TxnAborted, got %v", finalState)
	}
}
