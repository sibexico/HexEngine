package storage

import (
	"fmt"
)

// RecoveryManager implements the ARIES recovery algorithm
type RecoveryManager struct {
	logManager *LogManager
	stats RecoveryStats
	metrics *Metrics // Performance metrics
}

// RecoveryStats tracks recovery statistics
type RecoveryStats struct {
	CommittedTxns int
	UncommittedTxns int
	RedoOperations int
	UndoOperations int
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(logManager *LogManager) *RecoveryManager {
	return &RecoveryManager{
		logManager: logManager,
		stats: RecoveryStats{},
		metrics: NewMetrics(), // Initialize metrics
	}
}

// Recover performs full ARIES recovery (Analysis, Redo, Undo)
func (rm *RecoveryManager) Recover() error {
	rm.metrics.RecordRecovery() // Track recovery start

	// Analysis Pass: Identify committed and uncommitted transactions
	committedTxns, uncommittedTxns, err := rm.AnalysisPass()
	if err != nil {
		return fmt.Errorf("analysis pass failed: %w", err)
	}

	// Redo Pass: Replay all committed transactions
	redoCount, err := rm.RedoPass(committedTxns)
	if err != nil {
		return fmt.Errorf("redo pass failed: %w", err)
	}

	// Undo Pass: Rollback uncommitted transactions
	undoCount, err := rm.UndoPass(uncommittedTxns)
	if err != nil {
		return fmt.Errorf("undo pass failed: %w", err)
	}

	// Update stats
	rm.stats.CommittedTxns = len(committedTxns)
	rm.stats.UncommittedTxns = len(uncommittedTxns)
	rm.stats.RedoOperations = redoCount
	rm.stats.UndoOperations = undoCount

	return nil
}

// AnalysisPass identifies committed and uncommitted transactions
func (rm *RecoveryManager) AnalysisPass() (map[uint64]bool, map[uint64]bool, error) {
	logs, err := rm.logManager.ReadAllLogs()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read logs: %w", err)
	}

	committedTxns := make(map[uint64]bool)
	uncommittedTxns := make(map[uint64]bool)
	activeTxns := make(map[uint64]bool)

	// Scan all log records
	for _, record := range logs {
		switch record.Type {
		case LogInsert, LogUpdate, LogDelete:
			// Track active transactions
			activeTxns[record.TxnID] = true
			uncommittedTxns[record.TxnID] = true

		case LogCommit:
			// Mark transaction as committed
			committedTxns[record.TxnID] = true
			delete(uncommittedTxns, record.TxnID)
			delete(activeTxns, record.TxnID)

		case LogAbort:
			// Transaction explicitly aborted
			delete(uncommittedTxns, record.TxnID)
			delete(activeTxns, record.TxnID)

		case LogCheckpoint:
			// Checkpoint marker (informational)
			continue
		}
	}

	return committedTxns, uncommittedTxns, nil
}

// RedoPass replays all operations from committed transactions
func (rm *RecoveryManager) RedoPass(committedTxns map[uint64]bool) (int, error) {
	logs, err := rm.logManager.ReadAllLogs()
	if err != nil {
		return 0, fmt.Errorf("failed to read logs: %w", err)
	}

	redoCount := 0

	// Replay operations from committed transactions
	for _, record := range logs {
		// Only redo operations from committed transactions
		if !committedTxns[record.TxnID] {
			continue
		}

		switch record.Type {
		case LogInsert:
			// Redo insert: apply AfterData
			// In a real implementation, this would write to the buffer pool/disk
			// For now, we just count the operation
			redoCount++
			rm.metrics.RecordRedoOp() // Track redo operation

		case LogUpdate:
			// Redo update: apply AfterData
			redoCount++
			rm.metrics.RecordRedoOp() // Track redo operation

		case LogDelete:
			// Redo delete: remove the tuple
			// For now, we just count the operation
			redoCount++
			rm.metrics.RecordRedoOp() // Track redo operation
		}
	}

	return redoCount, nil
}

// UndoPass rolls back operations from uncommitted transactions
func (rm *RecoveryManager) UndoPass(uncommittedTxns map[uint64]bool) (int, error) {
	logs, err := rm.logManager.ReadAllLogs()
	if err != nil {
		return 0, fmt.Errorf("failed to read logs: %w", err)
	}

	undoCount := 0

	// Process logs in reverse order for undo
	for i := len(logs) - 1; i >= 0; i-- {
		record := logs[i]

		// Only undo operations from uncommitted transactions
		if !uncommittedTxns[record.TxnID] {
			continue
		}

		switch record.Type {
		case LogInsert:
			// Undo insert: remove the inserted tuple
			// In a real implementation, this would delete from buffer pool/disk
			undoCount++
			rm.metrics.RecordUndoOp() // Track undo operation

		case LogUpdate:
			// Undo update: restore BeforeData
			undoCount++
			rm.metrics.RecordUndoOp() // Track undo operation

		case LogDelete:
			// Undo delete: restore the deleted tuple (using BeforeData)
			undoCount++
			rm.metrics.RecordUndoOp() // Track undo operation
		}
	}

	return undoCount, nil
}

// GetRecoveryStats returns recovery statistics
func (rm *RecoveryManager) GetRecoveryStats() RecoveryStats {
	return rm.stats
}

// GetMetrics returns the recovery manager metrics
func (rm *RecoveryManager) GetMetrics() *Metrics {
	return rm.metrics
}

// CreateCheckpoint creates a checkpoint in the log
func (rm *RecoveryManager) CreateCheckpoint() error {
	record := &LogRecord{
		TxnID: 0, // System transaction
		Type: LogCheckpoint,
	}

	_, err := rm.logManager.AppendLog(record)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}

	err = rm.logManager.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush checkpoint: %w", err)
	}

	return nil
}
