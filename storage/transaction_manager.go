package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TxnState represents the state of a transaction
type TxnState int32 // Changed to int32 for atomic operations

const (
	TxnRunning   TxnState = 0
	TxnCommitted TxnState = 1
	TxnAborted   TxnState = 2
)

// Transaction represents a database transaction
type Transaction struct {
	TxnID     uint64
	state     atomic.Int32 // Atomic state for lock-free transitions
	StartTime time.Time
	LastLSN   uint64   // Last log record for this transaction
	UndoLog   []uint64 // LSNs for undo operations
}

// GetState returns the current transaction state (thread-safe)
func (t *Transaction) GetState() TxnState {
	return TxnState(t.state.Load())
}

// SetState sets the transaction state atomically
func (t *Transaction) SetState(newState TxnState) {
	t.state.Store(int32(newState))
}

// CompareAndSwapState atomically compares and swaps the transaction state
// Returns true if the swap was successful
func (t *Transaction) CompareAndSwapState(oldState, newState TxnState) bool {
	return t.state.CompareAndSwap(int32(oldState), int32(newState))
}

// Reset resets transaction for reuse
func (t *Transaction) Reset() {
	t.TxnID = 0
	t.SetState(TxnRunning)
	t.StartTime = time.Time{}
	t.LastLSN = 0
	t.UndoLog = t.UndoLog[:0] // Keep capacity
}

// Transaction pool for reuse
var txnPool = sync.Pool{
	New: func() interface{} {
		return &Transaction{
			UndoLog: make([]uint64, 0, 16), // Pre-allocate some capacity
		}
	},
}

// TransactionManager manages database transactions
type TransactionManager struct {
	activeTxns        map[uint64]*Transaction
	nextTxnID         atomic.Uint64 // Atomic for lock-free ID allocation
	logManager        *LogManager
	groupCommitMgr    *GroupCommitManager // Group commit for better throughput
	metrics           *Metrics            // Performance metrics
	mutex             sync.RWMutex        // Protects activeTxns map only
	enableGroupCommit bool                // Feature flag
}

// Snapshot represents a point-in-time view for MVCC
type Snapshot struct {
	XminSnapshot uint64   // Oldest active transaction
	XmaxSnapshot uint64   // Next transaction ID
	ActiveTxns   []uint64 // Active transaction IDs at snapshot time
}

// IsVisible checks if a tuple is visible to a transaction
func (s *Snapshot) IsVisible(tuple *Tuple, currentTxnID uint64) bool {
	xmin := tuple.GetXmin()
	xmax := tuple.GetXmax()

	// Check if tuple was created by current transaction
	if xmin == currentTxnID {
		// Can see own inserts unless self-deleted
		return xmax == 0 || xmax != currentTxnID
	}

	// Tuple created after snapshot - not visible
	if xmin >= s.XmaxSnapshot {
		return false
	}

	// Tuple created by active transaction - not visible
	for _, activeTxn := range s.ActiveTxns {
		if xmin == activeTxn {
			return false
		}
	}

	// Tuple not deleted - visible
	if xmax == 0 {
		return true
	}

	// Tuple deleted by current transaction - not visible
	if xmax == currentTxnID {
		return false
	}

	// Tuple deleted after snapshot - visible
	if xmax >= s.XmaxSnapshot {
		return true
	}

	// Tuple deleted by active transaction - still visible
	for _, activeTxn := range s.ActiveTxns {
		if xmax == activeTxn {
			return true
		}
	}

	// Tuple deleted by committed transaction - not visible
	return false
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(logManager *LogManager) *TransactionManager {
	tm := &TransactionManager{
		activeTxns:        make(map[uint64]*Transaction),
		logManager:        logManager,
		metrics:           NewMetrics(),
		mutex:             sync.RWMutex{},
		enableGroupCommit: true, // Enable by default
	}
	tm.nextTxnID.Store(1) // Initialize atomic counter

	// Initialize group commit manager if enabled
	if tm.enableGroupCommit {
		// Use reasonable defaults: batch up to 100 commits, max 10ms delay
		tm.groupCommitMgr = NewGroupCommitManager(logManager, 100, 10*time.Millisecond)
	}

	return tm
}

// Begin starts a new transaction
func (tm *TransactionManager) Begin() (*Transaction, error) {
	// Get transaction from pool
	txn := txnPool.Get().(*Transaction)
	txn.Reset()

	// Allocate transaction ID atomically without lock
	txn.TxnID = tm.nextTxnID.Add(1) - 1 // Add returns new value, so subtract 1
	txn.SetState(TxnRunning)
	txn.StartTime = time.Now()

	// Only lock for the activeTxns map update
	tm.mutex.Lock()
	tm.activeTxns[txn.TxnID] = txn
	tm.mutex.Unlock()

	tm.metrics.RecordTxnStart() // Track transaction start

	return txn, nil
}

// Commit commits a transaction
func (tm *TransactionManager) Commit(txnID uint64) error {
	// Get transaction reference
	tm.mutex.Lock()
	txn, exists := tm.activeTxns[txnID]
	if !exists {
		tm.mutex.Unlock()
		return ErrTxnNotFound("Commit", txnID)
	}
	tm.mutex.Unlock()

	// Write commit log record
	record := &LogRecord{
		TxnID: txnID,
		Type:  LogCommit,
	}
	lsn, err := tm.logManager.AppendLog(record)
	if err != nil {
		return fmt.Errorf("failed to append commit log: %w", err)
	}
	txn.LastLSN = lsn

	// Update transaction state
	txn.SetState(TxnCommitted)

	// Remove from active transactions
	tm.mutex.Lock()
	delete(tm.activeTxns, txnID)
	tm.mutex.Unlock()

	// Flush log to ensure durability (fsync can take 1-10ms)
	if tm.enableGroupCommit && tm.groupCommitMgr != nil {
		// Use group commit for better throughput
		err = tm.groupCommitMgr.Commit(lsn)
	} else {
		// Fall back to individual flush
		err = tm.logManager.Flush()
	}

	if err != nil {
		return fmt.Errorf("failed to flush log: %w", err)
	}

	tm.metrics.RecordTxnCommit() // Track successful commit

	// Return transaction to pool
	txnPool.Put(txn)

	return nil
}

// Abort aborts a transaction and undoes all its changes
func (tm *TransactionManager) Abort(txnID uint64) error {
	tm.mutex.Lock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		tm.mutex.Unlock()
		return ErrTxnNotFound("Abort", txnID)
	}

	// Process undo log in reverse order (LIFO - Last In First Out)
	undoLogs := make([]uint64, len(txn.UndoLog))
	copy(undoLogs, txn.UndoLog)

	// Update state atomically first to prevent new operations
	txn.SetState(TxnAborted)

	// Remove from active transactions
	delete(tm.activeTxns, txnID)
	tm.mutex.Unlock()

	// Apply undo operations (without holding transaction manager lock)
	for i := len(undoLogs) - 1; i >= 0; i-- {
		lsn := undoLogs[i]
		if err := tm.applyUndo(lsn); err != nil {
			// Log error but continue undoing other operations
			// In production, this would be logged to error log
			_ = err
		}
	}

	// Write abort log record
	record := &LogRecord{
		TxnID: txnID,
		Type:  LogAbort,
	}
	lsn, err := tm.logManager.AppendLog(record)
	if err != nil {
		return fmt.Errorf("failed to append abort log: %w", err)
	}

	// Flush abort record to ensure durability
	if err := tm.logManager.Flush(); err != nil {
		return fmt.Errorf("failed to flush abort log: %w", err)
	}

	_ = lsn // Use lsn

	tm.metrics.RecordTxnAbort() // Track transaction abort

	// Return transaction to pool
	txnPool.Put(txn)

	return nil
}

// applyUndo applies an undo operation for a single log record
func (tm *TransactionManager) applyUndo(lsn uint64) error {
	// Read the log record
	records, err := tm.logManager.ReadAllLogs()
	if err != nil {
		return fmt.Errorf("failed to read logs: %w", err)
	}

	// Find the record with matching LSN
	var targetRecord *LogRecord
	for _, record := range records {
		if record.LSN == lsn {
			targetRecord = record
			break
		}
	}

	if targetRecord == nil {
		return fmt.Errorf("log record with LSN %d not found", lsn)
	}

	// Apply undo based on operation type
	switch targetRecord.Type {
	case LogUpdate:
		// Restore the before image
		return tm.undoUpdate(targetRecord)
	case LogInsert:
		// Delete the inserted tuple
		return tm.undoInsert(targetRecord)
	case LogDelete:
		// Re-insert the deleted tuple
		return tm.undoDelete(targetRecord)
	default:
		// Nothing to undo for other log types
		return nil
	}
}

// undoUpdate restores the before image of an update
func (tm *TransactionManager) undoUpdate(record *LogRecord) error {
	// Placeholder for undo update logic
	return nil
}

// undoInsert deletes an inserted tuple
func (tm *TransactionManager) undoInsert(record *LogRecord) error {
	// Placeholder for undo insert logic
	return nil
}

// undoDelete re-inserts a deleted tuple
func (tm *TransactionManager) undoDelete(record *LogRecord) error {
	// Placeholder for undo delete logic
	return nil
}

// GetActiveTxns returns all active transaction IDs
func (tm *TransactionManager) GetActiveTxns() []uint64 {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	txns := make([]uint64, 0, len(tm.activeTxns))
	for txnID := range tm.activeTxns {
		txns = append(txns, txnID)
	}

	return txns
}

// GetSnapshot creates a snapshot for MVCC visibility checks
func (tm *TransactionManager) GetSnapshot(txnID uint64) *Snapshot {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	nextTxn := tm.nextTxnID.Load()
	snapshot := &Snapshot{
		XminSnapshot: nextTxn,
		XmaxSnapshot: nextTxn,
		ActiveTxns:   make([]uint64, 0, len(tm.activeTxns)),
	}

	// Find oldest active transaction
	for activeTxnID := range tm.activeTxns {
		if activeTxnID < snapshot.XminSnapshot {
			snapshot.XminSnapshot = activeTxnID
		}
		snapshot.ActiveTxns = append(snapshot.ActiveTxns, activeTxnID)
	}

	return snapshot
}

// GetTransaction returns a transaction by ID (for internal use)
func (tm *TransactionManager) GetTransaction(txnID uint64) (*Transaction, bool) {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	txn, exists := tm.activeTxns[txnID]
	return txn, exists
}

// RecordUndo adds an LSN to the transaction's undo log
func (tm *TransactionManager) RecordUndo(txnID uint64, lsn uint64) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		return ErrTxnNotFound("RecordUndo", txnID)
	}

	txn.UndoLog = append(txn.UndoLog, lsn)
	txn.LastLSN = lsn

	return nil
}

// GetMetrics returns the transaction manager metrics
func (tm *TransactionManager) GetMetrics() *Metrics {
	return tm.metrics
}

// Close gracefully shuts down the transaction manager
func (tm *TransactionManager) Close() error {
	if tm.groupCommitMgr != nil {
		tm.groupCommitMgr.Shutdown()
	}
	return nil
}

// GetGroupCommitStats returns group commit statistics if enabled
func (tm *TransactionManager) GetGroupCommitStats() *GroupCommitStats {
	if tm.groupCommitMgr != nil {
		stats := tm.groupCommitMgr.Stats()
		return &stats
	}
	return nil
}
