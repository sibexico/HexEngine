package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// ParallelRecoveryManager implements parallel ARIES recovery
type ParallelRecoveryManager struct {
	logManager *LogManager
	stats RecoveryStats
	metrics *Metrics
	numWorkers int

	// Synchronization
	wg sync.WaitGroup
	errorChan chan error
	errorOnce sync.Once
	firstError error
}

// ParallelRecoveryConfig configures parallel recovery
type ParallelRecoveryConfig struct {
	NumWorkers int // Number of parallel workers
}

// DefaultParallelRecoveryConfig returns default configuration
func DefaultParallelRecoveryConfig() ParallelRecoveryConfig {
	return ParallelRecoveryConfig{
		NumWorkers: 4, // 4 workers for parallel processing
	}
}

// NewParallelRecoveryManager creates a new parallel recovery manager
func NewParallelRecoveryManager(logManager *LogManager, config ParallelRecoveryConfig) *ParallelRecoveryManager {
	if config.NumWorkers <= 0 {
		config = DefaultParallelRecoveryConfig()
	}

	return &ParallelRecoveryManager{
		logManager: logManager,
		stats: RecoveryStats{},
		metrics: NewMetrics(),
		numWorkers: config.NumWorkers,
		errorChan: make(chan error, config.NumWorkers),
	}
}

// PageDependencyGraph tracks dependencies between log records
type PageDependencyGraph struct {
	// Map from PageID to list of log records affecting that page
	pageRecords map[uint32][]*LogRecord

	// Map from PageID to latest LSN
	pageLatestLSN map[uint32]uint64

	mu sync.RWMutex
}

// NewPageDependencyGraph creates a new dependency graph
func NewPageDependencyGraph() *PageDependencyGraph {
	return &PageDependencyGraph{
		pageRecords: make(map[uint32][]*LogRecord),
		pageLatestLSN: make(map[uint32]uint64),
	}
}

// AddRecord adds a log record to the dependency graph
func (g *PageDependencyGraph) AddRecord(record *LogRecord) {
	g.mu.Lock()
	defer g.mu.Unlock()

	pageID := record.PageID
	g.pageRecords[pageID] = append(g.pageRecords[pageID], record)

	if record.LSN > g.pageLatestLSN[pageID] {
		g.pageLatestLSN[pageID] = record.LSN
	}
}

// GetPageRecords returns all records for a page
func (g *PageDependencyGraph) GetPageRecords(pageID uint32) []*LogRecord {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pageRecords[pageID]
}

// GetAllPages returns all page IDs in the graph
func (g *PageDependencyGraph) GetAllPages() []uint32 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	pages := make([]uint32, 0, len(g.pageRecords))
	for pageID := range g.pageRecords {
		pages = append(pages, pageID)
	}
	return pages
}

// ParallelRecover performs parallel ARIES recovery
func (rm *ParallelRecoveryManager) ParallelRecover() error {
	rm.metrics.RecordRecovery()

	// Analysis Pass: Identify committed/uncommitted transactions (sequential)
	committedTxns, uncommittedTxns, err := rm.AnalysisPass()
	if err != nil {
		return fmt.Errorf("analysis pass failed: %w", err)
	}

	// Build dependency graphs
	redoGraph := NewPageDependencyGraph()
	undoGraph := NewPageDependencyGraph()

	err = rm.BuildDependencyGraphs(committedTxns, uncommittedTxns, redoGraph, undoGraph)
	if err != nil {
		return fmt.Errorf("dependency graph construction failed: %w", err)
	}

	// Parallel Redo Pass
	redoCount, err := rm.ParallelRedoPass(redoGraph)
	if err != nil {
		return fmt.Errorf("parallel redo pass failed: %w", err)
	}

	// Parallel Undo Pass
	undoCount, err := rm.ParallelUndoPass(undoGraph)
	if err != nil {
		return fmt.Errorf("parallel undo pass failed: %w", err)
	}

	// Update stats
	rm.stats.CommittedTxns = len(committedTxns)
	rm.stats.UncommittedTxns = len(uncommittedTxns)
	rm.stats.RedoOperations = int(redoCount)
	rm.stats.UndoOperations = int(undoCount)

	return nil
}

// AnalysisPass identifies committed and uncommitted transactions
func (rm *ParallelRecoveryManager) AnalysisPass() (map[uint64]bool, map[uint64]bool, error) {
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
			activeTxns[record.TxnID] = true
			uncommittedTxns[record.TxnID] = true

		case LogCommit:
			committedTxns[record.TxnID] = true
			delete(uncommittedTxns, record.TxnID)
			delete(activeTxns, record.TxnID)

		case LogAbort:
			delete(uncommittedTxns, record.TxnID)
			delete(activeTxns, record.TxnID)

		case LogCheckpoint:
			continue
		}
	}

	return committedTxns, uncommittedTxns, nil
}

// BuildDependencyGraphs builds page-level dependency graphs
func (rm *ParallelRecoveryManager) BuildDependencyGraphs(
	committedTxns map[uint64]bool,
	uncommittedTxns map[uint64]bool,
	redoGraph *PageDependencyGraph,
	undoGraph *PageDependencyGraph,
) error {
	logs, err := rm.logManager.ReadAllLogs()
	if err != nil {
		return fmt.Errorf("failed to read logs: %w", err)
	}

	// Build redo graph (committed transactions)
	for _, record := range logs {
		if committedTxns[record.TxnID] {
			switch record.Type {
			case LogInsert, LogUpdate, LogDelete:
				redoGraph.AddRecord(record)
			}
		}
	}

	// Build undo graph (uncommitted transactions)
	for _, record := range logs {
		if uncommittedTxns[record.TxnID] {
			switch record.Type {
			case LogInsert, LogUpdate, LogDelete:
				undoGraph.AddRecord(record)
			}
		}
	}

	return nil
}

// ParallelRedoPass performs parallel redo
func (rm *ParallelRecoveryManager) ParallelRedoPass(graph *PageDependencyGraph) (int64, error) {
	pages := graph.GetAllPages()
	if len(pages) == 0 {
		return 0, nil
	}

	var redoCount int64

	// Create work queue
	pageQueue := make(chan uint32, len(pages))
	for _, pageID := range pages {
		pageQueue <- pageID
	}
	close(pageQueue)

	// Reset error state
	rm.firstError = nil
	rm.errorOnce = sync.Once{}

	// Launch workers
	for i := 0; i < rm.numWorkers; i++ {
		rm.wg.Add(1)
		go rm.redoWorker(pageQueue, graph, &redoCount)
	}

	// Wait for completion
	rm.wg.Wait()

	// Check for errors
	if rm.firstError != nil {
		return 0, rm.firstError
	}

	return redoCount, nil
}

// redoWorker processes redo operations for pages
func (rm *ParallelRecoveryManager) redoWorker(
	pageQueue <-chan uint32,
	graph *PageDependencyGraph,
	redoCount *int64,
) {
	defer rm.wg.Done()

	for pageID := range pageQueue {
		// Check if error occurred
		if rm.firstError != nil {
			return
		}

		records := graph.GetPageRecords(pageID)

		// Apply records in LSN order (already sorted by AddRecord)
		for _, record := range records {
			if err := rm.applyRedo(record); err != nil {
				rm.errorOnce.Do(func() {
					rm.firstError = fmt.Errorf("redo failed for LSN %d: %w", record.LSN, err)
				})
				return
			}

			atomic.AddInt64(redoCount, 1)
			rm.metrics.RecordRedoOp()
		}
	}
}

// applyRedo applies a redo operation
func (rm *ParallelRecoveryManager) applyRedo(record *LogRecord) error {
	// In a real implementation, this would:
	// 1. Fetch the page from buffer pool
	// 2. Check if page LSN < record LSN (idempotency)
	// 3. Apply the operation using AfterData
	// 4. Update page LSN
	// 5. Mark page as dirty

	// For now, we simulate the operation
	switch record.Type {
	case LogInsert:
		// Redo insert: apply AfterData
		return nil

	case LogUpdate:
		// Redo update: apply AfterData
		return nil

	case LogDelete:
		// Redo delete: remove tuple
		return nil

	default:
		return fmt.Errorf("unexpected log type: %v", record.Type)
	}
}

// ParallelUndoPass performs parallel undo
func (rm *ParallelRecoveryManager) ParallelUndoPass(graph *PageDependencyGraph) (int64, error) {
	pages := graph.GetAllPages()
	if len(pages) == 0 {
		return 0, nil
	}

	var undoCount int64

	// Create work queue
	pageQueue := make(chan uint32, len(pages))
	for _, pageID := range pages {
		pageQueue <- pageID
	}
	close(pageQueue)

	// Reset error state
	rm.firstError = nil
	rm.errorOnce = sync.Once{}

	// Launch workers
	for i := 0; i < rm.numWorkers; i++ {
		rm.wg.Add(1)
		go rm.undoWorker(pageQueue, graph, &undoCount)
	}

	// Wait for completion
	rm.wg.Wait()

	// Check for errors
	if rm.firstError != nil {
		return 0, rm.firstError
	}

	return undoCount, nil
}

// undoWorker processes undo operations for pages
func (rm *ParallelRecoveryManager) undoWorker(
	pageQueue <-chan uint32,
	graph *PageDependencyGraph,
	undoCount *int64,
) {
	defer rm.wg.Done()

	for pageID := range pageQueue {
		// Check if error occurred
		if rm.firstError != nil {
			return
		}

		records := graph.GetPageRecords(pageID)

		// Apply records in reverse LSN order for undo
		for i := len(records) - 1; i >= 0; i-- {
			record := records[i]

			if err := rm.applyUndo(record); err != nil {
				rm.errorOnce.Do(func() {
					rm.firstError = fmt.Errorf("undo failed for LSN %d: %w", record.LSN, err)
				})
				return
			}

			atomic.AddInt64(undoCount, 1)
			rm.metrics.RecordUndoOp()
		}
	}
}

// applyUndo applies an undo operation
func (rm *ParallelRecoveryManager) applyUndo(record *LogRecord) error {
	// In a real implementation, this would:
	// 1. Fetch the page from buffer pool
	// 2. Apply the undo operation using BeforeData
	// 3. Write a CLR (Compensation Log Record)
	// 4. Mark page as dirty

	// For now, we simulate the operation
	switch record.Type {
	case LogInsert:
		// Undo insert: remove the inserted tuple
		return nil

	case LogUpdate:
		// Undo update: restore BeforeData
		return nil

	case LogDelete:
		// Undo delete: restore deleted tuple using BeforeData
		return nil

	default:
		return fmt.Errorf("unexpected log type: %v", record.Type)
	}
}

// GetRecoveryStats returns recovery statistics
func (rm *ParallelRecoveryManager) GetRecoveryStats() RecoveryStats {
	return rm.stats
}

// GetMetrics returns the recovery manager metrics
func (rm *ParallelRecoveryManager) GetMetrics() *Metrics {
	return rm.metrics
}

// GetNumWorkers returns the number of workers
func (rm *ParallelRecoveryManager) GetNumWorkers() int {
	return rm.numWorkers
}
