package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineStage represents a stage in the transaction commit pipeline
type PipelineStage int

const (
	StageValidation PipelineStage = iota // Check transaction can commit
	StageWrite                           // Write commit record to log buffer
	StageFsync                           // Flush log to disk
)

// commitBatch represents a batch of transactions in the pipeline
type commitBatch struct {
	txnIDs    []uint64      // Transaction IDs in batch
	lsns      []uint64      // LSNs for each transaction
	responses []chan error  // Response channels for each transaction
	startTime time.Time     // When batch entered pipeline
	maxLSN    uint64        // Highest LSN in batch
	stage     PipelineStage // Current pipeline stage
	err       error         // Error if any stage fails
	validated atomic.Bool   // Whether validation stage completed
	written   atomic.Bool   // Whether write stage completed
	fsynced   atomic.Bool   // Whether fsync stage completed
}

// PipelineCommitManager implements a pipelined commit protocol that overlaps
// validation, WAL writes, and fsync operations across consecutive batches.
// While one batch waits for fsync, the next batch is validated and written.
type PipelineCommitManager struct {
	logManager *LogManager
	txnManager *TransactionManager

	// Configuration
	maxBatchSize  int           // Max commits per batch
	maxBatchDelay time.Duration // Max time to wait for more commits

	// Pipeline channels
	incomingChan   chan *pipelineRequest // Incoming commit requests
	validationChan chan *commitBatch     // Batches ready for validation
	writeChan      chan *commitBatch     // Batches ready for write
	fsyncChan      chan *commitBatch     // Batches ready for fsync
	shutdownCh     chan struct{}

	// Statistics
	stats PipelineStats

	// Worker control
	wg sync.WaitGroup
}

// pipelineRequest represents a single commit request
type pipelineRequest struct {
	txnID    uint64
	lsn      uint64
	response chan error
}

// PipelineStats tracks pipeline performance metrics
type PipelineStats struct {
	TotalCommits        atomic.Uint64 // Total commits processed
	TotalBatches        atomic.Uint64 // Total batches processed
	TotalFsyncs         atomic.Uint64 // Total fsync operations
	ValidationTime      atomic.Int64  // Total time in validation (ns)
	WriteTime           atomic.Int64  // Total time in write (ns)
	FsyncTime           atomic.Int64  // Total time in fsync (ns)
	PipelineUtilization atomic.Int64  // Percentage of time with work in flight
}

// PipelineStatsSnapshot is a plain-value snapshot of pipeline statistics
type PipelineStatsSnapshot struct {
	TotalCommits        uint64 // Total commits processed
	TotalBatches        uint64 // Total batches processed
	TotalFsyncs         uint64 // Total fsync operations
	ValidationTime      int64  // Total time in validation (ns)
	WriteTime           int64  // Total time in write (ns)
	FsyncTime           int64  // Total time in fsync (ns)
	PipelineUtilization int64  // Percentage of time with work in flight
}

// NewPipelineCommitManager creates a new pipelined commit manager
func NewPipelineCommitManager(logManager *LogManager, txnManager *TransactionManager, maxBatchSize int, maxBatchDelay time.Duration) *PipelineCommitManager {
	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}
	if maxBatchDelay <= 0 {
		maxBatchDelay = 10 * time.Millisecond
	}

	pcm := &PipelineCommitManager{
		logManager:     logManager,
		txnManager:     txnManager,
		maxBatchSize:   maxBatchSize,
		maxBatchDelay:  maxBatchDelay,
		incomingChan:   make(chan *pipelineRequest, 1000),
		validationChan: make(chan *commitBatch, 2), // Buffer 2 batches for pipelining
		writeChan:      make(chan *commitBatch, 2),
		fsyncChan:      make(chan *commitBatch, 2),
		shutdownCh:     make(chan struct{}),
	}

	// Start pipeline workers
	pcm.wg.Add(4)
	go pcm.batchingWorker()   // Collect requests into batches
	go pcm.validationWorker() // Stage 1: Validate
	go pcm.writeWorker()      // Stage 2: Write to buffer
	go pcm.fsyncWorker()      // Stage 3: Fsync to disk

	return pcm
}

// Commit submits a transaction for pipelined commit
func (pcm *PipelineCommitManager) Commit(txnID uint64, lsn uint64) error {
	select {
	case <-pcm.shutdownCh:
		return NewStorageError(ErrCodeInternal, "Commit", "pipeline commit manager shutdown", nil)
	default:
	}

	req := &pipelineRequest{
		txnID:    txnID,
		lsn:      lsn,
		response: make(chan error, 1),
	}

	select {
	case pcm.incomingChan <- req:
		// Wait for response from pipeline
		return <-req.response
	case <-pcm.shutdownCh:
		return NewStorageError(ErrCodeInternal, "Commit", "pipeline commit manager shutdown", nil)
	}
}

// batchingWorker collects incoming requests into batches
func (pcm *PipelineCommitManager) batchingWorker() {
	defer pcm.wg.Done()

	batch := &commitBatch{
		txnIDs:    make([]uint64, 0, pcm.maxBatchSize),
		lsns:      make([]uint64, 0, pcm.maxBatchSize),
		responses: make([]chan error, 0, pcm.maxBatchSize),
		startTime: time.Now(),
		stage:     StageValidation,
	}

	timer := time.NewTimer(pcm.maxBatchDelay)
	defer timer.Stop()

	for {
		select {
		case req := <-pcm.incomingChan:
			// Add to current batch
			batch.txnIDs = append(batch.txnIDs, req.txnID)
			batch.lsns = append(batch.lsns, req.lsn)
			batch.responses = append(batch.responses, req.response)
			if req.lsn > batch.maxLSN {
				batch.maxLSN = req.lsn
			}

			pcm.stats.TotalCommits.Add(1)

			// Send batch if full
			if len(batch.txnIDs) >= pcm.maxBatchSize {
				pcm.validationChan <- batch
				pcm.stats.TotalBatches.Add(1)

				// Start new batch
				batch = &commitBatch{
					txnIDs:    make([]uint64, 0, pcm.maxBatchSize),
					lsns:      make([]uint64, 0, pcm.maxBatchSize),
					responses: make([]chan error, 0, pcm.maxBatchSize),
					startTime: time.Now(),
					stage:     StageValidation,
				}
				timer.Reset(pcm.maxBatchDelay)
			}

		case <-timer.C:
			// Timeout - send partial batch if not empty
			if len(batch.txnIDs) > 0 {
				pcm.validationChan <- batch
				pcm.stats.TotalBatches.Add(1)

				// Start new batch
				batch = &commitBatch{
					txnIDs:    make([]uint64, 0, pcm.maxBatchSize),
					lsns:      make([]uint64, 0, pcm.maxBatchSize),
					responses: make([]chan error, 0, pcm.maxBatchSize),
					startTime: time.Now(),
					stage:     StageValidation,
				}
			}
			timer.Reset(pcm.maxBatchDelay)

		case <-pcm.shutdownCh:
			// Drain remaining requests with error
			for {
				select {
				case req := <-pcm.incomingChan:
					req.response <- NewStorageError(ErrCodeInternal, "Commit", "pipeline shutting down", nil)
				default:
					return
				}
			}
		}
	}
}

// validationWorker validates transactions can commit
func (pcm *PipelineCommitManager) validationWorker() {
	defer pcm.wg.Done()

	for {
		select {
		case batch := <-pcm.validationChan:
			startTime := time.Now()

			// Validate all transactions in batch
			// In a full implementation, this would check:
			// - Serialization conflicts
			// - Constraint violations
			// - Write-write conflicts
			// For now, we assume validation always succeeds

			// Mark validation complete
			batch.validated.Store(true)
			batch.stage = StageWrite

			// Track timing
			duration := time.Since(startTime).Nanoseconds()
			pcm.stats.ValidationTime.Add(duration)

			// Send to write stage
			pcm.writeChan <- batch

		case <-pcm.shutdownCh:
			return
		}
	}
}

// writeWorker writes commit records to log buffer
func (pcm *PipelineCommitManager) writeWorker() {
	defer pcm.wg.Done()

	for {
		select {
		case batch := <-pcm.writeChan:
			startTime := time.Now()

			// Write commit records for all transactions
			// The actual write to buffer is fast (memcpy), no disk I/O yet
			for i, txnID := range batch.txnIDs {
				record := &LogRecord{
					TxnID: txnID,
					Type:  LogCommit,
				}
				lsn, err := pcm.logManager.AppendLog(record)
				if err != nil {
					batch.err = fmt.Errorf("failed to append commit log for txn %d: %w", txnID, err)
					pcm.respondToBatch(batch, batch.err)
					continue
				}
				batch.lsns[i] = lsn
				if lsn > batch.maxLSN {
					batch.maxLSN = lsn
				}
			}

			// Mark write complete
			batch.written.Store(true)
			batch.stage = StageFsync

			// Track timing
			duration := time.Since(startTime).Nanoseconds()
			pcm.stats.WriteTime.Add(duration)

			// Send to fsync stage
			pcm.fsyncChan <- batch

		case <-pcm.shutdownCh:
			return
		}
	}
}

// fsyncWorker flushes log to disk
func (pcm *PipelineCommitManager) fsyncWorker() {
	defer pcm.wg.Done()

	for {
		select {
		case batch := <-pcm.fsyncChan:
			startTime := time.Now()

			// Fsync can take 1-10ms; validation and write stages process next batch concurrently
			err := pcm.logManager.FlushToLSN(batch.maxLSN)
			if err != nil {
				batch.err = fmt.Errorf("failed to flush log: %w", err)
				pcm.respondToBatch(batch, batch.err)
				continue
			}

			// Mark fsync complete
			batch.fsynced.Store(true)
			pcm.stats.TotalFsyncs.Add(1)

			// Track timing
			duration := time.Since(startTime).Nanoseconds()
			pcm.stats.FsyncTime.Add(duration)

			// Respond to all transactions in batch
			pcm.respondToBatch(batch, nil)

		case <-pcm.shutdownCh:
			return
		}
	}
}

// respondToBatch sends response to all transactions in batch
func (pcm *PipelineCommitManager) respondToBatch(batch *commitBatch, err error) {
	for _, respChan := range batch.responses {
		respChan <- err
	}
}

// Shutdown stops the pipeline and waits for in-flight batches
func (pcm *PipelineCommitManager) Shutdown() {
	select {
	case <-pcm.shutdownCh:
		// Already shut down
		return
	default:
		close(pcm.shutdownCh)
		pcm.wg.Wait()
	}
}

// GetStats returns current pipeline statistics
// Returns a snapshot with atomic loads to avoid race conditions
func (pcm *PipelineCommitManager) GetStats() PipelineStatsSnapshot {
	return PipelineStatsSnapshot{
		TotalCommits:        pcm.stats.TotalCommits.Load(),
		TotalBatches:        pcm.stats.TotalBatches.Load(),
		TotalFsyncs:         pcm.stats.TotalFsyncs.Load(),
		ValidationTime:      pcm.stats.ValidationTime.Load(),
		WriteTime:           pcm.stats.WriteTime.Load(),
		FsyncTime:           pcm.stats.FsyncTime.Load(),
		PipelineUtilization: pcm.stats.PipelineUtilization.Load(),
	}
}

// GetAverageBatchSize returns average commits per batch
func (pcm *PipelineCommitManager) GetAverageBatchSize() float64 {
	batches := pcm.stats.TotalBatches.Load()
	if batches == 0 {
		return 0
	}
	commits := pcm.stats.TotalCommits.Load()
	return float64(commits) / float64(batches)
}

// GetAverageValidationTime returns average validation time per batch (nanoseconds)
func (pcm *PipelineCommitManager) GetAverageValidationTime() int64 {
	batches := pcm.stats.TotalBatches.Load()
	if batches == 0 {
		return 0
	}
	total := pcm.stats.ValidationTime.Load()
	return total / int64(batches)
}

// GetAverageWriteTime returns average write time per batch (nanoseconds)
func (pcm *PipelineCommitManager) GetAverageWriteTime() int64 {
	batches := pcm.stats.TotalBatches.Load()
	if batches == 0 {
		return 0
	}
	total := pcm.stats.WriteTime.Load()
	return total / int64(batches)
}

// GetAverageFsyncTime returns average fsync time per batch (nanoseconds)
func (pcm *PipelineCommitManager) GetAverageFsyncTime() int64 {
	fsyncs := pcm.stats.TotalFsyncs.Load()
	if fsyncs == 0 {
		return 0
	}
	total := pcm.stats.FsyncTime.Load()
	return total / int64(fsyncs)
}

// GetTotalPipelineTime returns total time spent in all stages (nanoseconds)
func (pcm *PipelineCommitManager) GetTotalPipelineTime() int64 {
	return pcm.stats.ValidationTime.Load() +
		pcm.stats.WriteTime.Load() +
		pcm.stats.FsyncTime.Load()
}

// GetPipelineEfficiency returns ratio of actual work time to elapsed time
// Higher is better - indicates pipeline is keeping stages busy
func (pcm *PipelineCommitManager) GetPipelineEfficiency() float64 {
	batches := pcm.stats.TotalBatches.Load()
	if batches == 0 {
		return 0
	}

	// Total work time
	workTime := float64(pcm.GetTotalPipelineTime())

	// Theoretical minimum time (if all stages ran sequentially)
	seqTime := workTime

	// In ideal pipeline with 3 stages, we should approach 3× speedup
	// Actual speedup = sequential_time / parallel_time
	// With perfect pipelining, fsync dominates, so parallel_time ≈ fsync_time
	fsyncTime := float64(pcm.stats.FsyncTime.Load())
	if fsyncTime == 0 {
		return 0
	}

	// Efficiency = actual_speedup / theoretical_max_speedup
	actualSpeedup := seqTime / fsyncTime
	theoreticalMaxSpeedup := 3.0 // 3 stages

	return actualSpeedup / theoreticalMaxSpeedup
}
