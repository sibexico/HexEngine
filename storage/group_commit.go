package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// commitRequest represents a single transaction commit waiting to be grouped
type commitRequest struct {
	lsn      uint64
	response chan error
}

// GroupCommitManager batches multiple transaction commits into a single fsync,
// reducing I/O overhead. Instead of fsyncing on every commit, commits are grouped
// and persisted together.
type GroupCommitManager struct {
	logManager *LogManager

	// Configuration
	maxBatchSize  int           // Maximum commits per batch
	maxBatchDelay time.Duration // Maximum time to wait for more commits

	// Channels
	commitChan chan *commitRequest
	shutdownCh chan struct{}

	// Statistics
	totalCommits atomic.Uint64
	totalBatches atomic.Uint64
	totalFsyncs  atomic.Uint64

	wg sync.WaitGroup
}

// NewGroupCommitManager creates a new group commit manager
func NewGroupCommitManager(logManager *LogManager, maxBatchSize int, maxBatchDelay time.Duration) *GroupCommitManager {
	if maxBatchSize <= 0 {
		maxBatchSize = 100 // Default batch size
	}
	if maxBatchDelay <= 0 {
		maxBatchDelay = 10 * time.Millisecond // Default delay
	}

	gcm := &GroupCommitManager{
		logManager:    logManager,
		maxBatchSize:  maxBatchSize,
		maxBatchDelay: maxBatchDelay,
		commitChan:    make(chan *commitRequest, 1000), // Buffered for high throughput
		shutdownCh:    make(chan struct{}),
	}

	// Start background worker
	gcm.wg.Add(1)
	go gcm.worker()

	return gcm
}

// Commit submits a transaction commit request and waits for it to be persisted
func (gcm *GroupCommitManager) Commit(lsn uint64) error {
	// Check if already shut down first
	select {
	case <-gcm.shutdownCh:
		return NewStorageError(ErrCodeInternal, "Commit", "group commit manager shutdown", nil)
	default:
	}

	req := &commitRequest{
		lsn:      lsn,
		response: make(chan error, 1),
	}

	select {
	case gcm.commitChan <- req:
		gcm.totalCommits.Add(1)
		// Wait for response
		return <-req.response
	case <-gcm.shutdownCh:
		return NewStorageError(ErrCodeInternal, "Commit", "group commit manager shutdown", nil)
	}
}

// worker is the background goroutine that processes commit batches
func (gcm *GroupCommitManager) worker() {
	defer gcm.wg.Done()

	batch := make([]*commitRequest, 0, gcm.maxBatchSize)
	timer := time.NewTimer(gcm.maxBatchDelay)
	defer timer.Stop()

	for {
		select {
		case req := <-gcm.commitChan:
			batch = append(batch, req)

			// Flush if batch is full
			if len(batch) >= gcm.maxBatchSize {
				gcm.flushBatch(batch)
				batch = batch[:0] // Clear slice but keep capacity
				timer.Reset(gcm.maxBatchDelay)
			} else if len(batch) == 1 {
				// First request in batch, start timer
				timer.Reset(gcm.maxBatchDelay)
			}

		case <-timer.C:
			// Timeout - flush whatever we have
			if len(batch) > 0 {
				gcm.flushBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(gcm.maxBatchDelay)

		case <-gcm.shutdownCh:
			// Flush remaining batch before shutdown
			if len(batch) > 0 {
				gcm.flushBatch(batch)
			}

			// Drain any remaining requests in the channel and reject them
			for {
				select {
				case req := <-gcm.commitChan:
					req.response <- NewStorageError(ErrCodeInternal, "Commit", "group commit manager shutdown", nil)
					close(req.response)
				default:
					return
				}
			}
		}
	}
}

// flushBatch persists all commits in the batch with a single fsync
func (gcm *GroupCommitManager) flushBatch(batch []*commitRequest) {
	if len(batch) == 0 {
		return
	}

	gcm.totalBatches.Add(1)

	// Find the maximum LSN in the batch
	maxLSN := batch[0].lsn
	for _, req := range batch[1:] {
		if req.lsn > maxLSN {
			maxLSN = req.lsn
		}
	}

	// Flush the log up to maxLSN with a single fsync
	err := gcm.logManager.FlushToLSN(maxLSN)
	gcm.totalFsyncs.Add(1)

	// Notify all waiting transactions
	for _, req := range batch {
		req.response <- err
		close(req.response)
	}
}

// Shutdown gracefully shuts down the group commit manager
func (gcm *GroupCommitManager) Shutdown() {
	close(gcm.shutdownCh)
	gcm.wg.Wait()
}

// Stats returns statistics about group commit performance
func (gcm *GroupCommitManager) Stats() GroupCommitStats {
	totalCommits := gcm.totalCommits.Load()
	totalBatches := gcm.totalBatches.Load()
	totalFsyncs := gcm.totalFsyncs.Load()

	avgBatchSize := float64(0)
	if totalBatches > 0 {
		avgBatchSize = float64(totalCommits) / float64(totalBatches)
	}

	return GroupCommitStats{
		TotalCommits:     totalCommits,
		TotalBatches:     totalBatches,
		TotalFsyncs:      totalFsyncs,
		AverageBatchSize: avgBatchSize,
	}
}

// GroupCommitStats contains statistics about group commit performance
type GroupCommitStats struct {
	TotalCommits     uint64
	TotalBatches     uint64
	TotalFsyncs      uint64
	AverageBatchSize float64
}
