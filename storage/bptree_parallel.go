package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// ParallelScanConfig contains configuration for parallel range scans
type ParallelScanConfig struct {
	NumWorkers        int  // Number of parallel workers
	ChunkSize         int  // Number of entries per chunk
	OrderedResults    bool // Whether to maintain order (slower)
	EnablePrefetching bool // Whether to prefetch next chunks
}

// DefaultParallelScanConfig returns a sensible default configuration
func DefaultParallelScanConfig() ParallelScanConfig {
	return ParallelScanConfig{
		NumWorkers:        4,
		ChunkSize:         1000,
		OrderedResults:    false,
		EnablePrefetching: true,
	}
}

// ScanResult represents a result from a parallel scan
type ScanResult struct {
	Key   int64
	Value int64
	Chunk int // Chunk number for ordering
	Index int // Index within chunk
}

// ParallelRangeScan performs a parallel range scan on the B+ Tree
// Returns results through a channel, allowing streaming of large result sets
func (tree *BPlusTree) ParallelRangeScan(startKey, endKey int64, config ParallelScanConfig) (<-chan ScanResult, <-chan error, error) {
	if startKey > endKey {
		return nil, nil, fmt.Errorf("startKey %d > endKey %d", startKey, endKey)
	}

	// Validate config
	if config.NumWorkers <= 0 {
		config.NumWorkers = 1
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = 1000
	}

	// Create result and error channels
	resultChan := make(chan ScanResult, config.NumWorkers*10)
	errorChan := make(chan error, 1)

	// Find the starting leaf node
	tree.rootMu.RLock()
	if tree.root == nil {
		tree.rootMu.RUnlock()
		close(resultChan)
		close(errorChan)
		return resultChan, errorChan, nil
	}
	currentRoot := tree.root
	tree.rootMu.RUnlock()

	startLeaf := tree.findLeafConcurrent(currentRoot, startKey)
	if startLeaf == nil {
		close(resultChan)
		close(errorChan)
		return resultChan, errorChan, nil
	}

	// Launch coordinator goroutine
	go tree.runParallelScan(startLeaf, startKey, endKey, config, resultChan, errorChan)

	return resultChan, errorChan, nil
}

// runParallelScan orchestrates the parallel scan
func (tree *BPlusTree) runParallelScan(startLeaf *BPTreeNode, startKey, endKey int64,
	config ParallelScanConfig, resultChan chan<- ScanResult, errorChan chan<- error) {

	defer close(resultChan)
	defer close(errorChan)

	// Collect leaf chunks for parallel processing
	chunks := tree.partitionLeaves(startLeaf, startKey, endKey, config.ChunkSize)
	if len(chunks) == 0 {
		return
	}

	// Create worker pool
	var wg sync.WaitGroup
	chunkChan := make(chan leafChunk, config.NumWorkers*2)

	// Launch workers
	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			tree.scanWorker(workerID, chunkChan, startKey, endKey, resultChan)
		}(i)
	}

	// Feed chunks to workers
	for _, chunk := range chunks {
		chunkChan <- chunk
	}
	close(chunkChan)

	// Wait for all workers to complete
	wg.Wait()
}

// leafChunk represents a chunk of leaf nodes to scan
type leafChunk struct {
	startNode  *BPTreeNode
	startIndex int
	endNode    *BPTreeNode
	endIndex   int
	chunkNum   int
}

// partitionLeaves divides the leaf node range into chunks for parallel processing
func (tree *BPlusTree) partitionLeaves(startLeaf *BPTreeNode, startKey, endKey int64, chunkSize int) []leafChunk {
	var chunks []leafChunk
	chunkNum := 0

	currentNode := startLeaf
	startIndex := tree.findKeyIndex(currentNode, startKey)
	entriesInChunk := 0
	chunkStart := currentNode
	chunkStartIndex := startIndex

	for currentNode != nil {
		// Determine end index for this node
		endIndex := len(currentNode.keys)
		for i := startIndex; i < len(currentNode.keys); i++ {
			if currentNode.keys[i] > endKey {
				endIndex = i
				break
			}
		}

		entriesInNode := endIndex - startIndex
		if entriesInChunk+entriesInNode >= chunkSize {
			// This chunk is full
			remainingInChunk := chunkSize - entriesInChunk
			finalIndex := startIndex + remainingInChunk

			chunks = append(chunks, leafChunk{
				startNode:  chunkStart,
				startIndex: chunkStartIndex,
				endNode:    currentNode,
				endIndex:   finalIndex,
				chunkNum:   chunkNum,
			})

			chunkNum++
			entriesInChunk = 0
			chunkStart = currentNode
			chunkStartIndex = finalIndex

			// If we didn't consume all entries in this node, continue with remainder
			if finalIndex < endIndex {
				entriesInChunk = endIndex - finalIndex
				startIndex = finalIndex
				continue
			}
		} else {
			entriesInChunk += entriesInNode
		}

		// Check if we've reached the end
		if endIndex < len(currentNode.keys) || (len(currentNode.keys) > 0 && currentNode.keys[len(currentNode.keys)-1] >= endKey) {
			// Final chunk
			if entriesInChunk > 0 {
				chunks = append(chunks, leafChunk{
					startNode:  chunkStart,
					startIndex: chunkStartIndex,
					endNode:    currentNode,
					endIndex:   endIndex,
					chunkNum:   chunkNum,
				})
			}
			break
		}

		// Move to next leaf
		currentNode = currentNode.next
		startIndex = 0
	}

	return chunks
}

// scanWorker processes chunks from the chunk channel
func (tree *BPlusTree) scanWorker(workerID int, chunkChan <-chan leafChunk,
	startKey, endKey int64, resultChan chan<- ScanResult) {

	for chunk := range chunkChan {
		tree.processChunk(chunk, startKey, endKey, resultChan)
	}
}

// processChunk scans a single chunk and sends results
func (tree *BPlusTree) processChunk(chunk leafChunk, startKey, endKey int64, resultChan chan<- ScanResult) {
	currentNode := chunk.startNode
	resultIndex := 0

	for currentNode != nil {
		// Acquire read lock on node
		currentNode.mu.RLock()

		// Determine scan range for this node
		startIdx := 0
		endIdx := len(currentNode.keys)

		if currentNode == chunk.startNode {
			startIdx = chunk.startIndex
		}
		if currentNode == chunk.endNode {
			endIdx = chunk.endIndex
		}

		// Scan entries in this node
		for i := startIdx; i < endIdx && i < len(currentNode.keys); i++ {
			key := currentNode.keys[i]
			if key >= startKey && key <= endKey {
				resultChan <- ScanResult{
					Key:   key,
					Value: currentNode.values[i],
					Chunk: chunk.chunkNum,
					Index: resultIndex,
				}
				resultIndex++
			}
		}

		nextNode := currentNode.next
		currentNode.mu.RUnlock()

		// Break if we've processed the end node
		if currentNode == chunk.endNode {
			break
		}

		currentNode = nextNode
	}
}

// findKeyIndex finds the index of the first key >= target in a leaf node
func (tree *BPlusTree) findKeyIndex(node *BPTreeNode, target int64) int {
	node.mu.RLock()
	defer node.mu.RUnlock()

	for i, key := range node.keys {
		if key >= target {
			return i
		}
	}
	return len(node.keys)
}

// ParallelRangeScanOrdered performs a parallel range scan with ordered results.
// Slower than unordered scan but maintains key order.
func (tree *BPlusTree) ParallelRangeScanOrdered(startKey, endKey int64, config ParallelScanConfig) ([]BPTreeEntry, error) {
	if startKey > endKey {
		return nil, fmt.Errorf("startKey %d > endKey %d", startKey, endKey)
	}

	config.OrderedResults = true
	resultChan, errorChan, err := tree.ParallelRangeScan(startKey, endKey, config)
	if err != nil {
		return nil, err
	}

	// Collect results into chunks
	chunkMap := make(map[int][]ScanResult)
	maxChunk := -1

	for result := range resultChan {
		chunkMap[result.Chunk] = append(chunkMap[result.Chunk], result)
		if result.Chunk > maxChunk {
			maxChunk = result.Chunk
		}
	}

	// Check for errors
	select {
	case err := <-errorChan:
		if err != nil {
			return nil, err
		}
	default:
	}

	// Sort chunks and combine into ordered result
	var entries []BPTreeEntry
	for chunkNum := 0; chunkNum <= maxChunk; chunkNum++ {
		chunk, ok := chunkMap[chunkNum]
		if !ok {
			continue
		}

		// Sort within chunk by index
		sortedChunk := make([]ScanResult, len(chunk))
		for _, result := range chunk {
			if result.Index < len(sortedChunk) {
				sortedChunk[result.Index] = result
			}
		}

		// Append to results
		for _, result := range sortedChunk {
			if result.Key != 0 { // Skip empty entries
				entries = append(entries, BPTreeEntry{
					Key:   result.Key,
					Value: result.Value,
				})
			}
		}
	}

	return entries, nil
}

// ParallelRangeScanCount counts entries in a range using parallel workers.
// Faster than scan-and-count since we don't return values.
func (tree *BPlusTree) ParallelRangeScanCount(startKey, endKey int64, config ParallelScanConfig) (int64, error) {
	if startKey > endKey {
		return 0, fmt.Errorf("startKey %d > endKey %d", startKey, endKey)
	}

	// Find the starting leaf node
	tree.rootMu.RLock()
	if tree.root == nil {
		tree.rootMu.RUnlock()
		return 0, nil
	}
	currentRoot := tree.root
	tree.rootMu.RUnlock()

	startLeaf := tree.findLeafConcurrent(currentRoot, startKey)
	if startLeaf == nil {
		return 0, nil
	}

	// Collect leaf chunks
	chunks := tree.partitionLeaves(startLeaf, startKey, endKey, config.ChunkSize)
	if len(chunks) == 0 {
		return 0, nil
	}

	// Parallel count
	var totalCount int64
	var wg sync.WaitGroup
	chunkChan := make(chan leafChunk, config.NumWorkers*2)

	// Launch workers
	for i := 0; i < config.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localCount := int64(0)
			for chunk := range chunkChan {
				localCount += tree.countChunk(chunk, startKey, endKey)
			}
			atomic.AddInt64(&totalCount, localCount)
		}()
	}

	// Feed chunks
	for _, chunk := range chunks {
		chunkChan <- chunk
	}
	close(chunkChan)

	wg.Wait()
	return totalCount, nil
}

// countChunk counts entries in a single chunk
func (tree *BPlusTree) countChunk(chunk leafChunk, startKey, endKey int64) int64 {
	count := int64(0)
	currentNode := chunk.startNode

	for currentNode != nil {
		currentNode.mu.RLock()

		startIdx := 0
		endIdx := len(currentNode.keys)

		if currentNode == chunk.startNode {
			startIdx = chunk.startIndex
		}
		if currentNode == chunk.endNode {
			endIdx = chunk.endIndex
		}

		for i := startIdx; i < endIdx && i < len(currentNode.keys); i++ {
			key := currentNode.keys[i]
			if key >= startKey && key <= endKey {
				count++
			}
		}

		nextNode := currentNode.next
		currentNode.mu.RUnlock()

		if currentNode == chunk.endNode {
			break
		}

		currentNode = nextNode
	}

	return count
}

// ParallelRangeScanStats contains statistics about a parallel scan
type ParallelRangeScanStats struct {
	NumChunks      int
	NumWorkers     int
	EntriesScanned int64
	ChunkSizes     []int
}

// GetParallelScanStats analyzes a range scan and returns statistics
func (tree *BPlusTree) GetParallelScanStats(startKey, endKey int64, config ParallelScanConfig) (ParallelRangeScanStats, error) {
	if startKey > endKey {
		return ParallelRangeScanStats{}, fmt.Errorf("startKey %d > endKey %d", startKey, endKey)
	}

	tree.rootMu.RLock()
	if tree.root == nil {
		tree.rootMu.RUnlock()
		return ParallelRangeScanStats{}, nil
	}
	currentRoot := tree.root
	tree.rootMu.RUnlock()

	startLeaf := tree.findLeafConcurrent(currentRoot, startKey)
	if startLeaf == nil {
		return ParallelRangeScanStats{}, nil
	}

	chunks := tree.partitionLeaves(startLeaf, startKey, endKey, config.ChunkSize)

	stats := ParallelRangeScanStats{
		NumChunks:  len(chunks),
		NumWorkers: config.NumWorkers,
		ChunkSizes: make([]int, len(chunks)),
	}

	for i, chunk := range chunks {
		chunkSize := tree.countChunk(chunk, startKey, endKey)
		stats.ChunkSizes[i] = int(chunkSize)
		stats.EntriesScanned += chunkSize
	}

	return stats, nil
}
