package storage

import (
	"sync"
	"time"
)

// AccessPattern represents a detected access pattern with stride
type AccessPattern struct {
	StartPageID    uint32
	LastPageID     uint32
	Stride         int32   // Distance between consecutive accesses
	AccessCount    int     // Number of accesses matching this stride
	Confidence     float64 // Confidence score (0.0 - 1.0)
	LastAccessTime time.Time
	History        []uint32 // Recent page accesses for pattern analysis
	HistorySize    int      // Max history size
}

// Prefetcher detects sequential access patterns and prefetches pages
type Prefetcher struct {
	bpm *BufferPoolManager

	// Pattern tracking
	mu       sync.RWMutex
	patterns map[uint64]*AccessPattern // Key: transaction ID or thread ID

	// Configuration
	detectionThreshold int // Min consecutive accesses to trigger prefetching
	prefetchDistance   int // Number of pages to prefetch ahead
	prefetchEnabled    bool

	// Statistics
	stats PrefetchStats
}

// PrefetchStats tracks prefetching effectiveness
type PrefetchStats struct {
	PatternsDetected  uint64
	PagesPrefetched   uint64
	PrefetchHits      uint64 // Pages used before eviction
	PrefetchMisses    uint64 // Pages evicted before use
	PrefetchQueueFull uint64
	AvgConfidence     float64 // Average confidence of triggered prefetches
	StridesDetected   uint64  // Number of non-sequential strides detected
}

// NewPrefetcher creates a new prefetcher for the given buffer pool
func NewPrefetcher(bpm *BufferPoolManager) *Prefetcher {
	return &Prefetcher{
		bpm:                bpm,
		patterns:           make(map[uint64]*AccessPattern),
		detectionThreshold: 3, // Trigger after 3 consecutive accesses
		prefetchDistance:   8, // Prefetch 8 pages ahead
		prefetchEnabled:    true,
	}
}

// Configure sets prefetcher parameters
func (p *Prefetcher) Configure(detectionThreshold, prefetchDistance int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.detectionThreshold = detectionThreshold
	p.prefetchDistance = prefetchDistance
}

// Enable or disable prefetching
func (p *Prefetcher) SetEnabled(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.prefetchEnabled = enabled
}

// RecordAccess records a page access and detects patterns with stride and confidence
// contextID should be transaction ID or goroutine ID
func (p *Prefetcher) RecordAccess(contextID uint64, pageID uint32) {
	if !p.prefetchEnabled {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	pattern, exists := p.patterns[contextID]
	now := time.Now()

	if !exists {
		// First access for this context
		p.patterns[contextID] = &AccessPattern{
			StartPageID:    pageID,
			LastPageID:     pageID,
			Stride:         0,
			AccessCount:    1,
			Confidence:     0.0,
			LastAccessTime: now,
			History:        []uint32{pageID},
			HistorySize:    10, // Track last 10 accesses
		}
		return
	}

	// Timeout old patterns (more than 1 second idle)
	if now.Sub(pattern.LastAccessTime) > time.Second {
		// Reset pattern
		pattern.StartPageID = pageID
		pattern.LastPageID = pageID
		pattern.Stride = 0
		pattern.AccessCount = 1
		pattern.Confidence = 0.0
		pattern.LastAccessTime = now
		pattern.History = []uint32{pageID}
		return
	}

	// Calculate stride
	currentStride := int32(pageID) - int32(pattern.LastPageID)

	// Update history
	pattern.History = append(pattern.History, pageID)
	if len(pattern.History) > pattern.HistorySize {
		pattern.History = pattern.History[1:]
	}

	// Detect and update stride pattern
	if pattern.Stride == 0 {
		// First stride - establish pattern
		pattern.Stride = currentStride
		pattern.AccessCount = 2
		pattern.Confidence = 0.5 // Initial moderate confidence
	} else if pattern.Stride == currentStride {
		// Stride matches - increase confidence
		pattern.AccessCount++
		pattern.Confidence = p.calculateConfidence(pattern)
	} else {
		// Stride changed - analyze if we should update pattern or reset
		if p.shouldUpdateStride(pattern, currentStride) {
			// Update to new stride
			pattern.Stride = currentStride
			pattern.AccessCount = 2
			pattern.Confidence = 0.3 // Lower confidence for new pattern
		} else {
			// Reset pattern
			pattern.StartPageID = pageID
			pattern.Stride = 0
			pattern.AccessCount = 1
			pattern.Confidence = 0.0
			pattern.History = []uint32{pageID}
		}
	}

	pattern.LastPageID = pageID
	pattern.LastAccessTime = now

	// Trigger prefetching if confidence is high enough
	confidenceThreshold := 0.6 // Only prefetch when 60%+ confident
	if pattern.AccessCount >= p.detectionThreshold && pattern.Confidence >= confidenceThreshold {
		p.triggerPrefetch(pattern)
	}
}

// calculateConfidence computes confidence score based on pattern consistency
func (p *Prefetcher) calculateConfidence(pattern *AccessPattern) float64 {
	// Base confidence on access count (more accesses = higher confidence)
	baseConfidence := float64(pattern.AccessCount) / 10.0
	if baseConfidence > 1.0 {
		baseConfidence = 1.0
	}

	// Analyze history for consistency
	if len(pattern.History) < 3 {
		return baseConfidence
	}

	// Check how many strides match the expected pattern
	matchingStrides := 0
	totalStrides := 0

	for i := 1; i < len(pattern.History); i++ {
		stride := int32(pattern.History[i]) - int32(pattern.History[i-1])
		if stride == pattern.Stride {
			matchingStrides++
		}
		totalStrides++
	}

	// Consistency ratio
	consistencyRatio := float64(matchingStrides) / float64(totalStrides)

	// Weighted average: 70% consistency, 30% base confidence
	finalConfidence := 0.7*consistencyRatio + 0.3*baseConfidence

	return finalConfidence
}

// shouldUpdateStride determines if we should update to a new stride or reset
func (p *Prefetcher) shouldUpdateStride(pattern *AccessPattern, newStride int32) bool {
	// Don't update if we have high confidence in current pattern
	if pattern.Confidence > 0.8 {
		return false
	}

	// Check if new stride appears multiple times in recent history
	if len(pattern.History) < 3 {
		return true // Not enough data, try new pattern
	}

	// Count occurrences of new stride in recent history
	newStrideCount := 0
	for i := 1; i < len(pattern.History); i++ {
		stride := int32(pattern.History[i]) - int32(pattern.History[i-1])
		if stride == newStride {
			newStrideCount++
		}
	}

	// Update if new stride appears at least twice
	return newStrideCount >= 2
}

// triggerPrefetch initiates asynchronous prefetching based on detected stride
// Must be called with p.mu held
func (p *Prefetcher) triggerPrefetch(pattern *AccessPattern) {
	p.stats.PatternsDetected++

	// Track stride-based patterns
	if pattern.Stride != 1 && pattern.Stride != -1 {
		p.stats.StridesDetected++
	}

	// Update average confidence
	if p.stats.PatternsDetected == 1 {
		p.stats.AvgConfidence = pattern.Confidence
	} else {
		// Running average
		alpha := 0.1 // Exponential moving average factor
		p.stats.AvgConfidence = alpha*pattern.Confidence + (1-alpha)*p.stats.AvgConfidence
	}

	// Calculate next page based on stride
	nextPage := uint32(int32(pattern.LastPageID) + pattern.Stride)

	// Launch prefetch in background with confidence-based distance
	// Higher confidence = prefetch more pages
	prefetchCount := int(float64(p.prefetchDistance) * pattern.Confidence)
	if prefetchCount < 2 {
		prefetchCount = 2 // Always prefetch at least 2 pages
	}

	go p.doPrefetch(nextPage, pattern.Stride, prefetchCount)
}

// doPrefetch performs the actual prefetching with stride support
func (p *Prefetcher) doPrefetch(startPage uint32, stride int32, count int) {
	for i := 0; i < count; i++ {
		pageID := uint32(int32(startPage) + int32(i)*stride)

		// Don't prefetch if page is already in buffer pool
		if p.bpm.IsPageInPool(pageID) {
			continue
		}

		// Attempt to prefetch (non-blocking)
		page, err := p.bpm.FetchPage(pageID)
		if err != nil {
			// Stop prefetching on error
			break
		}

		// Unpin immediately - page is in cache for future use
		p.bpm.UnpinPage(pageID, false)

		// Update stats
		p.mu.Lock()
		p.stats.PagesPrefetched++
		p.mu.Unlock()

		_ = page // Mark as intentionally used
	}
}

// IsPageInPool checks if a page is already in the buffer pool to avoid redundant prefetches.
func (bpm *BufferPoolManager) IsPageInPool(pageID uint32) bool {
	_, exists := bpm.pageTable.Get(pageID)
	return exists
}

// ClearPattern removes pattern tracking for a context (e.g., after transaction commit)
func (p *Prefetcher) ClearPattern(contextID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.patterns, contextID)
}

// GetStats returns current prefetching statistics
func (p *Prefetcher) GetStats() PrefetchStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.stats
}

// ResetStats resets prefetching statistics
func (p *Prefetcher) ResetStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats = PrefetchStats{}
}

// Cleanup removes stale patterns (called periodically)
func (p *Prefetcher) Cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	for contextID, pattern := range p.patterns {
		if now.Sub(pattern.LastAccessTime) > 5*time.Second {
			delete(p.patterns, contextID)
		}
	}
}

// StartCleanupWorker starts a background goroutine to clean up stale patterns
func (p *Prefetcher) StartCleanupWorker(stopChan <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.Cleanup()
		case <-stopChan:
			return
		}
	}
}
