package storage

import (
	"sync"
)

// ShardedPageTable provides a thread-safe, sharded hash table for pages
// Reduces lock contention by partitioning the page table into multiple shards
type ShardedPageTable struct {
	shards []*PageTableShard
	numShards uint32
}

// PageTableShard represents a single shard with its own lock
type PageTableShard struct {
	mu sync.RWMutex
	pages map[uint32]*Page
}

// NewShardedPageTable creates a new sharded page table
// numShards should be a power of 2 for efficient modulo operations
// Recommended: 64-256 shards for good parallelism
func NewShardedPageTable(numShards uint32) *ShardedPageTable {
	if numShards == 0 {
		numShards = 64 // Default to 64 shards
	}

	shards := make([]*PageTableShard, numShards)
	for i := uint32(0); i < numShards; i++ {
		shards[i] = &PageTableShard{
			pages: make(map[uint32]*Page),
		}
	}

	return &ShardedPageTable{
		shards: shards,
		numShards: numShards,
	}
}

// getShard returns the shard for a given page ID
func (spt *ShardedPageTable) getShard(pageId uint32) *PageTableShard {
	// Fast modulo using bitwise AND if numShards is power of 2
	// Otherwise falls back to regular modulo
	return spt.shards[pageId%spt.numShards]
}

// Get retrieves a page from the table
func (spt *ShardedPageTable) Get(pageId uint32) (*Page, bool) {
	shard := spt.getShard(pageId)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	page, exists := shard.pages[pageId]
	return page, exists
}

// Put adds or updates a page in the table
func (spt *ShardedPageTable) Put(pageId uint32, page *Page) {
	shard := spt.getShard(pageId)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.pages[pageId] = page
}

// Delete removes a page from the table
func (spt *ShardedPageTable) Delete(pageId uint32) {
	shard := spt.getShard(pageId)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.pages, pageId)
}

// Size returns the total number of pages across all shards
func (spt *ShardedPageTable) Size() int {
	total := 0
	for _, shard := range spt.shards {
		shard.mu.RLock()
		total += len(shard.pages)
		shard.mu.RUnlock()
	}
	return total
}

// GetAll returns all pages (useful for iteration)
// This acquires all locks and should be used sparingly
func (spt *ShardedPageTable) GetAll() []*Page {
	// Pre-acquire all locks to ensure consistency
	for _, shard := range spt.shards {
		shard.mu.RLock()
	}
	defer func() {
		for _, shard := range spt.shards {
			shard.mu.RUnlock()
		}
	}()

	pages := make([]*Page, 0, spt.Size())
	for _, shard := range spt.shards {
		for _, page := range shard.pages {
			pages = append(pages, page)
		}
	}

	return pages
}

// Clear removes all pages from all shards
func (spt *ShardedPageTable) Clear() {
	for _, shard := range spt.shards {
		shard.mu.Lock()
		shard.pages = make(map[uint32]*Page)
		shard.mu.Unlock()
	}
}

// ForEach executes a function for each page in the table
// The function is called while holding the shard lock, so it should be fast
func (spt *ShardedPageTable) ForEach(fn func(pageId uint32, page *Page) bool) {
	for _, shard := range spt.shards {
		shard.mu.RLock()
		for pageId, page := range shard.pages {
			shouldContinue := fn(pageId, page)
			if !shouldContinue {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}
