package storage

import (
	"fmt"
	"sync"
)

// Page represents a page in memory with metadata
type Page struct {
	pageId uint32
	pinCount int32
	isDirty bool
	data *SlottedPage
	mutex sync.RWMutex
}

// NewPage creates a new page with the given page ID
func NewPage(pageId uint32) *Page {
	return &Page{
		pageId: pageId,
		pinCount: 0,
		isDirty: false,
		data: NewSlottedPage(),
		mutex: sync.RWMutex{},
	}
}

// GetPageId returns the page ID
func (p *Page) GetPageId() uint32 {
	return p.pageId
}

// GetPinCount returns the pin count
func (p *Page) GetPinCount() int32 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.pinCount
}

// IsDirty returns whether the page is dirty
func (p *Page) IsDirty() bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.isDirty
}

// SetDirty sets the dirty flag
func (p *Page) SetDirty(dirty bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.isDirty = dirty
}

// Pin increments the pin count
func (p *Page) Pin() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.pinCount++
}

// Unpin decrements the pin count
func (p *Page) Unpin() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// GetData returns the slotted page data
func (p *Page) GetData() *SlottedPage {
	return p.data
}

// BufferPoolManager manages a pool of pages in memory
type BufferPoolManager struct {
	poolSize uint32
	pages []*Page
	pageTable *ShardedPageTable // Sharded page table for better concurrency
	freeList []uint32 // indices of free frames
	diskManager *DiskManager
	logManager *LogManager // WAL integration
	replacer Replacer // Page replacement policy (LRU, 2Q, etc.)
	metrics *Metrics // Performance metrics
	prefetcher *Prefetcher // Sequential prefetcher

	// Separate locks for better concurrency
	freeListMutex sync.Mutex // Protects freeList only
	pagesMutex sync.RWMutex // Protects pages array and page operations

	// Configuration
	enablePrefetch bool
}

// NewBufferPoolManager creates a new buffer pool manager
func NewBufferPoolManager(poolSize uint32, diskManager *DiskManager) (*BufferPoolManager, error) {
	return NewBufferPoolManagerWithReplacer(poolSize, diskManager, "2q")
}

// NewBufferPoolManagerWithReplacer creates a buffer pool with a specific replacement policy
func NewBufferPoolManagerWithReplacer(poolSize uint32, diskManager *DiskManager, replacerAlg string) (*BufferPoolManager, error) {
	if poolSize == 0 {
		return nil, fmt.Errorf("pool size must be greater than 0")
	}

	bpm := &BufferPoolManager{
		poolSize: poolSize,
		pages: make([]*Page, poolSize),
		pageTable: NewShardedPageTable(64), // 64 shards for good parallelism
		freeList: make([]uint32, 0, poolSize),
		diskManager: diskManager,
		logManager: nil, // Optional - can be set later
		replacer: NewReplacer(replacerAlg, poolSize), // Configurable replacer
		metrics: NewMetrics(), // Initialize metrics
		freeListMutex: sync.Mutex{},
		pagesMutex: sync.RWMutex{},
		enablePrefetch: true, // Enable prefetching by default
	}

	// Initialize prefetcher
	bpm.prefetcher = NewPrefetcher(bpm)

	// Initialize free list with all frame indices
	for i := uint32(0); i < poolSize; i++ {
		bpm.freeList = append(bpm.freeList, i)
	}

	return bpm, nil
}

// SetLogManager sets the log manager for WAL integration
func (bpm *BufferPoolManager) SetLogManager(logManager *LogManager) {
	bpm.pagesMutex.Lock()
	defer bpm.pagesMutex.Unlock()
	bpm.logManager = logManager
}

// GetPoolSize returns the pool size
func (bpm *BufferPoolManager) GetPoolSize() uint32 {
	return bpm.poolSize
}

// NewPage creates a new page, allocates it on disk, and brings it into the buffer pool
func (bpm *BufferPoolManager) NewPage() (*Page, error) {
	// Allocate a new page on disk
	pageId := bpm.diskManager.AllocatePage()

	// Find a free frame (acquires freeListMutex internally)
	frameId, err := bpm.getFrameId()
	if err != nil {
		return nil, fmt.Errorf("failed to get free frame: %w", err)
	}

	// Create new page
	page := NewPage(pageId)
	page.Pin() // Pin the page

	// Update pages array and page table
	bpm.pagesMutex.Lock()
	bpm.pages[frameId] = page
	bpm.pagesMutex.Unlock()

	bpm.pageTable.Put(pageId, page)

	// Remove from replacer since it's pinned
	bpm.replacer.Pin(frameId)

	return page, nil
}

// FetchPage fetches a page from disk if not in buffer pool, or returns existing page
func (bpm *BufferPoolManager) FetchPage(pageId uint32) (*Page, error) {
	// Record access for prefetching (use pageId as context for now)
	if bpm.enablePrefetch {
		go bpm.prefetcher.RecordAccess(uint64(pageId), pageId)
	}

	// Check if page is already in buffer pool
	if page, exists := bpm.pageTable.Get(pageId); exists {
		bpm.metrics.RecordCacheHit() // Track cache hit
		page.Pin()
		// Remove from replacer since it's now pinned
		bpm.pagesMutex.RLock()
		for frameId := uint32(0); frameId < bpm.poolSize; frameId++ {
			if bpm.pages[frameId] == page {
				bpm.replacer.Pin(frameId)
				break
			}
		}
		bpm.pagesMutex.RUnlock()
		return page, nil
	}

	// Cache miss - need to fetch from disk
	bpm.metrics.RecordCacheMiss()

	// Find a free frame (acquires freeListMutex internally)
	frameId, err := bpm.getFrameId()
	if err != nil {
		return nil, fmt.Errorf("failed to get free frame: %w", err)
	}

	// Read page from disk
	pageData, err := bpm.diskManager.ReadPage(pageId)
	if err != nil {
		return nil, fmt.Errorf("failed to read page from disk: %w", err)
	}

	// Deserialize page data into SlottedPage
	slottedPage, err := DeserializeSlottedPage(pageData)
	if err != nil {
		// If deserialization fails, create a new empty page
		// This handles the case of reading a newly allocated but uninitialized page
		slottedPage = NewSlottedPage()
	}

	// Create page and load data
	page := NewPage(pageId)
	page.data = slottedPage
	page.Pin()

	// Place page in buffer pool
	bpm.pagesMutex.Lock()
	bpm.pages[frameId] = page
	bpm.pagesMutex.Unlock()

	bpm.pageTable.Put(pageId, page)

	// Remove from replacer since it's pinned
	bpm.replacer.Pin(frameId)

	return page, nil
}

// UnpinPage unpins a page and optionally marks it as dirty
func (bpm *BufferPoolManager) UnpinPage(pageId uint32, isDirty bool) error {
	page, exists := bpm.pageTable.Get(pageId)
	if !exists {
		return fmt.Errorf("page %d not found in buffer pool", pageId)
	}

	page.Unpin()

	if isDirty {
		page.SetDirty(true)
	}

	// If page is unpinned, add to LRU replacer
	if page.GetPinCount() == 0 {
		// Find frame ID for this page
		bpm.pagesMutex.RLock()
		for frameId := uint32(0); frameId < bpm.poolSize; frameId++ {
			if bpm.pages[frameId] == page {
				bpm.replacer.Unpin(frameId)
				break
			}
		}
		bpm.pagesMutex.RUnlock()
	}

	return nil
}

// getFrameId returns a free frame ID, evicting a page if necessary
func (bpm *BufferPoolManager) getFrameId() (uint32, error) {
	// Try to get a free frame first
	bpm.freeListMutex.Lock()
	if len(bpm.freeList) > 0 {
		frameId := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		bpm.freeListMutex.Unlock()
		return frameId, nil
	}
	bpm.freeListMutex.Unlock()

	// No free frames, need to evict a page
	return bpm.evictPage()
}

// evictPage evicts an unpinned page using LRU policy
func (bpm *BufferPoolManager) evictPage() (uint32, error) {
	// Use LRU replacer to find victim
	frameId, ok := bpm.replacer.Victim()
	if !ok {
		return 0, ErrNoFreePages("evictPage")
	}

	bpm.pagesMutex.Lock()
	page := bpm.pages[frameId]
	if page != nil {
		// Flush dirty page before eviction
		if page.IsDirty() {
			bpm.metrics.RecordDirtyPageFlush() // Track dirty page flush
			err := bpm.flushPage(page)
			if err != nil {
				bpm.pagesMutex.Unlock()
				return 0, fmt.Errorf("failed to flush dirty page: %w", err)
			}
		}

		// Remove from page table
		bpm.pageTable.Delete(page.GetPageId())

		// Clear the frame
		bpm.pages[frameId] = nil
	}
	bpm.pagesMutex.Unlock()

	bpm.metrics.RecordPageEviction() // Track page eviction

	return frameId, nil
}

// evictPagesParallel evicts multiple pages concurrently for better scalability
// Returns the number of pages successfully evicted
func (bpm *BufferPoolManager) evictPagesParallel(count int) ([]uint32, error) {
	if count <= 0 {
		return nil, fmt.Errorf("evict count must be positive")
	}

	// Get victim frames from replacer
	victims := make([]uint32, 0, count)
	for i := 0; i < count; i++ {
		frameId, ok := bpm.replacer.Victim()
		if !ok {
			break // No more victims available
		}
		victims = append(victims, frameId)
	}

	if len(victims) == 0 {
		return nil, ErrNoFreePages("evictPagesParallel")
	}

	// Collect pages to evict (with lock)
	type evictTask struct {
		frameId uint32
		page *Page
	}

	tasks := make([]evictTask, 0, len(victims))
	bpm.pagesMutex.Lock()
	for _, frameId := range victims {
		if bpm.pages[frameId] != nil {
			tasks = append(tasks, evictTask{
				frameId: frameId,
				page: bpm.pages[frameId],
			})
		}
	}
	bpm.pagesMutex.Unlock()

	if len(tasks) == 0 {
		return nil, ErrNoFreePages("evictPagesParallel: no valid pages")
	}

	// Parallel flush of dirty pages
	var wg sync.WaitGroup
	errorsChan := make(chan error, len(tasks))

	for _, task := range tasks {
		if task.page.IsDirty() {
			wg.Add(1)
			go func(t evictTask) {
				defer wg.Done()

				bpm.metrics.RecordDirtyPageFlush()
				err := bpm.flushPage(t.page)
				if err != nil {
					errorsChan <- fmt.Errorf("failed to flush page %d: %w", t.page.GetPageId(), err)
				}
			}(task)
		}
	}

	// Wait for all flushes to complete
	wg.Wait()
	close(errorsChan)

	// Check for errors
	var flushErrors []error
	for err := range errorsChan {
		flushErrors = append(flushErrors, err)
	}

	if len(flushErrors) > 0 {
		// Return first error (could aggregate all errors if needed)
		return nil, flushErrors[0]
	}

	// Remove from page table and clear frames
	evictedFrames := make([]uint32, 0, len(tasks))
	bpm.pagesMutex.Lock()
	for _, task := range tasks {
		if bpm.pages[task.frameId] != nil {
			bpm.pageTable.Delete(task.page.GetPageId())
			bpm.pages[task.frameId] = nil
			evictedFrames = append(evictedFrames, task.frameId)
			bpm.metrics.RecordPageEviction()
		}
	}
	bpm.pagesMutex.Unlock()

	return evictedFrames, nil
}

// getFrameIdBatch attempts to get multiple free frames at once, evicting in parallel if needed
func (bpm *BufferPoolManager) getFrameIdBatch(count int) ([]uint32, error) {
	if count <= 0 {
		return nil, fmt.Errorf("batch count must be positive")
	}

	frames := make([]uint32, 0, count)

	// Try to get free frames first
	bpm.freeListMutex.Lock()
	availableFree := len(bpm.freeList)
	if availableFree > 0 {
		takeCount := availableFree
		if takeCount > count {
			takeCount = count
		}
		frames = append(frames, bpm.freeList[:takeCount]...)
		bpm.freeList = bpm.freeList[takeCount:]
	}
	bpm.freeListMutex.Unlock()

	// If we got enough frames, return
	if len(frames) >= count {
		return frames[:count], nil
	}

	// Need to evict pages to get more frames
	needed := count - len(frames)
	evicted, err := bpm.evictPagesParallel(needed)
	if err != nil {
		if len(frames) > 0 {
			// Partial success - return what we have
			return frames, nil
		}
		return nil, err
	}

	frames = append(frames, evicted...)
	return frames, nil
}

// flushPage writes a page back to disk
func (bpm *BufferPoolManager) flushPage(page *Page) error {
	// WRITE-AHEAD RULE: Flush log before writing dirty page
	if bpm.logManager != nil && page.IsDirty() {
		err := bpm.logManager.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush WAL before page write: %w", err)
		}
	}

	// Serialize the SlottedPage to bytes
	pageData := page.data.Serialize()

	// Write to disk
	err := bpm.diskManager.WritePage(page.GetPageId(), pageData)
	if err != nil {
		return err
	}

	page.SetDirty(false)
	return nil
}

// FlushPage explicitly flushes a page to disk
func (bpm *BufferPoolManager) FlushPage(pageId uint32) error {
	page, exists := bpm.pageTable.Get(pageId)

	if !exists {
		return fmt.Errorf("page %d not found in buffer pool", pageId)
	}

	return bpm.flushPage(page)
}

// FlushAllPages flushes all dirty pages to disk using batch writes
func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.pagesMutex.RLock()

	// Collect all dirty pages
	dirtyPages := make([]PageWrite, 0)
	for _, page := range bpm.pages {
		if page != nil && page.IsDirty() {
			// Serialize page data
			data := page.GetData().Serialize()

			dirtyPages = append(dirtyPages, PageWrite{
				PageID: page.GetPageId(),
				Data: data,
			})
		}
	}
	bpm.pagesMutex.RUnlock()

	// Batch write all dirty pages (single fsync)
	if len(dirtyPages) > 0 {
		err := bpm.diskManager.WritePagesV(dirtyPages)
		if err != nil {
			return fmt.Errorf("failed to batch write pages: %w", err)
		}

		// Mark all pages as clean
		bpm.pagesMutex.Lock()
		for _, pw := range dirtyPages {
			if page, exists := bpm.pageTable.Get(pw.PageID); exists {
				page.SetDirty(false)
			}
		}
		bpm.pagesMutex.Unlock()
	}

	return nil
}

// FlushAllPagesParallel flushes all dirty pages concurrently with configurable parallelism
func (bpm *BufferPoolManager) FlushAllPagesParallel(workers int) error {
	if workers <= 0 {
		workers = 4 // Default to 4 workers
	}

	bpm.pagesMutex.RLock()

	// Collect all dirty pages
	dirtyPages := make([]*Page, 0)
	for _, page := range bpm.pages {
		if page != nil && page.IsDirty() {
			dirtyPages = append(dirtyPages, page)
		}
	}
	bpm.pagesMutex.RUnlock()

	if len(dirtyPages) == 0 {
		return nil
	}

	// Flush WAL first (write-ahead rule)
	if bpm.logManager != nil {
		err := bpm.logManager.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
	}

	// Parallel flush with worker pool
	type flushJob struct {
		page *Page
		err error
	}

	jobs := make(chan *Page, len(dirtyPages))
	results := make(chan flushJob, len(dirtyPages))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for page := range jobs {
				// Serialize and write
				data := page.GetData().Serialize()
				err := bpm.diskManager.WritePage(page.GetPageId(), data)
				results <- flushJob{page: page, err: err}
			}
		}()
	}

	// Send jobs
	for _, page := range dirtyPages {
		jobs <- page
	}
	close(jobs)

	// Wait for workers
	wg.Wait()
	close(results)

	// Collect results and mark pages clean
	var flushErrors []error
	cleanPages := make([]*Page, 0, len(dirtyPages))

	for result := range results {
		if result.err != nil {
			flushErrors = append(flushErrors, result.err)
		} else {
			cleanPages = append(cleanPages, result.page)
		}
	}

	// Mark successfully flushed pages as clean
	if len(cleanPages) > 0 {
		for _, page := range cleanPages {
			page.SetDirty(false)
		}
	}

	if len(flushErrors) > 0 {
		return fmt.Errorf("failed to flush %d pages: %v", len(flushErrors), flushErrors[0])
	}

	return nil
}

// GetDirtyPageCount returns the number of dirty pages in the buffer pool
func (bpm *BufferPoolManager) GetDirtyPageCount() int {
	count := 0
	// Iterate through all pages in the pool
	for i := uint32(0); i < bpm.poolSize; i++ {
		page := bpm.pages[i]
		if page.IsDirty() {
			count++
		}
	}
	return count
}

// GetCapacity returns the total capacity of the buffer pool
func (bpm *BufferPoolManager) GetCapacity() int {
	return int(bpm.poolSize)
}

// GetDirtyPages returns up to maxPages dirty page IDs
func (bpm *BufferPoolManager) GetDirtyPages(maxPages int) []uint32 {
	dirtyPages := make([]uint32, 0, maxPages)
	// Iterate through all pages in the pool
	for i := uint32(0); i < bpm.poolSize; i++ {
		if len(dirtyPages) >= maxPages {
			break
		}
		page := bpm.pages[i]
		if page.IsDirty() {
			pageID := page.GetPageId()
			// Only include pages that are actually in use
			if pageID > 0 {
				dirtyPages = append(dirtyPages, pageID)
			}
		}
	}
	return dirtyPages
}

// GetMetrics returns the buffer pool metrics
func (bpm *BufferPoolManager) GetMetrics() *Metrics {
	return bpm.metrics
}
