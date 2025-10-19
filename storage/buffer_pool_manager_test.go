package storage

import (
	"os"
	"testing"
)

func TestBufferPoolManager(t *testing.T) {
	testFileName := "test_buffer_pool.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	poolSize := uint32(3) // Small pool for testing
	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Test initial state
	if bpm.GetPoolSize() != poolSize {
		t.Errorf("Expected pool size %d, got %d", poolSize, bpm.GetPoolSize())
	}
}

func TestFetchNewPage(t *testing.T) {
	testFileName := "test_fetch_new.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	poolSize := uint32(3)
	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Fetch a new page
	page, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create new page: %v", err)
	}

	if page == nil {
		t.Fatal("NewPage returned nil page")
	}

	pageId := page.GetPageId()

	// Page should be pinned
	initialPinCount := page.GetPinCount()
	if initialPinCount <= 0 {
		t.Errorf("Expected page to be pinned, but pin count is %d", initialPinCount)
	}

	// Fetch the same page again
	samePage, err := bpm.FetchPage(pageId)
	if err != nil {
		t.Fatalf("Failed to fetch existing page: %v", err)
	}

	if samePage.GetPageId() != pageId {
		t.Errorf("Expected same page ID %d, got %d", pageId, samePage.GetPageId())
	}

	// Pin count should increase
	newPinCount := samePage.GetPinCount()
	if newPinCount != initialPinCount+1 {
		t.Errorf("Expected pin count to increase from %d to %d, got %d",
			initialPinCount, initialPinCount+1, newPinCount)
	}
}

func TestUnpinPage(t *testing.T) {
	testFileName := "test_unpin.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	poolSize := uint32(3)
	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Create a new page
	page, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create new page: %v", err)
	}

	pageId := page.GetPageId()
	initialPinCount := page.GetPinCount()

	// Unpin the page
	err = bpm.UnpinPage(pageId, false) // Not dirty
	if err != nil {
		t.Fatalf("Failed to unpin page: %v", err)
	}

	// Pin count should decrease
	if page.GetPinCount() != initialPinCount-1 {
		t.Errorf("Expected pin count to decrease from %d to %d, got %d",
			initialPinCount, initialPinCount-1, page.GetPinCount())
	}

	// Mark as dirty and unpin
	err = bpm.UnpinPage(pageId, true) // Mark dirty
	if err != nil {
		t.Fatalf("Failed to unpin page as dirty: %v", err)
	}

	// Page should be marked dirty when pin count reaches 0
	if page.GetPinCount() == 0 && !page.IsDirty() {
		t.Error("Expected page to be marked dirty when unpinned with dirty=true")
	}
}

func TestPageEviction(t *testing.T) {
	testFileName := "test_eviction.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	poolSize := uint32(2) // Very small pool to test eviction
	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Fill the buffer pool
	page1, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create page 1: %v", err)
	}

	page2, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create page 2: %v", err)
	}

	// Unpin both pages so they can be evicted
	err = bpm.UnpinPage(page1.GetPageId(), false)
	if err != nil {
		t.Fatalf("Failed to unpin page 1: %v", err)
	}

	err = bpm.UnpinPage(page2.GetPageId(), false)
	if err != nil {
		t.Fatalf("Failed to unpin page 2: %v", err)
	}

	// Create a third page - should trigger eviction
	page3, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create page 3 (should trigger eviction): %v", err)
	}

	if page3 == nil {
		t.Fatal("Expected page 3 to be created successfully despite full buffer pool")
	}

	// Buffer pool should still contain only 2 pages
	// (This test verifies that eviction worked)
}

// TestBufferPoolWithWAL tests WAL integration with buffer pool
func TestBufferPoolWithWAL(t *testing.T) {
	testFileName := "test_bpm_wal.db"
	testLogFile := "test_bpm_wal.log"
	defer os.Remove(testFileName)
	defer os.Remove(testLogFile)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	lm, err := NewLogManager(testLogFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	bpm, err := NewBufferPoolManager(3, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Set log manager
	bpm.SetLogManager(lm)

	// Create a new page and insert data
	page, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create new page: %v", err)
	}

	// Insert a tuple
	tuple := NewTuple([]byte("Test data with WAL"))
	_, err = page.data.InsertTuple(tuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Mark page as dirty
	page.SetDirty(true)

	// Log the change
	logRecord := &LogRecord{
		TxnID: 1,
		Type: LogInsert,
		PageID: page.GetPageId(),
		AfterData: []byte("Test data with WAL"),
	}
	_, err = lm.AppendLog(logRecord)
	if err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	// Unpin the page
	err = bpm.UnpinPage(page.GetPageId(), true)
	if err != nil {
		t.Fatalf("Failed to unpin page: %v", err)
	}

	// Flush the page explicitly
	err = bpm.FlushPage(page.GetPageId())
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	// Verify page is not dirty anymore
	if page.IsDirty() {
		t.Error("Page should not be dirty after flush")
	}

	// Fetch the page again
	fetchedPage, err := bpm.FetchPage(page.GetPageId())
	if err != nil {
		t.Fatalf("Failed to fetch page: %v", err)
	}

	// Verify data persisted
	readTuple, err := fetchedPage.data.ReadTuple(0)
	if err != nil {
		t.Fatalf("Failed to read tuple: %v", err)
	}

	if string(readTuple.GetData()) != "Test data with WAL" {
		t.Errorf("Data mismatch. Expected 'Test data with WAL', got '%s'", string(readTuple.GetData()))
	}
}

// TestPagePersistence tests that pages are properly persisted and can be read back
func TestPagePersistence(t *testing.T) {
	testFileName := "test_persistence.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}

	bpm, err := NewBufferPoolManager(5, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	// Create and populate multiple pages
	testData := []string{
		"First page data",
		"Second page data",
		"Third page data",
	}

	pageIDs := make([]uint32, 0)
	for _, data := range testData {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatalf("Failed to create page: %v", err)
		}

		tuple := NewTuple([]byte(data))
		_, err = page.data.InsertTuple(tuple)
		if err != nil {
			t.Fatalf("Failed to insert tuple: %v", err)
		}

		pageIDs = append(pageIDs, page.GetPageId())
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Flush all pages
	err = bpm.FlushAllPages()
	if err != nil {
		t.Fatalf("Failed to flush all pages: %v", err)
	}

	dm.Close()

	// Reopen the database
	dm2, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to reopen DiskManager: %v", err)
	}
	defer dm2.Close()

	bpm2, err := NewBufferPoolManager(5, dm2)
	if err != nil {
		t.Fatalf("Failed to create second BufferPoolManager: %v", err)
	}

	// Verify all pages can be read back with correct data
	for i, pageID := range pageIDs {
		page, err := bpm2.FetchPage(pageID)
		if err != nil {
			t.Fatalf("Failed to fetch page %d: %v", pageID, err)
		}

		tuple, err := page.data.ReadTuple(0)
		if err != nil {
			t.Fatalf("Failed to read tuple from page %d: %v", pageID, err)
		}

		if string(tuple.GetData()) != testData[i] {
			t.Errorf("Page %d data mismatch. Expected '%s', got '%s'",
				pageID, testData[i], string(tuple.GetData()))
		}
	}
}
