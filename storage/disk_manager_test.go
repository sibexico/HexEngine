package storage

import (
	"os"
	"testing"
)

func TestDiskManager(t *testing.T) {
	// Clean up test files
	testFileName := "test_disk_manager.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	// Test page allocation
	pageId1 := dm.AllocatePage()
	pageId2 := dm.AllocatePage()

	if pageId1 != 0 {
		t.Errorf("Expected first page ID to be 0, got %d", pageId1)
	}
	if pageId2 != 1 {
		t.Errorf("Expected second page ID to be 1, got %d", pageId2)
	}
}

func TestReadWritePage(t *testing.T) {
	testFileName := "test_read_write.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	// Test data for two different pages
	testData1 := make([]byte, PageSize)
	testData2 := make([]byte, PageSize)

	// Fill with different patterns
	for i := 0; i < PageSize; i++ {
		testData1[i] = byte(i % 256)
		testData2[i] = byte((i + 128) % 256)
	}

	// Allocate and write to pages
	pageId1 := dm.AllocatePage()
	pageId2 := dm.AllocatePage()

	err = dm.WritePage(pageId1, testData1)
	if err != nil {
		t.Fatalf("Failed to write page %d: %v", pageId1, err)
	}

	err = dm.WritePage(pageId2, testData2)
	if err != nil {
		t.Fatalf("Failed to write page %d: %v", pageId2, err)
	}

	// Read back and verify
	readData1, err := dm.ReadPage(pageId1)
	if err != nil {
		t.Fatalf("Failed to read page %d: %v", pageId1, err)
	}

	readData2, err := dm.ReadPage(pageId2)
	if err != nil {
		t.Fatalf("Failed to read page %d: %v", pageId2, err)
	}

	// Verify data integrity
	for i := 0; i < PageSize; i++ {
		if readData1[i] != testData1[i] {
			t.Errorf("Page 1 data mismatch at byte %d: expected %d, got %d", i, testData1[i], readData1[i])
			break
		}
		if readData2[i] != testData2[i] {
			t.Errorf("Page 2 data mismatch at byte %d: expected %d, got %d", i, testData2[i], readData2[i])
			break
		}
	}
}

func TestAllocatePage(t *testing.T) {
	testFileName := "test_allocate.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	// Test that page IDs are monotonically increasing
	var lastPageId uint32 = 0
	for i := 0; i < 10; i++ {
		pageId := dm.AllocatePage()
		if i == 0 {
			lastPageId = pageId
		} else {
			if pageId != lastPageId+1 {
				t.Errorf("Expected page ID to be %d, got %d", lastPageId+1, pageId)
			}
			lastPageId = pageId
		}
	}
}
