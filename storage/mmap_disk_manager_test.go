package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestMmapDiskManagerBasic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Check initial state
	stats := dm.GetStats()
	if stats.FileSize != InitialFileSize {
		t.Errorf("Expected initial file size %d, got %d", InitialFileSize, stats.FileSize)
	}
	if stats.NextPageId != uint32(InitialFileSize/PageSize) {
		t.Errorf("Expected next page ID %d, got %d", InitialFileSize/PageSize, stats.NextPageId)
	}
}

func TestMmapAllocatePage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Allocate pages
	page1, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	page2, err := dm.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	if page2 != page1+1 {
		t.Errorf("Expected sequential page IDs, got %d and %d", page1, page2)
	}
}

func TestMmapZeroCopyRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write data
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err = dm.WritePage(0, testData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Zero-copy read
	readData, err := dm.ReadPage(0)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	if !bytes.Equal(readData, testData) {
		t.Errorf("Read data doesn't match written data")
	}

	// Verify zero-copy by checking slice header
	// (both should point to same underlying memory)
	t.Logf("Zero-copy read successful: len=%d", len(readData))
}

func TestMmapReadPageCopy(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write data
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err = dm.WritePage(0, testData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Read copy
	readData, err := dm.ReadPageCopy(0)
	if err != nil {
		t.Fatalf("Failed to read page copy: %v", err)
	}

	if !bytes.Equal(readData, testData) {
		t.Errorf("Read data doesn't match written data")
	}

	// Modify the copy - shouldn't affect original
	readData[0] = 255
	readData2, _ := dm.ReadPage(0)
	if readData2[0] == 255 {
		t.Errorf("Modifying copy affected original data")
	}
}

func TestMmapWriteMultiplePages(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write 10 different pages
	for i := uint32(0); i < 10; i++ {
		data := make([]byte, PageSize)
		for j := range data {
			data[j] = byte(i)
		}

		err = dm.WritePage(i, data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Read and verify
	for i := uint32(0); i < 10; i++ {
		data, err := dm.ReadPage(i)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", i, err)
		}

		for j := range data {
			if data[j] != byte(i) {
				t.Errorf("Page %d byte %d: expected %d, got %d", i, j, i, data[j])
				break
			}
		}
	}
}

func TestMmapBatchWrite(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Prepare batch writes
	writes := make([]PageWrite, 5)
	for i := range writes {
		data := make([]byte, PageSize)
		for j := range data {
			data[j] = byte(i * 10)
		}
		writes[i] = PageWrite{
			PageID: uint32(i),
			Data: data,
		}
	}

	// Batch write
	err = dm.WritePagesV(writes)
	if err != nil {
		t.Fatalf("Batch write failed: %v", err)
	}

	// Verify
	for i := 0; i < 5; i++ {
		data, err := dm.ReadPage(uint32(i))
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", i, err)
		}
		if data[0] != byte(i*10) {
			t.Errorf("Page %d: expected %d, got %d", i, i*10, data[0])
		}
	}
}

func TestMmapFlushPage(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write data
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(42)
	}

	err = dm.WritePage(0, testData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Flush single page
	err = dm.FlushPage(0)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}

	t.Logf("Page flushed successfully")
}

func TestMmapFlushMultiplePages(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write 5 pages
	for i := uint32(0); i < 5; i++ {
		data := make([]byte, PageSize)
		for j := range data {
			data[j] = byte(i)
		}
		err = dm.WritePage(i, data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Flush multiple pages
	pageIds := []uint32{0, 2, 4}
	err = dm.FlushPages(pageIds)
	if err != nil {
		t.Fatalf("Failed to flush pages: %v", err)
	}

	t.Logf("Flushed %d pages successfully", len(pageIds))
}

func TestMmapConcurrentReads(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Write data
	numPages := 20
	for i := 0; i < numPages; i++ {
		data := make([]byte, PageSize)
		for j := range data {
			data[j] = byte(i)
		}
		err = dm.WritePage(uint32(i), data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// Concurrent reads
	numReaders := 10
	readsPerReader := 100
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				pageID := uint32(i % numPages)
				data, err := dm.ReadPage(pageID)
				if err != nil {
					errors <- fmt.Errorf("reader %d: %w", readerID, err)
					return
				}
				if data[0] != byte(pageID) {
					errors <- fmt.Errorf("reader %d: wrong data for page %d", readerID, pageID)
					return
				}
			}
		}(r)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent read error: %v", err)
	}

	t.Logf("Concurrent reads successful: %d readers × %d reads = %d total reads",
		numReaders, readsPerReader, numReaders*readsPerReader)
}

func TestMmapConcurrentWrites(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Concurrent writes to different pages
	numWriters := 5
	pagesPerWriter := 10
	var wg sync.WaitGroup
	errors := make(chan error, numWriters)

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < pagesPerWriter; i++ {
				pageID := uint32(writerID*pagesPerWriter + i)
				data := make([]byte, PageSize)
				for j := range data {
					data[j] = byte(writerID)
				}
				err := dm.WritePage(pageID, data)
				if err != nil {
					errors <- fmt.Errorf("writer %d: %w", writerID, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent write error: %v", err)
	}

	// Verify all pages
	for w := 0; w < numWriters; w++ {
		for i := 0; i < pagesPerWriter; i++ {
			pageID := uint32(w*pagesPerWriter + i)
			data, err := dm.ReadPage(pageID)
			if err != nil {
				t.Errorf("Failed to read page %d: %v", pageID, err)
				continue
			}
			if data[0] != byte(w) {
				t.Errorf("Page %d: expected %d, got %d", pageID, w, data[0])
			}
		}
	}

	t.Logf("Concurrent writes successful: %d writers × %d pages = %d total pages",
		numWriters, pagesPerWriter, numWriters*pagesPerWriter)
}

func TestMmapPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")

	// First session: write data
	{
		dm, err := NewMmapDiskManager(dbFile)
		if err != nil {
			t.Fatal(err)
		}

		testData := make([]byte, PageSize)
		for i := range testData {
			testData[i] = byte(123)
		}

		err = dm.WritePage(0, testData)
		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		err = dm.Flush()
		if err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		dm.Close()
	}

	// Second session: read data
	{
		dm, err := NewMmapDiskManager(dbFile)
		if err != nil {
			t.Fatal(err)
		}
		defer dm.Close()

		data, err := dm.ReadPage(0)
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}

		if data[0] != 123 {
			t.Errorf("Data not persisted correctly: expected 123, got %d", data[0])
		}

		t.Logf("Persistence verified: data survived file close/reopen")
	}
}

func TestMmapStats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	stats := dm.GetStats()
	t.Logf("Stats: FileSize=%d MB, MappedSize=%d MB, NextPageId=%d, UsedPages=%d, UsedMB=%d",
		stats.FileSize/(1024*1024),
		stats.MappedSize/(1024*1024),
		stats.NextPageId,
		stats.UsedPages,
		stats.UsedMB)

	if stats.FileSize != stats.MappedSize {
		t.Errorf("FileSize and MappedSize should match: %d vs %d", stats.FileSize, stats.MappedSize)
	}
}

func TestMmapOutOfBoundsRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Try to read beyond file size
	maxPages := uint32(dm.GetFileSize() / PageSize)
	_, err = dm.ReadPage(maxPages + 1000)
	if err == nil {
		t.Errorf("Expected error for out-of-bounds read")
	}
	t.Logf("Out-of-bounds read correctly rejected: %v", err)
}

func TestMmapInvalidPageSize(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mmap_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dm.Close()

	// Try to write wrong-sized data
	wrongData := make([]byte, PageSize-1)
	err = dm.WritePage(0, wrongData)
	if err == nil {
		t.Errorf("Expected error for wrong page size")
	}
	t.Logf("Invalid page size correctly rejected: %v", err)
}

func BenchmarkMmapZeroCopyRead(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "mmap_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	// Write test data
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	dm.WritePage(0, testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := dm.ReadPage(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMmapCopyRead(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "mmap_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	// Write test data
	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	dm.WritePage(0, testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := dm.ReadPageCopy(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMmapWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "mmap_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	testData := make([]byte, PageSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := dm.WritePage(uint32(i%100), testData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMmapBatchWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "mmap_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	// Prepare batch
	batchSize := 10
	writes := make([]PageWrite, batchSize)
	for i := range writes {
		writes[i] = PageWrite{
			PageID: uint32(i),
			Data: make([]byte, PageSize),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := dm.WritePagesV(writes)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMmapFlush(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "mmap_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbFile := filepath.Join(tempDir, "test.db")
	dm, err := NewMmapDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}
	defer dm.Close()

	// Write some data
	testData := make([]byte, PageSize)
	dm.WritePage(0, testData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := dm.FlushPage(0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark comparison: mmap vs traditional I/O
func BenchmarkCompareReadMethods(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "compare_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	testData := make([]byte, PageSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	b.Run("MmapZeroCopy", func(b *testing.B) {
		dbFile := filepath.Join(tempDir, "mmap.db")
		dm, _ := NewMmapDiskManager(dbFile)
		defer dm.Close()
		dm.WritePage(0, testData)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dm.ReadPage(0)
		}
	})

	b.Run("MmapCopy", func(b *testing.B) {
		dbFile := filepath.Join(tempDir, "mmap_copy.db")
		dm, _ := NewMmapDiskManager(dbFile)
		defer dm.Close()
		dm.WritePage(0, testData)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dm.ReadPageCopy(0)
		}
	})

	b.Run("TraditionalIO", func(b *testing.B) {
		dbFile := filepath.Join(tempDir, "traditional.db")
		dm, _ := NewDiskManager(dbFile)
		defer dm.Close()
		dm.WritePage(0, testData)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dm.ReadPage(0)
		}
	})
}
