package storage

import (
	"testing"
)

// Helper function to create test setup
func createTestTreeForParallel(filename string, poolSize uint32) (*BPlusTree, *BufferPoolManager, error) {
	dm, err := NewDiskManager(filename)
	if err != nil {
		return nil, nil, err
	}

	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		dm.Close()
		return nil, nil, err
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		dm.Close()
		return nil, nil, err
	}

	return tree, bpm, nil
}

// TestParallelRangeScanBasic tests basic parallel range scan
func TestParallelRangeScanBasic(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	for i := int64(1); i <= 100; i++ {
		err = tree.Insert(i, i*10)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Perform parallel scan
	config := DefaultParallelScanConfig()
	config.NumWorkers = 2
	config.ChunkSize = 25

	resultChan, errorChan, err := tree.ParallelRangeScan(10, 50, config)
	if err != nil {
		t.Fatalf("ParallelRangeScan failed: %v", err)
	}

	// Collect results
	results := make(map[int64]int64)
	for result := range resultChan {
		results[result.Key] = result.Value
	}

	// Check for errors
	select {
	case err := <-errorChan:
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
	default:
	}

	// Verify results
	expectedCount := 41 // Keys 10-50 inclusive
	if len(results) != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, len(results))
	}

	// Verify each result
	for key := int64(10); key <= 50; key++ {
		expectedValue := key * 10
		if val, ok := results[key]; !ok {
			t.Errorf("Missing key %d", key)
		} else if val != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, val)
		}
	}
}

// TestParallelRangeScanOrdered tests ordered parallel scan
func TestParallelRangeScanOrdered(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_ordered.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	for i := int64(1); i <= 1000; i++ {
		err = tree.Insert(i, i*2)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Perform ordered parallel scan
	config := DefaultParallelScanConfig()
	config.NumWorkers = 4
	config.ChunkSize = 100

	entries, err := tree.ParallelRangeScanOrdered(100, 500, config)
	if err != nil {
		t.Fatalf("ParallelRangeScanOrdered failed: %v", err)
	}

	// Verify count
	expectedCount := 401 // Keys 100-500 inclusive
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify ordering and values
	for i, entry := range entries {
		expectedKey := int64(100 + i)
		expectedValue := expectedKey * 2

		if entry.Key != expectedKey {
			t.Errorf("Index %d: expected key %d, got %d", i, expectedKey, entry.Key)
		}
		if entry.Value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", entry.Key, expectedValue, entry.Value)
		}
	}
}

// TestParallelRangeScanCount tests count-only parallel scan
func TestParallelRangeScanCount(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_count.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	for i := int64(1); i <= 10000; i++ {
		err = tree.Insert(i, i)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Count entries in range
	config := DefaultParallelScanConfig()
	config.NumWorkers = 8
	config.ChunkSize = 500

	count, err := tree.ParallelRangeScanCount(1000, 5000, config)
	if err != nil {
		t.Fatalf("ParallelRangeScanCount failed: %v", err)
	}

	expectedCount := int64(4001) // 1000-5000 inclusive
	if count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, count)
	}
}

// TestParallelRangeScanEmpty tests scan on empty tree
func TestParallelRangeScanEmpty(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_empty.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	config := DefaultParallelScanConfig()
	resultChan, errorChan, err := tree.ParallelRangeScan(1, 100, config)
	if err != nil {
		t.Fatalf("ParallelRangeScan failed: %v", err)
	}

	// Should receive no results
	count := 0
	for range resultChan {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 results from empty tree, got %d", count)
	}

	// Check for errors
	select {
	case err := <-errorChan:
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	default:
	}
}

// TestParallelRangeScanFullRange tests scanning entire tree
func TestParallelRangeScanFullRange(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_full.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	numEntries := 5000
	for i := 1; i <= numEntries; i++ {
		err = tree.Insert(int64(i), int64(i*3))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Scan entire range
	config := DefaultParallelScanConfig()
	config.NumWorkers = 8
	config.ChunkSize = 200

	count, err := tree.ParallelRangeScanCount(1, int64(numEntries), config)
	if err != nil {
		t.Fatalf("ParallelRangeScanCount failed: %v", err)
	}

	if count != int64(numEntries) {
		t.Errorf("Expected count %d, got %d", numEntries, count)
	}
}

// TestParallelRangeScanStats tests scan statistics
func TestParallelRangeScanStats(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_stats.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	for i := int64(1); i <= 2000; i++ {
		err = tree.Insert(i, i)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Get scan statistics
	config := DefaultParallelScanConfig()
	config.NumWorkers = 4
	config.ChunkSize = 500

	stats, err := tree.GetParallelScanStats(100, 1500, config)
	if err != nil {
		t.Fatalf("GetParallelScanStats failed: %v", err)
	}

	// Verify statistics
	expectedEntries := int64(1401) // 100-1500 inclusive
	if stats.EntriesScanned != expectedEntries {
		t.Errorf("Expected %d entries scanned, got %d", expectedEntries, stats.EntriesScanned)
	}

	if stats.NumWorkers != config.NumWorkers {
		t.Errorf("Expected %d workers, got %d", config.NumWorkers, stats.NumWorkers)
	}

	if stats.NumChunks == 0 {
		t.Error("Expected at least 1 chunk")
	}

	t.Logf("Scan stats: %d chunks, %d workers, %d entries, chunk sizes: %v",
		stats.NumChunks, stats.NumWorkers, stats.EntriesScanned, stats.ChunkSizes)
}

// TestParallelRangeScanVariousWorkerCounts tests with different worker counts
func TestParallelRangeScanVariousWorkerCounts(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_workers.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	// Insert test data
	numEntries := 10000
	for i := 1; i <= numEntries; i++ {
		err = tree.Insert(int64(i), int64(i))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	workerCounts := []int{1, 2, 4, 8, 16}
	expectedCount := int64(5001) // Keys 1000-6000 inclusive

	for _, numWorkers := range workerCounts {
		config := DefaultParallelScanConfig()
		config.NumWorkers = numWorkers
		config.ChunkSize = 500

		count, err := tree.ParallelRangeScanCount(1000, 6000, config)
		if err != nil {
			t.Errorf("ParallelRangeScanCount with %d workers failed: %v", numWorkers, err)
			continue
		}

		if count != expectedCount {
			t.Errorf("Workers=%d: expected count %d, got %d", numWorkers, expectedCount, count)
		}
	}
}

// TestParallelRangeScanInvalidRange tests error handling for invalid ranges
func TestParallelRangeScanInvalidRange(t *testing.T) {
	tree, bpm, err := createTestTreeForParallel("test_parallel_scan_invalid.db", 10)
	if err != nil {
		t.Fatalf("Failed to create test tree: %v", err)
	}
	defer bpm.diskManager.Close()

	config := DefaultParallelScanConfig()

	// Test startKey > endKey
	_, _, err = tree.ParallelRangeScan(100, 50, config)
	if err == nil {
		t.Error("Expected error for startKey > endKey, got nil")
	}

	// Test ordered scan with invalid range
	_, err = tree.ParallelRangeScanOrdered(100, 50, config)
	if err == nil {
		t.Error("Expected error for startKey > endKey in ordered scan, got nil")
	}

	// Test count with invalid range
	_, err = tree.ParallelRangeScanCount(100, 50, config)
	if err == nil {
		t.Error("Expected error for startKey > endKey in count, got nil")
	}
}

// Benchmark parallel scan operations

func BenchmarkParallelRangeScan(b *testing.B) {
	tree, bpm, _ := createTestTreeForParallel("bench_parallel_scan.db", 100)
	defer bpm.diskManager.Close()

	// Insert 100K entries
	for i := int64(1); i <= 100000; i++ {
		tree.Insert(i, i)
	}

	config := DefaultParallelScanConfig()
	config.NumWorkers = 4
	config.ChunkSize = 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultChan, errorChan, _ := tree.ParallelRangeScan(10000, 50000, config)
		count := 0
		for range resultChan {
			count++
		}
		<-errorChan
	}
}

func BenchmarkParallelRangeScanCount(b *testing.B) {
	tree, bpm, _ := createTestTreeForParallel("bench_parallel_scan_count.db", 100)
	defer bpm.diskManager.Close()

	// Insert 100K entries
	for i := int64(1); i <= 100000; i++ {
		tree.Insert(i, i)
	}

	config := DefaultParallelScanConfig()
	config.NumWorkers = 4
	config.ChunkSize = 1000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.ParallelRangeScanCount(10000, 50000, config)
	}
}

func BenchmarkParallelRangeScanVsSequential(b *testing.B) {
	tree, bpm, _ := createTestTreeForParallel("bench_parallel_vs_seq.db", 100)
	defer bpm.diskManager.Close()

	// Insert 50K entries
	for i := int64(1); i <= 50000; i++ {
		tree.Insert(i, i)
	}

	b.Run("Sequential", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count := 0
			for key := int64(10000); key <= 30000; key++ {
				if _, ok, _ := tree.Search(key); ok {
					count++
				}
			}
		}
	})

	b.Run("Parallel-1Worker", func(b *testing.B) {
		config := DefaultParallelScanConfig()
		config.NumWorkers = 1
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.ParallelRangeScanCount(10000, 30000, config)
		}
	})

	b.Run("Parallel-2Workers", func(b *testing.B) {
		config := DefaultParallelScanConfig()
		config.NumWorkers = 2
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.ParallelRangeScanCount(10000, 30000, config)
		}
	})

	b.Run("Parallel-4Workers", func(b *testing.B) {
		config := DefaultParallelScanConfig()
		config.NumWorkers = 4
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.ParallelRangeScanCount(10000, 30000, config)
		}
	})

	b.Run("Parallel-8Workers", func(b *testing.B) {
		config := DefaultParallelScanConfig()
		config.NumWorkers = 8
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.ParallelRangeScanCount(10000, 30000, config)
		}
	})
}
