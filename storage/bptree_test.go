package storage

import (
	"os"
	"testing"
)

func TestBPlusTree(t *testing.T) {
	testFileName := "test_bptree.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	if tree == nil {
		t.Fatal("NewBPlusTree returned nil")
	}
}

func TestInsert(t *testing.T) {
	testFileName := "test_insert.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Insert some keys
	testKeys := []int64{10, 20, 30, 5, 15, 25, 35}
	for _, key := range testKeys {
		err := tree.Insert(key, key*100) // value = key * 100
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Search for inserted keys
	for _, key := range testKeys {
		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Key %d not found after insertion", key)
		}
		expectedValue := key * 100
		if value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, value)
		}
	}

	// Search for non-existent key
	_, found, err := tree.Search(999)
	if err != nil {
		t.Fatalf("Failed to search for non-existent key: %v", err)
	}
	if found {
		t.Error("Found non-existent key 999")
	}
}

func TestDelete(t *testing.T) {
	testFileName := "test_delete.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Insert keys
	testKeys := []int64{10, 20, 30, 40, 50}
	for _, key := range testKeys {
		err := tree.Insert(key, key*100)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Delete a key
	err = tree.Delete(30)
	if err != nil {
		t.Fatalf("Failed to delete key 30: %v", err)
	}

	// Verify it's deleted
	_, found, err := tree.Search(30)
	if err != nil {
		t.Fatalf("Failed to search after delete: %v", err)
	}
	if found {
		t.Error("Key 30 still found after deletion")
	}

	// Verify other keys still exist
	for _, key := range []int64{10, 20, 40, 50} {
		_, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Key %d not found after deleting different key", key)
		}
	}
}

func TestNodeSplit(t *testing.T) {
	testFileName := "test_node_split.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(20, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Insert enough keys to trigger splits
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := int64(i)
		err := tree.Insert(key, key*10)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Verify all keys are still accessible after splits
	for i := 0; i < numKeys; i++ {
		key := int64(i)
		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Failed to search for key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Key %d not found after node splits", key)
		}
		expectedValue := key * 10
		if value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, value)
		}
	}
}

func TestRangeScan(t *testing.T) {
	testFileName := "test_range_scan.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Insert keys in random order
	testKeys := []int64{50, 20, 80, 10, 30, 60, 90, 40, 70}
	for _, key := range testKeys {
		err := tree.Insert(key, key*100)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Create iterator for range scan
	iter, err := tree.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	// Collect all keys in order
	var keys []int64
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		keys = append(keys, key)
	}

	// Verify keys are in sorted order
	if len(keys) != len(testKeys) {
		t.Errorf("Expected %d keys, got %d", len(testKeys), len(keys))
	}

	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("Keys not in sorted order: %v", keys)
			break
		}
	}
}

func TestBulkLoad(t *testing.T) {
	testFileName := "test_bulkload.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Create sorted entries
	entries := make([]BPTreeEntry, 100)
	for i := 0; i < 100; i++ {
		entries[i] = BPTreeEntry{
			Key: int64(i * 10),
			Value: int64(i * 100),
		}
	}

	// Bulk load the tree
	err = tree.BulkLoad(entries)
	if err != nil {
		t.Fatalf("BulkLoad failed: %v", err)
	}

	// Verify all keys can be found
	for i := 0; i < 100; i++ {
		key := int64(i * 10)
		expectedValue := int64(i * 100)

		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Search error for key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Key %d not found after bulk load", key)
		}
		if value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, value)
		}
	}

	// Verify tree structure using iterator
	iter, err := tree.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	count := 0
	prevKey := int64(-1)
	for iter.HasNext() {
		key, value, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}

		// Verify sorted order
		if key <= prevKey {
			t.Errorf("Keys not in sorted order: %d after %d", key, prevKey)
		}
		prevKey = key

		// Verify value
		expectedValue := (key / 10) * 100
		if value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, value)
		}

		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}
}

func TestBulkLoadUnsorted(t *testing.T) {
	testFileName := "test_bulkload_unsorted.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Create unsorted entries
	entries := []BPTreeEntry{
		{Key: 50, Value: 500},
		{Key: 20, Value: 200},
		{Key: 80, Value: 800},
	}

	// Bulk load should fail with unsorted data
	err = tree.BulkLoad(entries)
	if err == nil {
		t.Error("Expected error for unsorted entries, got nil")
	}
}

func TestBulkLoadDuplicates(t *testing.T) {
	testFileName := "test_bulkload_duplicates.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Create entries with duplicates
	entries := []BPTreeEntry{
		{Key: 10, Value: 100},
		{Key: 20, Value: 200},
		{Key: 20, Value: 300}, // Duplicate
		{Key: 30, Value: 400},
	}

	// Bulk load should fail with duplicate keys
	err = tree.BulkLoad(entries)
	if err == nil {
		t.Error("Expected error for duplicate keys, got nil")
	}
}

func TestBulkLoadEmpty(t *testing.T) {
	testFileName := "test_bulkload_empty.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Bulk load with empty entries should succeed
	err = tree.BulkLoad([]BPTreeEntry{})
	if err != nil {
		t.Errorf("BulkLoad with empty entries failed: %v", err)
	}
}

func TestBulkLoadLarge(t *testing.T) {
	testFileName := "test_bulkload_large.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(100, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}

	// Create 10,000 sorted entries
	entries := make([]BPTreeEntry, 10000)
	for i := 0; i < 10000; i++ {
		entries[i] = BPTreeEntry{
			Key: int64(i),
			Value: int64(i * 7), // Arbitrary value
		}
	}

	// Bulk load
	err = tree.BulkLoad(entries)
	if err != nil {
		t.Fatalf("BulkLoad failed: %v", err)
	}

	// Spot check some keys
	testKeys := []int64{0, 100, 500, 1000, 5000, 9999}
	for _, key := range testKeys {
		expectedValue := key * 7
		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Search error for key %d: %v", key, err)
		}
		if !found {
			t.Errorf("Key %d not found after bulk load", key)
		}
		if value != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", key, expectedValue, value)
		}
	}

	// Verify total count
	iter, err := tree.Iterator()
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	count := 0
	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		count++
	}

	if count != 10000 {
		t.Errorf("Expected 10000 entries, got %d", count)
	}
}
