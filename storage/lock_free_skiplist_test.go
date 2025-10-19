package storage

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

// TestSkipListBasic tests basic operations
func TestSkipListBasic(t *testing.T) {
	config := DefaultSkipListConfig()
	sl := NewLockFreeSkipList(config)

	// Insert
	if !sl.Insert([]byte("key1"), []byte("value1")) {
		t.Error("Failed to insert key1")
	}

	// Search
	val, found := sl.Search([]byte("key1"))
	if !found {
		t.Error("Key1 not found")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Search non-existent
	_, found = sl.Search([]byte("key2"))
	if found {
		t.Error("Key2 should not be found")
	}

	// Length
	if sl.Length() != 1 {
		t.Errorf("Expected length 1, got %d", sl.Length())
	}
}

// TestSkipListInsertMultiple tests multiple insertions
func TestSkipListInsertMultiple(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for _, key := range keys {
		value := fmt.Sprintf("value_%s", key)
		if !sl.Insert([]byte(key), []byte(value)) {
			t.Errorf("Failed to insert %s", key)
		}
	}

	// Verify all keys
	for _, key := range keys {
		val, found := sl.Search([]byte(key))
		if !found {
			t.Errorf("Key %s not found", key)
		}
		expected := fmt.Sprintf("value_%s", key)
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", key, expected, val)
		}
	}

	if sl.Length() != int64(len(keys)) {
		t.Errorf("Expected length %d, got %d", len(keys), sl.Length())
	}
}

// TestSkipListDelete tests deletion
func TestSkipListDelete(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		sl.Insert([]byte(key), []byte("value_"+key))
	}

	// Delete middle key
	if !sl.Delete([]byte("c")) {
		t.Error("Failed to delete key c")
	}

	// Verify deletion
	_, found := sl.Search([]byte("c"))
	if found {
		t.Error("Key c should be deleted")
	}

	// Verify other keys still exist
	for _, key := range []string{"a", "b", "d", "e"} {
		_, found := sl.Search([]byte(key))
		if !found {
			t.Errorf("Key %s should still exist", key)
		}
	}

	if sl.Length() != 4 {
		t.Errorf("Expected length 4, got %d", sl.Length())
	}

	// Delete non-existent key
	if sl.Delete([]byte("z")) {
		t.Error("Should not delete non-existent key")
	}
}

// TestSkipListDuplicateInsert tests inserting duplicate keys
func TestSkipListDuplicateInsert(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	key := []byte("key")

	// First insert
	if !sl.Insert(key, []byte("value1")) {
		t.Error("First insert should succeed")
	}

	// Duplicate insert
	if sl.Insert(key, []byte("value2")) {
		t.Error("Duplicate insert should fail")
	}

	// Value should remain original
	val, found := sl.Search(key)
	if !found {
		t.Error("Key should exist")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

// TestSkipListRange tests range iteration
func TestSkipListRange(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert keys 0-9
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		sl.Insert(key, value)
	}

	// Range query [key03, key07]
	results := sl.RangeScan([]byte("key03"), []byte("key07"))

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Verify order and values
	expected := []string{"key03", "key04", "key05", "key06", "key07"}
	for i, kv := range results {
		if string(kv.Key) != expected[i] {
			t.Errorf("Result %d: expected %s, got %s", i, expected[i], kv.Key)
		}
	}
}

// TestSkipListConcurrentInsert tests concurrent insertions
func TestSkipListConcurrentInsert(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	numGoroutines := 10
	insertsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()

			for i := 0; i < insertsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("key_%d_%d", id, i))
				value := []byte(fmt.Sprintf("value_%d_%d", id, i))
				sl.Insert(key, value)
			}
		}(g)
	}

	wg.Wait()

	// Verify length
	expected := int64(numGoroutines * insertsPerGoroutine)
	if sl.Length() != expected {
		t.Errorf("Expected length %d, got %d", expected, sl.Length())
	}

	// Verify all keys exist
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < insertsPerGoroutine; i++ {
			key := []byte(fmt.Sprintf("key_%d_%d", g, i))
			_, found := sl.Search(key)
			if !found {
				t.Errorf("Key %s not found", key)
			}
		}
	}
}

// TestSkipListConcurrentDelete tests concurrent deletions
func TestSkipListConcurrentDelete(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert 1000 keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		sl.Insert(key, []byte("value"))
	}

	// Delete concurrently
	numGoroutines := 10
	keysPerGoroutine := numKeys / numGoroutines

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	deletedCount := int64(0)
	var mu sync.Mutex

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()

			start := id * keysPerGoroutine
			end := start + keysPerGoroutine

			for i := start; i < end; i++ {
				key := []byte(fmt.Sprintf("key%04d", i))
				if sl.Delete(key) {
					mu.Lock()
					deletedCount++
					mu.Unlock()
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify deletions
	if deletedCount != int64(numKeys) {
		t.Errorf("Expected %d deletions, got %d", numKeys, deletedCount)
	}

	if sl.Length() != 0 {
		t.Errorf("Expected length 0, got %d", sl.Length())
	}
}

// TestSkipListConcurrentMixed tests mixed concurrent operations
func TestSkipListConcurrentMixed(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	numGoroutines := 8
	opsPerGoroutine := 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(id)))

			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("key%04d", rng.Intn(1000)))
				value := []byte(fmt.Sprintf("value%d", id))

				op := rng.Intn(3)
				switch op {
				case 0: // Insert
					sl.Insert(key, value)
				case 1: // Delete
					sl.Delete(key)
				case 2: // Search
					sl.Search(key)
				}
			}
		}(g)
	}

	wg.Wait()

	// Just verify no crashes occurred
	t.Logf("Final length: %d", sl.Length())
}

// TestSkipListMinMax tests min/max key operations
func TestSkipListMinMax(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Empty list
	_, found := sl.MinKey()
	if found {
		t.Error("Empty list should have no min key")
	}

	// Insert keys
	keys := []string{"m", "a", "z", "d", "w"}
	for _, key := range keys {
		sl.Insert([]byte(key), []byte("value"))
	}

	// Check min
	minKey, found := sl.MinKey()
	if !found {
		t.Error("Should find min key")
	}
	if string(minKey) != "a" {
		t.Errorf("Expected min key 'a', got '%s'", minKey)
	}

	// Check max
	maxKey, found := sl.MaxKey()
	if !found {
		t.Error("Should find max key")
	}
	if string(maxKey) != "z" {
		t.Errorf("Expected max key 'z', got '%s'", maxKey)
	}
}

// TestSkipListStats tests statistics collection
func TestSkipListStats(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert 100 keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		sl.Insert(key, []byte("value"))
	}

	stats := sl.Stats()

	if stats.Length != 100 {
		t.Errorf("Expected length 100, got %d", stats.Length)
	}

	if stats.MaxLevel != 16 {
		t.Errorf("Expected max level 16, got %d", stats.MaxLevel)
	}

	// Level 0 should have all nodes
	if stats.LevelCounts[0] != 100 {
		t.Errorf("Level 0 should have 100 nodes, got %d", stats.LevelCounts[0])
	}

	// Higher levels should have fewer nodes
	for i := 1; i < int(stats.MaxLevel); i++ {
		if stats.LevelCounts[i] > stats.LevelCounts[i-1] {
			t.Errorf("Level %d has more nodes than level %d", i, i-1)
		}
	}

	t.Logf("Level distribution: %v", stats.LevelCounts[:8])
}

// TestSkipListOrdering tests that keys are maintained in sorted order
func TestSkipListOrdering(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert random keys
	keys := []string{"m", "a", "t", "h", "e", "w", "s"}
	for _, key := range keys {
		sl.Insert([]byte(key), []byte("value"))
	}

	// Get all keys via range scan
	results := sl.RangeScan([]byte("a"), []byte("z"))

	// Verify sorted order
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	if len(results) != len(sortedKeys) {
		t.Errorf("Expected %d results, got %d", len(sortedKeys), len(results))
	}

	for i, kv := range results {
		if string(kv.Key) != sortedKeys[i] {
			t.Errorf("Position %d: expected %s, got %s", i, sortedKeys[i], kv.Key)
		}
	}
}

// TestSkipListClear tests clearing the list
func TestSkipListClear(t *testing.T) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Insert keys
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		sl.Insert(key, []byte("value"))
	}

	if sl.Length() != 50 {
		t.Errorf("Expected length 50, got %d", sl.Length())
	}

	// Clear
	sl.Clear()

	if sl.Length() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", sl.Length())
	}

	// Verify keys are gone
	_, found := sl.Search([]byte("key00"))
	if found {
		t.Error("Keys should be cleared")
	}
}

// BenchmarkSkipListInsert benchmarks insert operations
func BenchmarkSkipListInsert(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%010d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Insert(keys[i], []byte("value"))
	}
}

// BenchmarkSkipListSearch benchmarks search operations
func BenchmarkSkipListSearch(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Pre-populate
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Insert(key, []byte("value"))
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%010d", rand.Intn(numKeys)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Search(keys[i])
	}
}

// BenchmarkSkipListDelete benchmarks delete operations
func BenchmarkSkipListDelete(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Pre-populate
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Insert(key, []byte("value"))
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%010d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Delete(keys[i])
	}
}

// BenchmarkSkipListConcurrentInsert benchmarks concurrent inserts
func BenchmarkSkipListConcurrentInsert(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key%010d", i))
			sl.Insert(key, []byte("value"))
			i++
		}
	})
}

// BenchmarkSkipListConcurrentSearch benchmarks concurrent searches
func BenchmarkSkipListConcurrentSearch(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Pre-populate
	numKeys := 100000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Insert(key, []byte("value"))
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			key := []byte(fmt.Sprintf("key%010d", rng.Intn(numKeys)))
			sl.Search(key)
		}
	})
}

// BenchmarkSkipListRangeScan benchmarks range scans
func BenchmarkSkipListRangeScan(b *testing.B) {
	sl := NewLockFreeSkipList(DefaultSkipListConfig())

	// Pre-populate
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key%010d", i))
		sl.Insert(key, []byte("value"))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		start := []byte(fmt.Sprintf("key%010d", rand.Intn(9900)))
		end := []byte(fmt.Sprintf("key%010d", rand.Intn(100)+9900))
		sl.RangeScan(start, end)
	}
}
