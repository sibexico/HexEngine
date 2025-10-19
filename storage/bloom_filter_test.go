package storage

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestBloomFilterBasic tests basic insert and lookup operations
func TestBloomFilterBasic(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements: 100,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	// Insert some keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range keys {
		bf.Insert(key)
	}

	// Check inserted keys are found
	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Errorf("Key %s should be found but wasn't", key)
		}
	}

	// Check non-inserted keys
	nonExistent := []byte("key999")
	// May return true (false positive) or false (true negative)
	// We can't assert the result, just verify it doesn't crash
	_ = bf.MayContain(nonExistent)
}

// TestBloomFilterFalsePositiveRate tests the false positive rate
func TestBloomFilterFalsePositiveRate(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements: 1000,
		FalsePositiveRate: 0.01, // 1% target
	}
	bf := NewBloomFilter(config)

	// Insert 1000 elements
	inserted := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		bf.Insert(key)
		inserted[string(key)] = true
	}

	// Test 10000 random non-inserted keys
	falsePositives := 0
	testCount := 10000

	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("notkey%d", i))
		if _, exists := inserted[string(key)]; !exists {
			if bf.MayContain(key) {
				falsePositives++
			}
		}
	}

	actualFPR := float64(falsePositives) / float64(testCount)
	t.Logf("False positive rate: %.4f%% (target: 1.00%%)", actualFPR*100)

	// Allow 3× the target rate (statistical variation)
	if actualFPR > 0.03 {
		t.Errorf("False positive rate too high: %.4f%% (max 3%%)", actualFPR*100)
	}
}

// TestBloomFilterNoFalseNegatives verifies no false negatives
func TestBloomFilterNoFalseNegatives(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Insert 100 keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("testkey%d", i))
		bf.Insert(key)
	}

	// Verify all inserted keys are found (no false negatives)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("testkey%d", i))
		if !bf.MayContain(key) {
			t.Errorf("False negative detected for key: %s", key)
		}
	}
}

// TestBloomFilterSerialization tests serialize/deserialize
func TestBloomFilterSerialization(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Insert some keys
	keys := []string{"apple", "banana", "cherry", "date"}
	for _, key := range keys {
		bf.Insert([]byte(key))
	}

	// Serialize
	data := bf.Serialize()
	t.Logf("Serialized size: %d bytes", len(data))

	// Deserialize
	bf2, err := DeserializeBloomFilter(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify all keys are still found
	for _, key := range keys {
		if !bf2.MayContain([]byte(key)) {
			t.Errorf("Key %s not found after deserialization", key)
		}
	}

	// Verify configuration matches
	if bf.GetNumBits() != bf2.GetNumBits() {
		t.Errorf("NumBits mismatch: %d vs %d", bf.GetNumBits(), bf2.GetNumBits())
	}
	if bf.GetNumHashes() != bf2.GetNumHashes() {
		t.Errorf("NumHashes mismatch: %d vs %d", bf.GetNumHashes(), bf2.GetNumHashes())
	}
}

// TestBloomFilterClear tests clearing the filter
func TestBloomFilterClear(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Insert keys
	for i := 0; i < 50; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	// Verify non-zero fill ratio
	fillBefore := bf.GetFillRatio()
	if fillBefore == 0 {
		t.Error("Fill ratio should be > 0 after inserts")
	}

	// Clear
	bf.Clear()

	// Verify empty
	fillAfter := bf.GetFillRatio()
	if fillAfter != 0 {
		t.Errorf("Fill ratio should be 0 after clear, got %.4f", fillAfter)
	}

	if bf.GetNumInserts() != 0 {
		t.Errorf("NumInserts should be 0 after clear, got %d", bf.GetNumInserts())
	}
}

// TestBloomFilterUnion tests merging two filters
func TestBloomFilterUnion(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf1 := NewBloomFilter(config)
	bf2 := NewBloomFilter(config)

	// Insert different keys into each filter
	for i := 0; i < 25; i++ {
		bf1.Insert([]byte(fmt.Sprintf("set1_%d", i)))
	}
	for i := 0; i < 25; i++ {
		bf2.Insert([]byte(fmt.Sprintf("set2_%d", i)))
	}

	// Union bf2 into bf1
	err := bf1.Union(bf2)
	if err != nil {
		t.Fatalf("Union failed: %v", err)
	}

	// Verify bf1 now contains keys from both sets
	for i := 0; i < 25; i++ {
		key1 := []byte(fmt.Sprintf("set1_%d", i))
		key2 := []byte(fmt.Sprintf("set2_%d", i))

		if !bf1.MayContain(key1) {
			t.Errorf("Union filter missing key from set1: %s", key1)
		}
		if !bf1.MayContain(key2) {
			t.Errorf("Union filter missing key from set2: %s", key2)
		}
	}
}

// TestBloomFilterIntersect tests intersection of two filters
func TestBloomFilterIntersect(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf1 := NewBloomFilter(config)
	bf2 := NewBloomFilter(config)

	// Insert overlapping keys
	for i := 0; i < 50; i++ {
		bf1.Insert([]byte(fmt.Sprintf("key%d", i)))
	}
	for i := 25; i < 75; i++ {
		bf2.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	// Intersect
	err := bf1.Intersect(bf2)
	if err != nil {
		t.Fatalf("Intersect failed: %v", err)
	}

	// Keys 25-49 should still be found (overlap)
	foundCount := 0
	for i := 25; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if bf1.MayContain(key) {
			foundCount++
		}
	}

	t.Logf("Intersection found %d/%d overlapping keys", foundCount, 25)
	// Due to false positives, we expect most but not necessarily all
	if foundCount < 20 {
		t.Errorf("Intersection found too few keys: %d < 20", foundCount)
	}
}

// TestBloomFilterClone tests cloning a filter
func TestBloomFilterClone(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Insert keys
	for i := 0; i < 30; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	// Clone
	bf2 := bf.Clone()

	// Verify clone contains same keys
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if !bf2.MayContain(key) {
			t.Errorf("Cloned filter missing key: %s", key)
		}
	}

	// Verify independent: modify original
	bf.Insert([]byte("newkey"))

	// Original should have it
	if !bf.MayContain([]byte("newkey")) {
		t.Error("Original should contain newkey")
	}

	// Clone should not (probably)
	// Can't assert this reliably due to false positives
}

// TestBloomFilterStats tests statistics calculation
func TestBloomFilterStats(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements: 100,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	// Initially empty
	if bf.GetNumInserts() != 0 {
		t.Errorf("Expected 0 inserts, got %d", bf.GetNumInserts())
	}

	if bf.GetFillRatio() != 0 {
		t.Errorf("Expected 0 fill ratio, got %.4f", bf.GetFillRatio())
	}

	// Insert 100 elements
	for i := 0; i < 100; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	// Check stats
	if bf.GetNumInserts() != 100 {
		t.Errorf("Expected 100 inserts, got %d", bf.GetNumInserts())
	}

	fillRatio := bf.GetFillRatio()
	t.Logf("Fill ratio after 100 inserts: %.2f%%", fillRatio*100)

	// Fill ratio should be significant but not 100%
	if fillRatio < 0.3 || fillRatio > 0.9 {
		t.Errorf("Fill ratio out of expected range: %.4f", fillRatio)
	}

	// Estimate false positive rate
	fpr := bf.EstimateFalsePositiveRate()
	t.Logf("Estimated FPR: %.4f%% (target: 1.00%%)", fpr*100)

	// Should be close to target (within 5×)
	if fpr > 0.05 {
		t.Errorf("Estimated FPR too high: %.4f%%", fpr*100)
	}
}

// TestPageBloomFilter tests page-level Bloom filter
func TestPageBloomFilter(t *testing.T) {
	config := DefaultBloomFilterConfig()
	pageID := uint32(123)
	pbf := NewPageBloomFilter(pageID, config)

	if pbf.GetPageID() != pageID {
		t.Errorf("Expected page ID %d, got %d", pageID, pbf.GetPageID())
	}

	// Insert keys
	keys := [][]byte{
		[]byte("row1"),
		[]byte("row2"),
		[]byte("row3"),
	}

	for _, key := range keys {
		pbf.InsertKey(key)
	}

	// Check keys exist
	for _, key := range keys {
		if !pbf.MayContainKey(key) {
			t.Errorf("Page filter should contain key: %s", key)
		}
	}

	// Get stats
	stats := pbf.GetStats()
	t.Logf("Page %d stats: inserts=%d, bits=%d, hashes=%d, fill=%.2f%%, fpr=%.4f%%",
		stats.PageID, stats.NumInserts, stats.NumBits, stats.NumHashes,
		stats.FillRatio*100, stats.EstimatedFPR*100)

	if stats.PageID != pageID {
		t.Errorf("Stats page ID mismatch: %d vs %d", stats.PageID, pageID)
	}

	if stats.NumInserts != 3 {
		t.Errorf("Expected 3 inserts, got %d", stats.NumInserts)
	}
}

// TestBloomFilterCapacity tests behavior at capacity
func TestBloomFilterCapacity(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedElements: 50,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)

	// Insert expected number
	for i := 0; i < 50; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	fpr50 := bf.EstimateFalsePositiveRate()
	t.Logf("FPR at capacity (50): %.4f%%", fpr50*100)

	// Insert 2× expected (overfill)
	for i := 50; i < 100; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	fpr100 := bf.EstimateFalsePositiveRate()
	t.Logf("FPR at 2× capacity (100): %.4f%%", fpr100*100)

	// FPR should increase when overfilled
	if fpr100 <= fpr50 {
		t.Errorf("FPR should increase when overfilled: %.4f%% vs %.4f%%", fpr100*100, fpr50*100)
	}

	// But all keys should still be found (no false negatives)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if !bf.MayContain(key) {
			t.Errorf("False negative at capacity for key: %s", key)
		}
	}
}

// TestBloomFilterEmptyKey tests handling of empty keys
func TestBloomFilterEmptyKey(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Insert empty key
	bf.Insert([]byte(""))

	// Should be found
	if !bf.MayContain([]byte("")) {
		t.Error("Empty key should be found after insertion")
	}

	// Should not affect other keys
	bf.Insert([]byte("nonempty"))
	if !bf.MayContain([]byte("nonempty")) {
		t.Error("Non-empty key should be found")
	}
}

// TestBloomFilterLargeKeys tests handling of large keys
func TestBloomFilterLargeKeys(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Create large keys (1KB each)
	largeKey1 := make([]byte, 1024)
	largeKey2 := make([]byte, 1024)

	for i := range largeKey1 {
		largeKey1[i] = byte(i % 256)
		largeKey2[i] = byte((i + 1) % 256)
	}

	// Insert
	bf.Insert(largeKey1)
	bf.Insert(largeKey2)

	// Verify
	if !bf.MayContain(largeKey1) {
		t.Error("Large key 1 should be found")
	}
	if !bf.MayContain(largeKey2) {
		t.Error("Large key 2 should be found")
	}
}

// BenchmarkBloomFilterInsert benchmarks insert operation
func BenchmarkBloomFilterInsert(b *testing.B) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bf.Insert(keys[i])
	}
}

// BenchmarkBloomFilterLookup benchmarks lookup operation
func BenchmarkBloomFilterLookup(b *testing.B) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Pre-populate
	for i := 0; i < 1000; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("lookup%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bf.MayContain(keys[i])
	}
}

// BenchmarkBloomFilterLookupHit benchmarks lookups that hit
func BenchmarkBloomFilterLookupHit(b *testing.B) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Pre-populate
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		bf.Insert(keys[i])
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bf.MayContain(keys[i%1000])
	}
}

// BenchmarkBloomFilterSerialize benchmarks serialization
func BenchmarkBloomFilterSerialize(b *testing.B) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	// Pre-populate
	for i := 0; i < 100; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bf.Serialize()
	}
}

// BenchmarkBloomFilterDeserialize benchmarks deserialization
func BenchmarkBloomFilterDeserialize(b *testing.B) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)

	for i := 0; i < 100; i++ {
		bf.Insert([]byte(fmt.Sprintf("key%d", i)))
	}

	data := bf.Serialize()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = DeserializeBloomFilter(data)
	}
}

// BenchmarkPageBloomFilter benchmarks page-level filter operations
func BenchmarkPageBloomFilter(b *testing.B) {
	config := DefaultBloomFilterConfig()
	pbf := NewPageBloomFilter(1, config)

	// Pre-populate
	for i := 0; i < 100; i++ {
		pbf.InsertKey([]byte(fmt.Sprintf("key%d", i)))
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("lookup%d", rand.Intn(200)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = pbf.MayContainKey(keys[i])
	}
}
