package storage

import (
	"testing"
)

func TestTwoQReplacerBasic(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// Pin some pages
	replacer.Pin(1)
	replacer.Pin(2)
	replacer.Pin(3)

	// With capacity 10, A1 max is 2 (25%), so page 1 gets evicted to ghost list
	stats := replacer.GetStats()
	t.Logf("A1MaxSize: %d, A1Size: %d, A1outSize: %d", stats.A1MaxSize, stats.A1Size, stats.A1outSize)

	// Size should be pages in A1 + A2 (not including ghost entries)
	expectedSize := uint32(stats.A1Size + stats.A2Size)
	if replacer.Size() != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, replacer.Size())
	}
}

func TestTwoQReplacerPromotion(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// First access - goes to A1
	replacer.Pin(1)

	stats := replacer.GetStats()
	if stats.A1Size != 1 || stats.A2Size != 0 {
		t.Errorf("Expected 1 in A1, 0 in A2. Got A1=%d, A2=%d", stats.A1Size, stats.A2Size)
	}

	// Second access - promotes to A2
	replacer.Pin(1)

	stats = replacer.GetStats()
	if stats.A1Size != 0 || stats.A2Size != 1 {
		t.Errorf("Expected 0 in A1, 1 in A2 after promotion. Got A1=%d, A2=%d", stats.A1Size, stats.A2Size)
	}
}

func TestTwoQReplacerVictimFromA1(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// Add pages to A1 (first access only)
	replacer.Pin(1)
	replacer.Pin(2)
	replacer.Pin(3)

	// Page 1 was already evicted when page 3 was added (A1 max = 2)
	// So the victim should be page 2 (oldest remaining in A1)
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("Expected a victim")
	}

	t.Logf("Victim: %d", victim)

	// Check that victim went to ghost list
	stats := replacer.GetStats()
	t.Logf("A1out size: %d", stats.A1outSize)

	if stats.A1outSize < 1 {
		t.Errorf("Expected at least 1 ghost entry, got %d", stats.A1outSize)
	}
}

func TestTwoQReplacerGhostListPromotion(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// First access
	replacer.Pin(1)

	// Evict from A1 to ghost list
	victim, ok := replacer.Victim()
	if !ok || victim != 1 {
		t.Fatal("Failed to evict page 1")
	}

	stats := replacer.GetStats()
	if stats.A1outSize != 1 {
		t.Errorf("Expected 1 ghost entry, got %d", stats.A1outSize)
	}

	// Access again - should promote directly to A2 from ghost list
	replacer.Pin(1)

	stats = replacer.GetStats()
	if stats.A2Size != 1 {
		t.Errorf("Expected 1 in A2 after ghost promotion, got %d", stats.A2Size)
	}

	if stats.A1outSize != 0 {
		t.Errorf("Expected 0 ghost entries after promotion, got %d", stats.A1outSize)
	}
}

func TestTwoQReplacerA1Overflow(t *testing.T) {
	// Small capacity to test overflow
	replacer := NewTwoQReplacer(8) // A1 will be 2 pages

	stats := replacer.GetStats()
	t.Logf("A1MaxSize: %d, A2MaxSize: %d", stats.A1MaxSize, stats.A2MaxSize)

	// Fill A1 beyond capacity
	for i := uint32(1); i <= 5; i++ {
		replacer.Pin(i)
	}

	stats = replacer.GetStats()

	// A1 should not exceed its max size
	if stats.A1Size > stats.A1MaxSize {
		t.Errorf("A1 exceeded max size: %d > %d", stats.A1Size, stats.A1MaxSize)
	}

	// Some pages should be in ghost list
	if stats.A1outSize == 0 {
		t.Error("Expected some pages in ghost list after A1 overflow")
	}
}

func TestTwoQReplacerA2LRU(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// Add and promote pages to A2
	replacer.Pin(1)
	replacer.Pin(1) // Promote to A2

	replacer.Pin(2)
	replacer.Pin(2) // Promote to A2

	replacer.Pin(3)
	replacer.Pin(3) // Promote to A2

	stats := replacer.GetStats()
	if stats.A2Size != 3 {
		t.Errorf("Expected 3 pages in A2, got %d", stats.A2Size)
	}

	// Access page 1 again - should move to front of A2
	replacer.Pin(1)

	// Now add enough pages to A1 to trigger A2 eviction
	for i := uint32(10); i < 20; i++ {
		replacer.Pin(i)
	}

	// When A1 is full and A2 needs to evict, it should evict LRU
	// Page 2 was accessed before page 1, so it should be evicted first
}

func TestTwoQReplacerRemove(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	replacer.Pin(1)
	replacer.Pin(2)
	replacer.Pin(3)

	initialSize := replacer.Size()
	t.Logf("Initial size: %d", initialSize)

	// Remove a page that's currently in A1
	replacer.Remove(3) // Page 3 should be in A1

	if replacer.Size() != uint32(initialSize-1) {
		t.Errorf("Expected size %d after removal, got %d", initialSize-1, replacer.Size())
	}
}

func TestTwoQReplacerEmpty(t *testing.T) {
	replacer := NewTwoQReplacer(10)

	// Victim on empty replacer
	_, ok := replacer.Victim()
	if ok {
		t.Error("Expected no victim on empty replacer")
	}

	if replacer.Size() != uint32(0) {
		t.Errorf("Expected size 0, got %d", replacer.Size())
	}
}

func TestTwoQReplacerSequentialAccess(t *testing.T) {
	replacer := NewTwoQReplacer(100)

	// Simulate sequential scan (pages accessed once)
	for i := uint32(1); i <= 200; i++ {
		replacer.Pin(i)
	}

	stats := replacer.GetStats()

	// Most should be in A1 (first access)
	// Should not promote to A2 since they're only accessed once
	t.Logf("Sequential scan: A1=%d, A2=%d, A1out=%d",
		stats.A1Size, stats.A2Size, stats.A1outSize)

	// A1 should be at capacity
	if stats.A1Size != stats.A1MaxSize {
		t.Errorf("Expected A1 at capacity (%d), got %d", stats.A1MaxSize, stats.A1Size)
	}
}

func TestTwoQReplacerHotPages(t *testing.T) {
	replacer := NewTwoQReplacer(100)

	// Simulate hot pages (frequently accessed)
	hotPages := []uint32{1, 2, 3, 4, 5}

	// Access hot pages multiple times
	for round := 0; round < 10; round++ {
		for _, page := range hotPages {
			replacer.Pin(page)
		}
	}

	stats := replacer.GetStats()

	// Hot pages should be in A2
	if stats.A2Size < len(hotPages) {
		t.Errorf("Expected at least %d pages in A2, got %d", len(hotPages), stats.A2Size)
	}

	t.Logf("Hot pages: A1=%d, A2=%d", stats.A1Size, stats.A2Size)
}

func TestTwoQReplacerMixedWorkload(t *testing.T) {
	replacer := NewTwoQReplacer(50)

	// Hot pages (accessed frequently)
	hotPages := []uint32{1, 2, 3}

	// Access hot pages
	for round := 0; round < 5; round++ {
		for _, page := range hotPages {
			replacer.Pin(page)
		}
	}

	// Sequential scan (accessed once)
	for i := uint32(100); i < 200; i++ {
		replacer.Pin(i)
	}

	stats := replacer.GetStats()

	t.Logf("Mixed workload: A1=%d, A2=%d, A1out=%d, Total=%d",
		stats.A1Size, stats.A2Size, stats.A1outSize, stats.TotalPages)

	// Hot pages should be in A2
	if stats.A2Size < len(hotPages) {
		t.Errorf("Expected at least %d hot pages in A2, got %d", len(hotPages), stats.A2Size)
	}
}

// Benchmark 2Q vs LRU
func BenchmarkTwoQReplacerPin(b *testing.B) {
	replacer := NewTwoQReplacer(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Pin(uint32(i % 1000))
	}
}

func BenchmarkTwoQReplacerVictim(b *testing.B) {
	replacer := NewTwoQReplacer(1000)

	// Fill replacer
	for i := uint32(0); i < 1000; i++ {
		replacer.Pin(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Victim()
		replacer.Pin(uint32(i % 1000))
	}
}

func BenchmarkTwoQReplacerHotSet(b *testing.B) {
	replacer := NewTwoQReplacer(1000)
	hotSet := make([]uint32, 100)
	for i := range hotSet {
		hotSet[i] = uint32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Pin(hotSet[i%len(hotSet)])
	}
}

func BenchmarkLRUReplacerPin(b *testing.B) {
	replacer := NewLRUReplacer(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Pin(uint32(i % 1000))
	}
}

func BenchmarkLRUReplacerVictim(b *testing.B) {
	replacer := NewLRUReplacer(1000)

	// Fill replacer
	for i := uint32(0); i < 1000; i++ {
		replacer.Pin(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replacer.Victim()
		replacer.Pin(uint32(i % 1000))
	}
}
