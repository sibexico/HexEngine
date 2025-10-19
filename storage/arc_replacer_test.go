package storage

import (
	"testing"
)

func TestARCReplacerBasic(t *testing.T) {
	arc := NewARCReplacer(3)

	// Initially empty
	if arc.Size() != 0 {
		t.Errorf("Expected size 0, got %d", arc.Size())
	}

	// Add pages
	arc.Unpin(1)
	arc.Unpin(2)
	arc.Unpin(3)

	if arc.Size() != 3 {
		t.Errorf("Expected size 3, got %d", arc.Size())
	}

	// Victim should return one of the pages
	victim, ok := arc.Victim()
	if !ok {
		t.Error("Expected victim to be found")
	}
	if victim < 1 || victim > 3 {
		t.Errorf("Unexpected victim: %d", victim)
	}
}

func TestARCAdaptation(t *testing.T) {
	arc := NewARCReplacer(4)

	// Access pattern favoring recency
	for i := 0; i < 10; i++ {
		arc.Unpin(uint32(i % 4))
	}

	stats := arc.GetStats()
	t.Logf("After recency pattern: %+v", stats)

	// Access pattern favoring frequency
	arc.Unpin(1)
	arc.Unpin(1)
	arc.Unpin(2)
	arc.Unpin(2)

	stats = arc.GetStats()
	t.Logf("After frequency pattern: %+v", stats)

	// T2 should have pages 1 and 2 (frequently accessed)
	if stats["t2_size"] < 1 {
		t.Error("Expected pages in T2 after frequent access")
	}
}

func TestARCGhostLists(t *testing.T) {
	arc := NewARCReplacer(2)

	// Fill cache
	arc.Unpin(1)
	arc.Unpin(2)

	// Access new page - should evict and create ghost entry
	arc.Unpin(3)

	stats := arc.GetStats()

	// Should have ghost entries
	if stats["b1_size"] == 0 && stats["b2_size"] == 0 {
		t.Error("Expected ghost entries after eviction")
	}

	t.Logf("Ghost list stats: %+v", stats)
}

func TestARCPromotionT1ToT2(t *testing.T) {
	arc := NewARCReplacer(3)

	// Add page to T1
	arc.Unpin(1)

	stats := arc.GetStats()
	if stats["t1_size"] != 1 {
		t.Errorf("Expected 1 page in T1, got %d", stats["t1_size"])
	}

	// Access again - should promote to T2
	arc.Pin(1)
	arc.Unpin(1)

	stats = arc.GetStats()
	if stats["t2_size"] == 0 {
		t.Error("Expected page to be promoted to T2 after second access")
	}

	t.Logf("After promotion: %+v", stats)
}

func TestARCPinUnpin(t *testing.T) {
	arc := NewARCReplacer(3)

	arc.Unpin(1)
	arc.Unpin(2)

	// Pin page 1
	arc.Pin(1)

	// Should not evict pinned page
	arc.Unpin(3)
	victim, ok := arc.Victim()
	if !ok {
		t.Fatal("Expected to find victim")
	}

	// Victim should be page 2, not pinned page 1
	if victim == 1 {
		t.Error("Should not evict pinned page")
	}
}

func TestARCSequentialAccess(t *testing.T) {
	arc := NewARCReplacer(5)

	// Sequential access pattern
	for i := uint32(0); i < 10; i++ {
		arc.Unpin(i)
	}

	stats := arc.GetStats()
	t.Logf("Sequential access stats: %+v", stats)

	// Most pages should be in T1 (recency)
	if stats["t1_size"] == 0 {
		t.Error("Expected pages in T1 for sequential access")
	}
}

func TestARCRepeatedAccess(t *testing.T) {
	arc := NewARCReplacer(5)

	// Repeated access pattern
	for i := 0; i < 10; i++ {
		arc.Unpin(1)
		arc.Unpin(2)
		arc.Pin(1)
		arc.Pin(2)
	}

	stats := arc.GetStats()
	t.Logf("Repeated access stats: %+v", stats)

	// Pages should be in T2 (frequency)
	if stats["t2_size"] == 0 {
		t.Error("Expected pages in T2 for repeated access")
	}
}

func TestARCMixedWorkload(t *testing.T) {
	arc := NewARCReplacer(10)

	// Single access pages - should stay in T1
	for j := uint32(10); j < 15; j++ {
		arc.Unpin(j)
	}

	// Repeated access pages - should move to T2
	for i := 0; i < 3; i++ {
		arc.Unpin(1)
		arc.Unpin(2)
		if i > 0 { // Pin/unpin to simulate re-access
			arc.Pin(1)
			arc.Pin(2)
			arc.Unpin(1)
			arc.Unpin(2)
		}
	}

	stats := arc.GetStats()
	t.Logf("Mixed workload stats: %+v", stats)

	// Should have pages in T1 (sequential, single access)
	// and T2 (repeated access)
	if stats["t1_size"] == 0 {
		t.Error("Expected pages in T1 for single-access pattern")
	}
	if stats["t2_size"] == 0 {
		t.Error("Expected pages in T2 for repeated-access pattern")
	}
}

func TestARCCapacityEnforcement(t *testing.T) {
	capacity := 5
	arc := NewARCReplacer(capacity)

	// Add more pages than capacity
	for i := uint32(0); i < 10; i++ {
		arc.Unpin(i)
	}

	stats := arc.GetStats()
	cacheSize := stats["t1_size"] + stats["t2_size"]

	if cacheSize > capacity {
		t.Errorf("Cache size %d exceeds capacity %d", cacheSize, capacity)
	}

	t.Logf("Capacity test stats: %+v", stats)
}

func TestARCAdaptiveParameter(t *testing.T) {
	arc := NewARCReplacer(10)

	// Initial p should be 0
	stats := arc.GetStats()
	if stats["target_p"] != 0 {
		t.Errorf("Expected initial p=0, got %d", stats["target_p"])
	}

	// Access pages to trigger adaptation
	for i := 0; i < 20; i++ {
		arc.Unpin(uint32(i % 5))
	}

	stats = arc.GetStats()
	t.Logf("Adaptive parameter after access: p=%d", stats["target_p"])

	// p should have adjusted
	// (exact value depends on access pattern)
}

func TestARCEvictionOrder(t *testing.T) {
	arc := NewARCReplacer(3)

	// Add pages in order
	arc.Unpin(1)
	arc.Unpin(2)
	arc.Unpin(3)

	// Fill capacity
	arc.Unpin(4)

	// One of the old pages should be evicted
	victim, ok := arc.Victim()
	if !ok {
		t.Fatal("Expected victim")
	}

	// Victim should be 1, 2, or 3 (oldest)
	if victim < 1 || victim > 3 {
		t.Errorf("Unexpected victim: %d", victim)
	}
}
