package storage

import (
	"testing"
)

// TestLRUReplacer tests basic LRU replacer functionality
func TestLRUReplacer(t *testing.T) {
	replacer := NewLRUReplacer(5)

	if replacer == nil {
		t.Fatal("LRU replacer should not be nil")
	}

	if replacer.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", replacer.Size())
	}
}

// TestLRUVictim tests victim selection
func TestLRUVictim(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Add frames in order: 0, 1, 2
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	// Oldest should be 0
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 0 {
		t.Errorf("Expected victim 0, got %d", victim)
	}

	// After evicting 0, next should be 1
	victim, ok = replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 1 {
		t.Errorf("Expected victim 1, got %d", victim)
	}
}

// TestLRUPin tests pinning frames
func TestLRUPin(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Add frames
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	if replacer.Size() != 3 {
		t.Errorf("Expected size 3, got %d", replacer.Size())
	}

	// Pin frame 1
	replacer.Pin(1)

	if replacer.Size() != 2 {
		t.Errorf("Expected size 2 after pin, got %d", replacer.Size())
	}

	// Victim should be 0 (oldest)
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 0 {
		t.Errorf("Expected victim 0, got %d", victim)
	}

	// Next victim should be 2 (frame 1 is pinned)
	victim, ok = replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 2 {
		t.Errorf("Expected victim 2, got %d", victim)
	}
}

// TestLRUAccess tests access updating recency
func TestLRUAccess(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Add frames in order: 0, 1, 2
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	// Access frame 0 (makes it most recently used)
	replacer.Unpin(0)

	// Now order should be: 1 (oldest), 2, 0 (newest)
	// Victim should be 1
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 1 {
		t.Errorf("Expected victim 1 (oldest), got %d", victim)
	}
}

// TestLRUEmpty tests empty replacer
func TestLRUEmpty(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// No frames added
	victim, ok := replacer.Victim()
	if ok {
		t.Errorf("Should not have a victim when empty, got %d", victim)
	}

	if replacer.Size() != 0 {
		t.Errorf("Expected size 0, got %d", replacer.Size())
	}
}

// TestLRUCapacity tests replacer at full capacity
func TestLRUCapacity(t *testing.T) {
	capacity := uint32(3)
	replacer := NewLRUReplacer(capacity)

	// Add frames up to capacity
	replacer.Unpin(0)
	replacer.Unpin(1)
	replacer.Unpin(2)

	if replacer.Size() != 3 {
		t.Errorf("Expected size 3, got %d", replacer.Size())
	}

	// Can still add more (capacity is for buffer pool, not replacer)
	replacer.Unpin(3)

	if replacer.Size() != 4 {
		t.Errorf("Expected size 4, got %d", replacer.Size())
	}
}

// TestLRUPinUnpin tests pin/unpin sequence
func TestLRUPinUnpin(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Unpin frames
	replacer.Unpin(0)
	replacer.Unpin(1)

	// Pin and immediately unpin
	replacer.Pin(0)
	replacer.Unpin(0)

	// Frame 0 should now be newest (most recently unpinned)
	// Victim should be 1
	victim, ok := replacer.Victim()
	if !ok {
		t.Fatal("Should have a victim")
	}
	if victim != 1 {
		t.Errorf("Expected victim 1, got %d", victim)
	}
}

// TestLRUMultipleVictims tests getting multiple victims in sequence
func TestLRUMultipleVictims(t *testing.T) {
	replacer := NewLRUReplacer(5)

	// Add frames in order
	frames := []uint32{0, 1, 2, 3, 4}
	for _, frame := range frames {
		replacer.Unpin(frame)
	}

	// Get victims in LRU order
	for i, expected := range frames {
		victim, ok := replacer.Victim()
		if !ok {
			t.Fatalf("Should have victim at iteration %d", i)
		}
		if victim != expected {
			t.Errorf("At iteration %d: expected victim %d, got %d", i, expected, victim)
		}

		if replacer.Size() != uint32(len(frames)-i-1) {
			t.Errorf("Expected size %d, got %d", len(frames)-i-1, replacer.Size())
		}
	}

	// Should be empty now
	_, ok := replacer.Victim()
	if ok {
		t.Error("Should not have victim after all evicted")
	}
}
