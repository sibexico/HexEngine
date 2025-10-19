package storage

// Replacer interface for page replacement policies
// Allows different algorithms (LRU, 2Q, ARC, etc.)
type Replacer interface {
	// Victim selects a frame to evict
	// Returns the frame ID and true if a victim was found, false otherwise
	Victim() (uint32, bool)

	// Pin marks a frame as in-use (not evictable)
	Pin(frameID uint32)

	// Unpin marks a frame as available for eviction
	Unpin(frameID uint32)

	// Size returns the number of evictable frames
	Size() uint32
}

// NewReplacer creates a replacer based on the specified algorithm
func NewReplacer(algorithm string, capacity uint32) Replacer {
	switch algorithm {
	case "arc":
		return NewARCReplacer(int(capacity))
	case "2q":
		return NewTwoQReplacer(int(capacity))
	case "lru":
		return NewLRUReplacer(capacity)
	default:
		// Default to ARC for best adaptive performance
		return NewARCReplacer(int(capacity))
	}
}
