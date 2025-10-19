package storage

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure for membership testing
// Used at page level to quickly reject negative lookups without reading page data
type BloomFilter struct {
	bits []byte // Bit array (packed into bytes)
	numBits uint32 // Total number of bits
	numHashes uint32 // Number of hash functions to use
	numInserts uint32 // Number of elements inserted (for statistics)
}

// BloomFilterConfig holds configuration for creating a Bloom filter
type BloomFilterConfig struct {
	ExpectedElements uint32 // Expected number of elements to insert
	FalsePositiveRate float64 // Target false positive rate (e.g., 0.01 = 1%)
}

// DefaultBloomFilterConfig returns a default configuration suitable for page-level filtering
// Assumes ~100 keys per page with 1% false positive rate
func DefaultBloomFilterConfig() BloomFilterConfig {
	return BloomFilterConfig{
		ExpectedElements: 100,
		FalsePositiveRate: 0.01,
	}
}

// NewBloomFilter creates a new Bloom filter with the given configuration
func NewBloomFilter(config BloomFilterConfig) *BloomFilter {
	// Calculate optimal number of bits: m = -n*ln(p) / (ln(2)^2)
	// where n = expected elements, p = false positive rate
	n := float64(config.ExpectedElements)
	p := config.FalsePositiveRate

	numBits := uint32(math.Ceil(-n * math.Log(p) / (math.Ln2 * math.Ln2)))

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	numHashes := uint32(math.Ceil(float64(numBits) / n * math.Ln2))

	// Ensure at least 1 hash function
	if numHashes < 1 {
		numHashes = 1
	}

	// Calculate byte array size
	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits: make([]byte, numBytes),
		numBits: numBits,
		numHashes: numHashes,
	}
}

// NewBloomFilterFromBytes reconstructs a Bloom filter from serialized bytes
func NewBloomFilterFromBytes(data []byte, numHashes uint32) *BloomFilter {
	if len(data) == 0 {
		return nil
	}

	numBits := uint32(len(data)) * 8

	return &BloomFilter{
		bits: data,
		numBits: numBits,
		numHashes: numHashes,
	}
}

// Insert adds an element to the Bloom filter
func (bf *BloomFilter) Insert(key []byte) {
	hashes := bf.getHashes(key)

	for _, h := range hashes {
		bitIndex := h % bf.numBits
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8

		bf.bits[byteIndex] |= 1 << bitOffset
	}

	bf.numInserts++
}

// MayContain checks if an element might be in the set
// Returns true if element might exist (could be false positive)
// Returns false if element definitely does not exist (no false negatives)
func (bf *BloomFilter) MayContain(key []byte) bool {
	hashes := bf.getHashes(key)

	for _, h := range hashes {
		bitIndex := h % bf.numBits
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8

		if (bf.bits[byteIndex] & (1 << bitOffset)) == 0 {
			return false // Definitely not present
		}
	}

	return true // Might be present (or false positive)
}

// getHashes generates k independent hash values for the key
// Uses double hashing technique: h_i(x) = h1(x) + i*h2(x) mod m
func (bf *BloomFilter) getHashes(key []byte) []uint32 {
	// Generate two base hashes
	h1 := bf.hash1(key)
	h2 := bf.hash2(key)

	hashes := make([]uint32, bf.numHashes)

	for i := uint32(0); i < bf.numHashes; i++ {
		// Double hashing: combines two hash functions to generate k hashes
		hashes[i] = (h1 + i*h2) % bf.numBits
	}

	return hashes
}

// hash1 computes the first hash function using FNV-1a
func (bf *BloomFilter) hash1(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

// hash2 computes the second hash function using FNV-1
func (bf *BloomFilter) hash2(key []byte) uint32 {
	h := fnv.New32()
	h.Write(key)
	sum := h.Sum32()

	// Ensure hash2 is never 0 (would make all double hashes equal to hash1)
	if sum == 0 {
		return 1
	}

	return sum
}

// Clear resets the Bloom filter to empty state
func (bf *BloomFilter) Clear() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.numInserts = 0
}

// GetBytes returns the raw byte array (for serialization)
func (bf *BloomFilter) GetBytes() []byte {
	return bf.bits
}

// GetNumBits returns the total number of bits in the filter
func (bf *BloomFilter) GetNumBits() uint32 {
	return bf.numBits
}

// GetNumHashes returns the number of hash functions used
func (bf *BloomFilter) GetNumHashes() uint32 {
	return bf.numHashes
}

// GetNumInserts returns the number of elements inserted
func (bf *BloomFilter) GetNumInserts() uint32 {
	return bf.numInserts
}

// EstimateFalsePositiveRate calculates the current false positive rate
// Based on: p ≈ (1 - e^(-kn/m))^k
// where k = num hashes, n = num inserts, m = num bits
func (bf *BloomFilter) EstimateFalsePositiveRate() float64 {
	if bf.numInserts == 0 {
		return 0.0
	}

	k := float64(bf.numHashes)
	n := float64(bf.numInserts)
	m := float64(bf.numBits)

	// Calculate p = (1 - e^(-kn/m))^k
	exponent := -k * n / m
	base := 1.0 - math.Exp(exponent)
	fpr := math.Pow(base, k)

	return fpr
}

// GetFillRatio returns the fraction of bits set to 1
func (bf *BloomFilter) GetFillRatio() float64 {
	bitsSet := 0

	for _, b := range bf.bits {
		// Count bits using Brian Kernighan's algorithm
		for b > 0 {
			b &= b - 1 // Clear the lowest set bit
			bitsSet++
		}
	}

	return float64(bitsSet) / float64(bf.numBits)
}

// Serialize converts the Bloom filter to a byte slice for storage
// Format: [numBits:4][numHashes:4][numInserts:4][bits:variable]
func (bf *BloomFilter) Serialize() []byte {
	headerSize := 12 // 3 × uint32
	totalSize := headerSize + len(bf.bits)

	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], bf.numBits)
	binary.LittleEndian.PutUint32(data[4:8], bf.numHashes)
	binary.LittleEndian.PutUint32(data[8:12], bf.numInserts)
	copy(data[12:], bf.bits)

	return data
}

// DeserializeBloomFilter reconstructs a Bloom filter from serialized bytes
func DeserializeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("invalid bloom filter data: too short")
	}

	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHashes := binary.LittleEndian.Uint32(data[4:8])
	numInserts := binary.LittleEndian.Uint32(data[8:12])

	expectedBytes := (numBits + 7) / 8
	if uint32(len(data)-12) != expectedBytes {
		return nil, fmt.Errorf("invalid bloom filter data: size mismatch")
	}

	bits := make([]byte, expectedBytes)
	copy(bits, data[12:])

	return &BloomFilter{
		bits: bits,
		numBits: numBits,
		numHashes: numHashes,
		numInserts: numInserts,
	}, nil
}

// Union merges another Bloom filter into this one (bitwise OR)
// Both filters must have the same configuration (numBits, numHashes)
func (bf *BloomFilter) Union(other *BloomFilter) error {
	if bf.numBits != other.numBits || bf.numHashes != other.numHashes {
		return fmt.Errorf("bloom filters have incompatible configurations")
	}

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}

	// Update insert count (approximate)
	bf.numInserts += other.numInserts

	return nil
}

// Intersect performs bitwise AND with another Bloom filter
// Both filters must have the same configuration (numBits, numHashes)
func (bf *BloomFilter) Intersect(other *BloomFilter) error {
	if bf.numBits != other.numBits || bf.numHashes != other.numHashes {
		return fmt.Errorf("bloom filters have incompatible configurations")
	}

	for i := range bf.bits {
		bf.bits[i] &= other.bits[i]
	}

	// Insert count is approximate after intersection
	if other.numInserts < bf.numInserts {
		bf.numInserts = other.numInserts
	}

	return nil
}

// Clone creates a deep copy of the Bloom filter
func (bf *BloomFilter) Clone() *BloomFilter {
	bits := make([]byte, len(bf.bits))
	copy(bits, bf.bits)

	return &BloomFilter{
		bits: bits,
		numBits: bf.numBits,
		numHashes: bf.numHashes,
		numInserts: bf.numInserts,
	}
}

// PageBloomFilter attaches a Bloom filter to a page for key lookups
type PageBloomFilter struct {
	pageID uint32
	filter *BloomFilter
}

// NewPageBloomFilter creates a new page-level Bloom filter
func NewPageBloomFilter(pageID uint32, config BloomFilterConfig) *PageBloomFilter {
	return &PageBloomFilter{
		pageID: pageID,
		filter: NewBloomFilter(config),
	}
}

// InsertKey adds a key to the page's Bloom filter
func (pbf *PageBloomFilter) InsertKey(key []byte) {
	pbf.filter.Insert(key)
}

// MayContainKey checks if a key might exist in the page
func (pbf *PageBloomFilter) MayContainKey(key []byte) bool {
	return pbf.filter.MayContain(key)
}

// GetPageID returns the page ID this filter belongs to
func (pbf *PageBloomFilter) GetPageID() uint32 {
	return pbf.pageID
}

// GetFilter returns the underlying Bloom filter
func (pbf *PageBloomFilter) GetFilter() *BloomFilter {
	return pbf.filter
}

// GetStats returns statistics about the page Bloom filter
func (pbf *PageBloomFilter) GetStats() PageBloomFilterStats {
	return PageBloomFilterStats{
		PageID: pbf.pageID,
		NumInserts: pbf.filter.GetNumInserts(),
		NumBits: pbf.filter.GetNumBits(),
		NumHashes: pbf.filter.GetNumHashes(),
		FillRatio: pbf.filter.GetFillRatio(),
		EstimatedFPR: pbf.filter.EstimateFalsePositiveRate(),
	}
}

// PageBloomFilterStats holds statistics for a page Bloom filter
type PageBloomFilterStats struct {
	PageID uint32
	NumInserts uint32
	NumBits uint32
	NumHashes uint32
	FillRatio float64
	EstimatedFPR float64
}
