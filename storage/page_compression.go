package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// CompressionType represents the compression algorithm used
type CompressionType uint8

const (
	CompressionNone   CompressionType = 0
	CompressionLZ4    CompressionType = 1
	CompressionSnappy CompressionType = 2
)

// CompressedPage represents a compressed page with metadata
type CompressedPage struct {
	CompressionType  CompressionType
	UncompressedSize uint16
	CompressedSize   uint16
	CompressedData   []byte
	OriginalChecksum uint32 // CRC32 of original data
}

// Page compression header layout:
// [0-1]: Magic number (0xC0DE for compressed pages)
// [2]: Compression type (0=none, 1=LZ4, 2=Snappy)
// [3]: Reserved
// [4-5]: Uncompressed size
// [6-7]: Compressed size
// [8-11]: Original checksum (CRC32)
// [12+]: Compressed data

const (
	CompressedPageMagic     = 0xC0DE
	CompressedHeaderSize    = 12
	MinCompressionThreshold = 100 // Minimum bytes saved to use compression
)

// CompressPage compresses a page using the specified algorithm
func CompressPage(data []byte, compressionType CompressionType) (*CompressedPage, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(data))
	}

	// Calculate checksum of original data
	checksum := crc32Checksum(data)

	var compressed []byte

	switch compressionType {
	case CompressionNone:
		compressed = data

	case CompressionLZ4:
		// LZ4 compression
		compressed = make([]byte, lz4.CompressBlockBound(len(data)))
		n, err := lz4.CompressBlock(data, compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("LZ4 compression failed: %w", err)
		}
		compressed = compressed[:n]

	case CompressionSnappy:
		// Snappy compression
		compressed = snappy.Encode(nil, data)

	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}

	compressedSize := uint16(len(compressed))

	// Check if compression is worthwhile
	if compressionType != CompressionNone {
		savings := len(data) - len(compressed)
		if savings < MinCompressionThreshold {
			// Compression not worth it, use uncompressed
			compressionType = CompressionNone
			compressed = data
			compressedSize = uint16(len(data))
		}
	}

	return &CompressedPage{
		CompressionType:  compressionType,
		UncompressedSize: uint16(len(data)),
		CompressedSize:   compressedSize,
		CompressedData:   compressed,
		OriginalChecksum: checksum,
	}, nil
}

// DecompressPage decompresses a compressed page
func DecompressPage(cp *CompressedPage) ([]byte, error) {
	var decompressed []byte
	var err error

	switch cp.CompressionType {
	case CompressionNone:
		decompressed = cp.CompressedData

	case CompressionLZ4:
		// LZ4 decompression
		decompressed = make([]byte, cp.UncompressedSize)
		n, err := lz4.UncompressBlock(cp.CompressedData, decompressed)
		if err != nil {
			return nil, fmt.Errorf("LZ4 decompression failed: %w", err)
		}
		if n != int(cp.UncompressedSize) {
			return nil, fmt.Errorf("LZ4 decompression size mismatch: got %d, expected %d", n, cp.UncompressedSize)
		}

	case CompressionSnappy:
		// Snappy decompression
		decompressed, err = snappy.Decode(nil, cp.CompressedData)
		if err != nil {
			return nil, fmt.Errorf("snappy decompression failed: %w", err)
		}
		if len(decompressed) != int(cp.UncompressedSize) {
			return nil, fmt.Errorf("snappy decompression size mismatch: got %d, expected %d", len(decompressed), cp.UncompressedSize)
		}

	default:
		return nil, fmt.Errorf("unsupported compression type: %d", cp.CompressionType)
	}

	// Verify checksum
	checksum := crc32Checksum(decompressed)
	if checksum != cp.OriginalChecksum {
		return nil, fmt.Errorf("checksum mismatch: got %08x, expected %08x", checksum, cp.OriginalChecksum)
	}

	return decompressed, nil
}

// SerializeCompressedPage serializes a compressed page to bytes
func SerializeCompressedPage(cp *CompressedPage) ([]byte, error) {
	totalSize := CompressedHeaderSize + len(cp.CompressedData)
	if totalSize > PageSize {
		return nil, fmt.Errorf("compressed page too large: %d bytes (max %d)", totalSize, PageSize)
	}

	// Create buffer (pad to PageSize for disk writes)
	buf := make([]byte, PageSize)

	// Write magic number
	binary.LittleEndian.PutUint16(buf[0:2], CompressedPageMagic)

	// Write compression type
	buf[2] = uint8(cp.CompressionType)

	// Reserved byte
	buf[3] = 0

	// Write sizes
	binary.LittleEndian.PutUint16(buf[4:6], cp.UncompressedSize)
	binary.LittleEndian.PutUint16(buf[6:8], cp.CompressedSize)

	// Write checksum
	binary.LittleEndian.PutUint32(buf[8:12], cp.OriginalChecksum)

	// Write compressed data
	copy(buf[CompressedHeaderSize:], cp.CompressedData)

	return buf, nil
}

// DeserializeCompressedPage deserializes a compressed page from bytes
func DeserializeCompressedPage(data []byte) (*CompressedPage, error) {
	if len(data) < CompressedHeaderSize {
		return nil, fmt.Errorf("data too short for compressed page header: %d bytes", len(data))
	}

	// Read magic number
	magic := binary.LittleEndian.Uint16(data[0:2])
	if magic != CompressedPageMagic {
		return nil, fmt.Errorf("invalid magic number: got %04x, expected %04x", magic, CompressedPageMagic)
	}

	// Read compression type
	compressionType := CompressionType(data[2])

	// Read sizes
	uncompressedSize := binary.LittleEndian.Uint16(data[4:6])
	compressedSize := binary.LittleEndian.Uint16(data[6:8])

	// Read checksum
	checksum := binary.LittleEndian.Uint32(data[8:12])

	// Read compressed data
	if CompressedHeaderSize+int(compressedSize) > len(data) {
		return nil, fmt.Errorf("insufficient data for compressed page: need %d bytes, have %d",
			CompressedHeaderSize+int(compressedSize), len(data))
	}

	compressedData := make([]byte, compressedSize)
	copy(compressedData, data[CompressedHeaderSize:CompressedHeaderSize+int(compressedSize)])

	return &CompressedPage{
		CompressionType:  compressionType,
		UncompressedSize: uncompressedSize,
		CompressedSize:   compressedSize,
		CompressedData:   compressedData,
		OriginalChecksum: checksum,
	}, nil
}

// IsCompressedPage checks if the page data represents a compressed page
func IsCompressedPage(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	magic := binary.LittleEndian.Uint16(data[0:2])
	return magic == CompressedPageMagic
}

// GetCompressionRatio returns the compression ratio (original size / compressed size)
func (cp *CompressedPage) GetCompressionRatio() float64 {
	if cp.CompressedSize == 0 {
		return 1.0
	}
	return float64(cp.UncompressedSize) / float64(cp.CompressedSize)
}

// GetSpaceSavings returns bytes saved by compression
func (cp *CompressedPage) GetSpaceSavings() int {
	return int(cp.UncompressedSize) - int(cp.CompressedSize)
}

// CompressPageTransparent compresses a page and returns serialized form (compression + serialization).
func CompressPageTransparent(data []byte, compressionType CompressionType) ([]byte, error) {
	cp, err := CompressPage(data, compressionType)
	if err != nil {
		return nil, err
	}

	return SerializeCompressedPage(cp)
}

// DecompressPageTransparent detects if page is compressed and decompresses if needed
// Returns original data if not compressed
func DecompressPageTransparent(data []byte) ([]byte, error) {
	if !IsCompressedPage(data) {
		// Not compressed, return as-is
		return data, nil
	}

	cp, err := DeserializeCompressedPage(data)
	if err != nil {
		return nil, err
	}

	return DecompressPage(cp)
}

// CRC32 checksum helper
func crc32Checksum(data []byte) uint32 {
	// Simple CRC32 implementation (using IEEE polynomial)
	const poly = 0xEDB88320
	crc := uint32(0xFFFFFFFF)

	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
	}

	return ^crc
}

// PageCompressionStats tracks compression statistics
type PageCompressionStats struct {
	TotalPages         uint64
	CompressedPages    uint64
	UncompressedPages  uint64
	TotalBytesOriginal uint64
	TotalBytesStored   uint64
	LZ4Count           uint64
	SnappyCount        uint64
	NoneCount          uint64
}

// AddCompression updates stats for a compression operation
func (pcs *PageCompressionStats) AddCompression(cp *CompressedPage) {
	pcs.TotalPages++
	pcs.TotalBytesOriginal += uint64(cp.UncompressedSize)
	pcs.TotalBytesStored += uint64(cp.CompressedSize) + CompressedHeaderSize

	switch cp.CompressionType {
	case CompressionNone:
		pcs.UncompressedPages++
		pcs.NoneCount++
	case CompressionLZ4:
		pcs.CompressedPages++
		pcs.LZ4Count++
	case CompressionSnappy:
		pcs.CompressedPages++
		pcs.SnappyCount++
	}
}

// GetCompressionRatio returns overall compression ratio
func (pcs *PageCompressionStats) GetCompressionRatio() float64 {
	if pcs.TotalBytesStored == 0 {
		return 1.0
	}
	return float64(pcs.TotalBytesOriginal) / float64(pcs.TotalBytesStored)
}

// GetSpaceSavings returns total bytes saved
func (pcs *PageCompressionStats) GetSpaceSavings() uint64 {
	if pcs.TotalBytesOriginal > pcs.TotalBytesStored {
		return pcs.TotalBytesOriginal - pcs.TotalBytesStored
	}
	return 0
}

// GetCompressionPercentage returns percentage of pages compressed
func (pcs *PageCompressionStats) GetCompressionPercentage() float64 {
	if pcs.TotalPages == 0 {
		return 0.0
	}
	return float64(pcs.CompressedPages) / float64(pcs.TotalPages) * 100.0
}

// ChooseBestCompression tries all algorithms and returns the best one
func ChooseBestCompression(data []byte) (*CompressedPage, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("page data must be exactly %d bytes", PageSize)
	}

	// Try LZ4
	lz4Page, err := CompressPage(data, CompressionLZ4)
	if err != nil {
		return nil, err
	}

	// Try Snappy
	snappyPage, err := CompressPage(data, CompressionSnappy)
	if err != nil {
		return nil, err
	}

	// Return whichever gives better compression
	if lz4Page.CompressedSize < snappyPage.CompressedSize {
		return lz4Page, nil
	}
	return snappyPage, nil
}
