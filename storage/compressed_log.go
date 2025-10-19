package storage

import (
	"encoding/binary"
	"fmt"
)

// CompressedLogRecord represents a WAL entry with delta compression.
// Stores only changed bytes instead of full before/after images (5-10x compression).
type CompressedLogRecord struct {
	LSN     uint64
	PrevLSN uint64
	TxnID   uint64
	Type    LogType
	PageID  uint32

	// Delta compression fields
	DataLength uint16 // Total length of the data region
	ChangeMask []byte // Bitmap indicating which bytes changed
	DeltaData  []byte // Only the changed bytes

	// For non-update operations, store full data
	FullData []byte
}

// CompressLogRecord compresses a standard log record using delta encoding
func CompressLogRecord(record *LogRecord) *CompressedLogRecord {
	compressed := &CompressedLogRecord{
		LSN:     record.LSN,
		PrevLSN: record.PrevLSN,
		TxnID:   record.TxnID,
		Type:    record.Type,
		PageID:  record.PageID,
	}

	// For UPDATE operations, use delta compression
	if record.Type == LogUpdate && len(record.BeforeData) > 0 && len(record.AfterData) > 0 {
		if len(record.BeforeData) != len(record.AfterData) {
			// Size changed - fall back to full data
			compressed.FullData = record.AfterData
			compressed.DataLength = uint16(len(record.AfterData))
			return compressed
		}

		// Find changed bytes
		dataLen := len(record.BeforeData)
		compressed.DataLength = uint16(dataLen)

		// Create change mask (1 bit per byte)
		maskSize := (dataLen + 7) / 8 // Ceiling division
		compressed.ChangeMask = make([]byte, maskSize)

		// Collect changed bytes
		var deltaBytes []byte
		for i := 0; i < dataLen; i++ {
			if record.BeforeData[i] != record.AfterData[i] {
				// Mark this byte as changed in the mask
				byteIdx := i / 8
				bitIdx := uint(i % 8)
				compressed.ChangeMask[byteIdx] |= (1 << bitIdx)

				// Add the new value to delta data
				deltaBytes = append(deltaBytes, record.AfterData[i])
			}
		}

		compressed.DeltaData = deltaBytes
	} else {
		// For INSERT, DELETE, COMMIT, ABORT - store full data
		if len(record.AfterData) > 0 {
			compressed.FullData = record.AfterData
			compressed.DataLength = uint16(len(record.AfterData))
		} else if len(record.BeforeData) > 0 {
			compressed.FullData = record.BeforeData
			compressed.DataLength = uint16(len(record.BeforeData))
		}
	}

	return compressed
}

// DecompressLogRecord reconstructs the original log record
func DecompressLogRecord(compressed *CompressedLogRecord, beforeData []byte) (*LogRecord, error) {
	record := &LogRecord{
		LSN:     compressed.LSN,
		PrevLSN: compressed.PrevLSN,
		TxnID:   compressed.TxnID,
		Type:    compressed.Type,
		PageID:  compressed.PageID,
	}

	// If we have delta data, reconstruct the after image
	if len(compressed.DeltaData) > 0 && len(compressed.ChangeMask) > 0 {
		if beforeData == nil {
			return nil, fmt.Errorf("beforeData required for delta decompression")
		}

		dataLen := int(compressed.DataLength)
		if len(beforeData) != dataLen {
			return nil, fmt.Errorf("beforeData length mismatch: expected %d, got %d", dataLen, len(beforeData))
		}

		// Reconstruct after image
		afterData := make([]byte, dataLen)
		copy(afterData, beforeData)

		deltaIdx := 0
		for i := 0; i < dataLen; i++ {
			byteIdx := i / 8
			bitIdx := uint(i % 8)

			// Check if this byte changed
			if byteIdx < len(compressed.ChangeMask) && (compressed.ChangeMask[byteIdx]&(1<<bitIdx)) != 0 {
				if deltaIdx >= len(compressed.DeltaData) {
					return nil, fmt.Errorf("delta data exhausted at index %d", i)
				}
				afterData[i] = compressed.DeltaData[deltaIdx]
				deltaIdx++
			}
		}

		record.BeforeData = beforeData
		record.AfterData = afterData
	} else if len(compressed.FullData) > 0 {
		// Full data stored
		if compressed.Type == LogUpdate || compressed.Type == LogInsert {
			record.AfterData = compressed.FullData
		} else if compressed.Type == LogDelete {
			record.BeforeData = compressed.FullData
		}
	}

	return record, nil
}

// Serialize converts CompressedLogRecord to bytes
// Format: LSN(8) | PrevLSN(8) | TxnID(8) | Type(1) | PageID(4) | DataLength(2) |
//
//	MaskLen(2) | ChangeMask | DeltaLen(2) | DeltaData | FullDataLen(2) | FullData
func (clr *CompressedLogRecord) Serialize() []byte {
	// Calculate size
	size := 8 + 8 + 8 + 1 + 4 + 2 + // Fixed header: 31 bytes
		2 + len(clr.ChangeMask) + // Mask length + mask
		2 + len(clr.DeltaData) + // Delta length + delta
		2 + len(clr.FullData) // Full data length + full data

	buf := make([]byte, size)
	offset := 0

	// Fixed header
	binary.LittleEndian.PutUint64(buf[offset:], clr.LSN)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], clr.PrevLSN)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], clr.TxnID)
	offset += 8
	buf[offset] = byte(clr.Type)
	offset += 1
	binary.LittleEndian.PutUint32(buf[offset:], clr.PageID)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], clr.DataLength)
	offset += 2

	// Change mask
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(clr.ChangeMask)))
	offset += 2
	copy(buf[offset:], clr.ChangeMask)
	offset += len(clr.ChangeMask)

	// Delta data
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(clr.DeltaData)))
	offset += 2
	copy(buf[offset:], clr.DeltaData)
	offset += len(clr.DeltaData)

	// Full data
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(clr.FullData)))
	offset += 2
	copy(buf[offset:], clr.FullData)

	return buf
}

// DeserializeCompressedLogRecord creates CompressedLogRecord from bytes
func DeserializeCompressedLogRecord(data []byte) (*CompressedLogRecord, error) {
	minSize := 31 + 2 + 2 + 2 // Fixed header + 3 length fields
	if len(data) < minSize {
		return nil, fmt.Errorf("data too short: %d bytes (need at least %d)", len(data), minSize)
	}

	clr := &CompressedLogRecord{}
	offset := 0

	// Fixed header
	clr.LSN = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	clr.PrevLSN = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	clr.TxnID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	clr.Type = LogType(data[offset])
	offset += 1
	clr.PageID = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	clr.DataLength = binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// Change mask
	maskLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if maskLen > 0 {
		if offset+int(maskLen) > len(data) {
			return nil, fmt.Errorf("invalid mask length: %d", maskLen)
		}
		clr.ChangeMask = make([]byte, maskLen)
		copy(clr.ChangeMask, data[offset:offset+int(maskLen)])
		offset += int(maskLen)
	}

	// Delta data
	deltaLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if deltaLen > 0 {
		if offset+int(deltaLen) > len(data) {
			return nil, fmt.Errorf("invalid delta length: %d", deltaLen)
		}
		clr.DeltaData = make([]byte, deltaLen)
		copy(clr.DeltaData, data[offset:offset+int(deltaLen)])
		offset += int(deltaLen)
	}

	// Full data
	fullLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if fullLen > 0 {
		if offset+int(fullLen) > len(data) {
			return nil, fmt.Errorf("invalid full data length: %d", fullLen)
		}
		clr.FullData = make([]byte, fullLen)
		copy(clr.FullData, data[offset:offset+int(fullLen)])
	}

	return clr, nil
}

// CompressionRatio returns the compression ratio achieved
func (clr *CompressedLogRecord) CompressionRatio() float64 {
	// Original size would be DataLength * 2 (before + after)
	originalSize := int(clr.DataLength) * 2
	if originalSize == 0 {
		return 1.0
	}

	// Compressed size
	compressedSize := len(clr.ChangeMask) + len(clr.DeltaData)
	if compressedSize == 0 {
		compressedSize = len(clr.FullData)
	}

	if compressedSize == 0 {
		return 1.0
	}

	return float64(originalSize) / float64(compressedSize)
}
