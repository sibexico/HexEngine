package storage

import (
	"bytes"
	"testing"
)

func TestCompressLogRecordUpdate(t *testing.T) {
	// Create an update that changes only a few bytes
	beforeData := make([]byte, 100)
	afterData := make([]byte, 100)
	for i := range beforeData {
		beforeData[i] = byte(i)
		afterData[i] = byte(i)
	}

	// Change only 3 bytes
	afterData[10] = 255
	afterData[50] = 128
	afterData[90] = 42

	record := &LogRecord{
		LSN: 1,
		PrevLSN: 0,
		TxnID: 100,
		Type: LogUpdate,
		PageID: 5,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	// Compress
	compressed := CompressLogRecord(record)

	// Verify compression
	if compressed.DataLength != 100 {
		t.Errorf("Expected DataLength 100, got %d", compressed.DataLength)
	}

	if len(compressed.DeltaData) != 3 {
		t.Errorf("Expected 3 changed bytes, got %d", len(compressed.DeltaData))
	}

	// Check changed bytes
	if compressed.DeltaData[0] != 255 || compressed.DeltaData[1] != 128 || compressed.DeltaData[2] != 42 {
		t.Errorf("Delta data incorrect: %v", compressed.DeltaData)
	}

	// Calculate compression ratio
	ratio := compressed.CompressionRatio()
	t.Logf("Compression ratio: %.2fx", ratio)
	if ratio < 10.0 {
		t.Errorf("Expected high compression ratio, got %.2fx", ratio)
	}
}

func TestCompressLogRecordInsert(t *testing.T) {
	// Insert operations store full data
	afterData := []byte("This is new data being inserted")

	record := &LogRecord{
		LSN: 2,
		PrevLSN: 1,
		TxnID: 101,
		Type: LogInsert,
		PageID: 10,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	// Should use full data, not delta
	if len(compressed.FullData) == 0 {
		t.Error("Expected full data for insert")
	}

	if !bytes.Equal(compressed.FullData, afterData) {
		t.Error("Full data does not match")
	}

	if len(compressed.DeltaData) != 0 {
		t.Error("Should not have delta data for insert")
	}
}

func TestCompressLogRecordDelete(t *testing.T) {
	// Delete operations store full before data
	beforeData := []byte("Data being deleted")

	record := &LogRecord{
		LSN: 3,
		PrevLSN: 2,
		TxnID: 102,
		Type: LogDelete,
		PageID: 15,
		BeforeData: beforeData,
	}

	compressed := CompressLogRecord(record)

	// Should use full data
	if len(compressed.FullData) == 0 {
		t.Error("Expected full data for delete")
	}

	if !bytes.Equal(compressed.FullData, beforeData) {
		t.Error("Full data does not match")
	}
}

func TestDecompressLogRecord(t *testing.T) {
	// Create original record with partial update
	beforeData := make([]byte, 50)
	afterData := make([]byte, 50)
	for i := range beforeData {
		beforeData[i] = byte(i * 2)
		afterData[i] = byte(i * 2)
	}
	afterData[5] = 99
	afterData[25] = 88
	afterData[45] = 77

	original := &LogRecord{
		LSN: 10,
		PrevLSN: 9,
		TxnID: 200,
		Type: LogUpdate,
		PageID: 20,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	// Compress
	compressed := CompressLogRecord(original)

	// Decompress
	decompressed, err := DecompressLogRecord(compressed, beforeData)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	// Verify reconstruction
	if decompressed.LSN != original.LSN {
		t.Errorf("LSN mismatch: expected %d, got %d", original.LSN, decompressed.LSN)
	}

	if !bytes.Equal(decompressed.AfterData, afterData) {
		t.Error("Decompressed after data does not match")
		t.Logf("Expected: %v", afterData)
		t.Logf("Got: %v", decompressed.AfterData)
	}
}

func TestSerializeCompressedLogRecord(t *testing.T) {
	// Create a compressed record
	beforeData := make([]byte, 100)
	afterData := make([]byte, 100)
	for i := range beforeData {
		beforeData[i] = byte(i)
		afterData[i] = byte(i)
	}
	afterData[10] = 255
	afterData[50] = 128

	record := &LogRecord{
		LSN: 100,
		PrevLSN: 99,
		TxnID: 500,
		Type: LogUpdate,
		PageID: 30,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	// Serialize
	data := compressed.Serialize()

	// Deserialize
	deserialized, err := DeserializeCompressedLogRecord(data)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify
	if deserialized.LSN != compressed.LSN {
		t.Errorf("LSN mismatch: expected %d, got %d", compressed.LSN, deserialized.LSN)
	}

	if deserialized.TxnID != compressed.TxnID {
		t.Errorf("TxnID mismatch: expected %d, got %d", compressed.TxnID, deserialized.TxnID)
	}

	if !bytes.Equal(deserialized.DeltaData, compressed.DeltaData) {
		t.Error("Delta data mismatch")
	}

	if !bytes.Equal(deserialized.ChangeMask, compressed.ChangeMask) {
		t.Error("Change mask mismatch")
	}
}

func TestCompressionRatioCalculation(t *testing.T) {
	tests := []struct {
		name string
		beforeSize int
		changedBytes int
		minRatio float64 // Minimum acceptable ratio
	}{
		{"1% changed", 1000, 10, 10.0}, // Bitmask overhead limits compression
		{"10% changed", 1000, 100, 6.0}, // More realistic expectation
		{"50% changed", 1000, 500, 2.0}, // Should still compress 2x
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeData := make([]byte, tt.beforeSize)
			afterData := make([]byte, tt.beforeSize)
			for i := range beforeData {
				beforeData[i] = byte(i % 256)
				afterData[i] = byte(i % 256)
			}

			// Change specific bytes
			for i := 0; i < tt.changedBytes; i++ {
				afterData[i] = byte((i + 100) % 256)
			}

			record := &LogRecord{
				LSN: 1,
				TxnID: 1,
				Type: LogUpdate,
				PageID: 1,
				BeforeData: beforeData,
				AfterData: afterData,
			}

			compressed := CompressLogRecord(record)
			ratio := compressed.CompressionRatio()

			t.Logf("Compression ratio: %.2fx (min expected %.2fx)", ratio, tt.minRatio)

			if ratio < tt.minRatio {
				t.Errorf("Compression ratio too low: got %.2fx, expected min %.2fx", ratio, tt.minRatio)
			}
		})
	}
}

func TestCompressEmptyUpdate(t *testing.T) {
	// Edge case: empty data
	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
	}

	compressed := CompressLogRecord(record)

	if compressed.DataLength != 0 {
		t.Errorf("Expected DataLength 0, got %d", compressed.DataLength)
	}
}

func TestCompressSizeMismatch(t *testing.T) {
	// Before and after have different sizes - should fall back to full data
	beforeData := make([]byte, 50)
	afterData := make([]byte, 100)

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	// Should use full data instead of delta
	if len(compressed.FullData) == 0 {
		t.Error("Expected full data for size mismatch")
	}

	if len(compressed.DeltaData) != 0 {
		t.Error("Should not have delta data for size mismatch")
	}
}

func TestDecompressWithoutBeforeData(t *testing.T) {
	// Create delta-compressed record
	beforeData := []byte{1, 2, 3, 4, 5}
	afterData := []byte{1, 2, 99, 4, 5}

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	// Try to decompress without before data
	_, err := DecompressLogRecord(compressed, nil)
	if err == nil {
		t.Error("Expected error when decompressing delta without before data")
	}
}

// Benchmark compression performance
func BenchmarkCompressLogRecord(b *testing.B) {
	// Create a typical update scenario - 4KB page, 10% changed
	beforeData := make([]byte, 4096)
	afterData := make([]byte, 4096)
	for i := range beforeData {
		beforeData[i] = byte(i % 256)
		afterData[i] = byte(i % 256)
	}

	// Change 10% of bytes
	for i := 0; i < 409; i++ {
		afterData[i*10] = byte((i + 100) % 256)
	}

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CompressLogRecord(record)
	}
}

func BenchmarkDecompressLogRecord(b *testing.B) {
	beforeData := make([]byte, 4096)
	afterData := make([]byte, 4096)
	for i := range beforeData {
		beforeData[i] = byte(i % 256)
		afterData[i] = byte(i % 256)
	}

	for i := 0; i < 409; i++ {
		afterData[i*10] = byte((i + 100) % 256)
	}

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecompressLogRecord(compressed, beforeData)
	}
}

func BenchmarkSerializeCompressed(b *testing.B) {
	beforeData := make([]byte, 4096)
	afterData := make([]byte, 4096)
	for i := range beforeData {
		beforeData[i] = byte(i % 256)
		afterData[i] = byte(i % 256)
	}
	afterData[100] = 255

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = compressed.Serialize()
	}
}

func BenchmarkDeserializeCompressed(b *testing.B) {
	beforeData := make([]byte, 4096)
	afterData := make([]byte, 4096)
	for i := range beforeData {
		beforeData[i] = byte(i % 256)
		afterData[i] = byte(i % 256)
	}
	afterData[100] = 255

	record := &LogRecord{
		LSN: 1,
		TxnID: 1,
		Type: LogUpdate,
		PageID: 1,
		BeforeData: beforeData,
		AfterData: afterData,
	}

	compressed := CompressLogRecord(record)
	data := compressed.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DeserializeCompressedLogRecord(data)
	}
}
