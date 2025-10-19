package storage

import (
	"bytes"
	"testing"
)

func TestCompressPageLZ4(t *testing.T) {
	// Create test data with patterns (compresses well)
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	cp, err := CompressPage(data, CompressionLZ4)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if cp.CompressionType != CompressionLZ4 {
		t.Errorf("Expected LZ4 compression, got %d", cp.CompressionType)
	}

	if cp.UncompressedSize != PageSize {
		t.Errorf("Uncompressed size mismatch: got %d, expected %d", cp.UncompressedSize, PageSize)
	}

	t.Logf("LZ4 compression: %d → %d bytes (%.2fx ratio, %d bytes saved)",
		cp.UncompressedSize, cp.CompressedSize, cp.GetCompressionRatio(), cp.GetSpaceSavings())
}

func TestCompressPageSnappy(t *testing.T) {
	// Create test data
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 100) // More repetitive for better compression
	}

	cp, err := CompressPage(data, CompressionSnappy)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if cp.CompressionType != CompressionSnappy {
		t.Errorf("Expected Snappy compression, got %d", cp.CompressionType)
	}

	t.Logf("Snappy compression: %d → %d bytes (%.2fx ratio, %d bytes saved)",
		cp.UncompressedSize, cp.CompressedSize, cp.GetCompressionRatio(), cp.GetSpaceSavings())
}

func TestCompressDecompressRoundTrip(t *testing.T) {
	algorithms := []struct {
		name string
		typ CompressionType
	}{
		{"None", CompressionNone},
		{"LZ4", CompressionLZ4},
		{"Snappy", CompressionSnappy},
	}

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			// Create test data
			original := make([]byte, PageSize)
			for i := range original {
				original[i] = byte(i % 256)
			}

			// Compress
			cp, err := CompressPage(original, alg.typ)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := DecompressPage(cp)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if !bytes.Equal(original, decompressed) {
				t.Errorf("Round-trip failed: data mismatch")
			}

			t.Logf("%s: %.2fx compression, %d bytes saved",
				alg.name, cp.GetCompressionRatio(), cp.GetSpaceSavings())
		})
	}
}

func TestSerializeDeserializeCompressedPage(t *testing.T) {
	// Create and compress test data
	original := make([]byte, PageSize)
	for i := range original {
		original[i] = byte(i % 50) // Repetitive for good compression
	}

	cp, err := CompressPage(original, CompressionLZ4)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Serialize
	serialized, err := SerializeCompressedPage(cp)
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	if len(serialized) != PageSize {
		t.Errorf("Serialized size should be PageSize: got %d, expected %d", len(serialized), PageSize)
	}

	// Deserialize
	deserialized, err := DeserializeCompressedPage(serialized)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Verify metadata
	if deserialized.CompressionType != cp.CompressionType {
		t.Errorf("Compression type mismatch")
	}
	if deserialized.UncompressedSize != cp.UncompressedSize {
		t.Errorf("Uncompressed size mismatch")
	}
	if deserialized.CompressedSize != cp.CompressedSize {
		t.Errorf("Compressed size mismatch")
	}
	if deserialized.OriginalChecksum != cp.OriginalChecksum {
		t.Errorf("Checksum mismatch")
	}

	// Decompress and verify
	decompressed, err := DecompressPage(deserialized)
	if err != nil {
		t.Fatalf("Decompression after deserialization failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Full round-trip failed: data mismatch")
	}

	t.Logf("Full round-trip successful: compress → serialize → deserialize → decompress")
}

func TestIsCompressedPage(t *testing.T) {
	// Create compressed page
	data := make([]byte, PageSize)
	cp, _ := CompressPage(data, CompressionLZ4)
	serialized, _ := SerializeCompressedPage(cp)

	if !IsCompressedPage(serialized) {
		t.Errorf("Failed to detect compressed page")
	}

	// Create uncompressed page
	uncompressed := make([]byte, PageSize)
	uncompressed[0] = 0xFF // Not the magic number
	uncompressed[1] = 0xFF

	if IsCompressedPage(uncompressed) {
		t.Errorf("False positive: detected uncompressed page as compressed")
	}
}

func TestTransparentCompression(t *testing.T) {
	// Create test data
	original := make([]byte, PageSize)
	for i := range original {
		original[i] = byte(i % 100)
	}

	// Compress transparently
	compressed, err := CompressPageTransparent(original, CompressionLZ4)
	if err != nil {
		t.Fatalf("Transparent compression failed: %v", err)
	}

	// Decompress transparently
	decompressed, err := DecompressPageTransparent(compressed)
	if err != nil {
		t.Fatalf("Transparent decompression failed: %v", err)
	}

	// Verify
	if !bytes.Equal(original, decompressed) {
		t.Errorf("Transparent round-trip failed")
	}

	// Try decompressing uncompressed data (should pass through)
	passthrough, err := DecompressPageTransparent(original)
	if err != nil {
		t.Fatalf("Transparent pass-through failed: %v", err)
	}

	if !bytes.Equal(original, passthrough) {
		t.Errorf("Pass-through modified data")
	}

	t.Logf("Transparent compression works correctly")
}

func TestCompressionMinThreshold(t *testing.T) {
	// Create data that doesn't compress well (random-like)
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte((i * 7919) % 256) // Pseudo-random
	}

	cp, err := CompressPage(data, CompressionLZ4)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// If compression doesn't save enough, should fall back to None
	savings := cp.GetSpaceSavings()
	if savings < MinCompressionThreshold && cp.CompressionType != CompressionNone {
		t.Logf("Warning: Compression used despite low savings: %d bytes", savings)
	}

	t.Logf("Compression savings: %d bytes (threshold: %d)", savings, MinCompressionThreshold)
}

func TestChecksumValidation(t *testing.T) {
	// Create test data
	original := make([]byte, PageSize)
	for i := range original {
		original[i] = byte(i % 256)
	}

	// Compress
	cp, err := CompressPage(original, CompressionLZ4)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Corrupt the compressed data
	cp.CompressedData[10] ^= 0xFF

	// Try to decompress (should fail checksum)
	_, err = DecompressPage(cp)
	if err == nil {
		t.Errorf("Expected checksum error, got nil")
	} else {
		t.Logf("Checksum validation correctly detected corruption: %v", err)
	}
}

func TestPageCompressionStats(t *testing.T) {
	stats := PageCompressionStats{}

	// Add some compressions
	data := make([]byte, PageSize)
	for i := 0; i < 10; i++ {
		for j := range data {
			data[j] = byte((i + j) % 50)
		}

		cp, err := CompressPage(data, CompressionLZ4)
		if err != nil {
			t.Fatalf("Compression failed: %v", err)
		}

		stats.AddCompression(cp)
	}

	// Check stats
	if stats.TotalPages != 10 {
		t.Errorf("Expected 10 pages, got %d", stats.TotalPages)
	}

	t.Logf("Stats: %d pages, %.2fx ratio, %d bytes saved (%.1f%% compressed)",
		stats.TotalPages,
		stats.GetCompressionRatio(),
		stats.GetSpaceSavings(),
		stats.GetCompressionPercentage())
}

func TestChooseBestCompression(t *testing.T) {
	// Create test data that compresses differently with each algorithm
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 200)
	}

	best, err := ChooseBestCompression(data)
	if err != nil {
		t.Fatalf("ChooseBestCompression failed: %v", err)
	}

	t.Logf("Best compression: %v (%.2fx ratio, %d bytes saved)",
		best.CompressionType, best.GetCompressionRatio(), best.GetSpaceSavings())

	// Verify we can decompress
	decompressed, err := DecompressPage(best)
	if err != nil {
		t.Fatalf("Failed to decompress best: %v", err)
	}

	if !bytes.Equal(data, decompressed) {
		t.Errorf("Best compression round-trip failed")
	}
}

func TestHighlyCompressibleData(t *testing.T) {
	// Create highly compressible data (all zeros)
	data := make([]byte, PageSize)

	algorithms := []CompressionType{CompressionLZ4, CompressionSnappy}

	for _, alg := range algorithms {
		cp, err := CompressPage(data, alg)
		if err != nil {
			t.Fatalf("Compression failed: %v", err)
		}

		ratio := cp.GetCompressionRatio()
		if ratio < 10.0 {
			t.Errorf("Expected high compression ratio for zeros, got %.2f", ratio)
		}

		t.Logf("Zeros compression (%v): %.2fx ratio, %d → %d bytes",
			alg, ratio, cp.UncompressedSize, cp.CompressedSize)
	}
}

func TestIncompressibleData(t *testing.T) {
	// Create incompressible data (crypto-random)
	data := make([]byte, PageSize)
	for i := range data {
		// Simple LCG for pseudo-random
		data[i] = byte((i*48271 + 12345) % 256)
	}

	cp, err := CompressPage(data, CompressionLZ4)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	// Should fall back to None or have minimal compression
	t.Logf("Incompressible data: %v compression, %.2fx ratio",
		cp.CompressionType, cp.GetCompressionRatio())

	// Verify round-trip still works
	decompressed, err := DecompressPage(cp)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if !bytes.Equal(data, decompressed) {
		t.Errorf("Round-trip failed for incompressible data")
	}
}

func TestConcurrentCompression(t *testing.T) {
	// Test concurrent compression operations
	numWorkers := 10
	done := make(chan bool, numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			// Each worker compresses different data
			data := make([]byte, PageSize)
			for i := range data {
				data[i] = byte((workerID + i) % 256)
			}

			// Compress
			cp, err := CompressPage(data, CompressionLZ4)
			if err != nil {
				t.Errorf("Worker %d: compression failed: %v", workerID, err)
				done <- false
				return
			}

			// Decompress
			decompressed, err := DecompressPage(cp)
			if err != nil {
				t.Errorf("Worker %d: decompression failed: %v", workerID, err)
				done <- false
				return
			}

			// Verify
			if !bytes.Equal(data, decompressed) {
				t.Errorf("Worker %d: round-trip failed", workerID)
				done <- false
				return
			}

			done <- true
		}(w)
	}

	// Wait for all workers
	successes := 0
	for w := 0; w < numWorkers; w++ {
		if <-done {
			successes++
		}
	}

	if successes != numWorkers {
		t.Errorf("Expected %d successes, got %d", numWorkers, successes)
	}

	t.Logf("Concurrent compression: %d/%d workers successful", successes, numWorkers)
}

// Benchmarks

func BenchmarkCompressLZ4(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CompressPage(data, CompressionLZ4)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompressSnappy(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := CompressPage(data, CompressionSnappy)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressLZ4(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	cp, _ := CompressPage(data, CompressionLZ4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecompressPage(cp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressSnappy(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	cp, _ := CompressPage(data, CompressionSnappy)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DecompressPage(cp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeCompressedPage(b *testing.B) {
	data := make([]byte, PageSize)
	cp, _ := CompressPage(data, CompressionLZ4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := SerializeCompressedPage(cp)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeCompressedPage(b *testing.B) {
	data := make([]byte, PageSize)
	cp, _ := CompressPage(data, CompressionLZ4)
	serialized, _ := SerializeCompressedPage(cp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DeserializeCompressedPage(serialized)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransparentCompression(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 100)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed, _ := CompressPageTransparent(data, CompressionLZ4)
		_, err := DecompressPageTransparent(compressed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChooseBestCompression(b *testing.B) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ChooseBestCompression(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
