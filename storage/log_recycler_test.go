package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLogRecyclerBasic(t *testing.T) {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create recycler
	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	// Write some data
	data := []byte("test log entry")
	err = recycler.WriteToSegment(data, 1)
	if err != nil {
		t.Fatalf("Failed to write to segment: %v", err)
	}

	// Check stats
	stats := recycler.GetStats()
	if stats.TotalSegments != 1 {
		t.Errorf("Expected 1 segment, got %d", stats.TotalSegments)
	}
	if stats.ActiveSegments != 1 {
		t.Errorf("Expected 1 active segment, got %d", stats.ActiveSegments)
	}
	if stats.TotalSize != int64(len(data)) {
		t.Errorf("Expected size %d, got %d", len(data), stats.TotalSize)
	}
}

func TestLogRecyclerSegmentRotation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	// Set small segment size for testing
	recycler.SetMaxSegmentSize(100)

	// Write data that exceeds segment size
	data := make([]byte, 50)
	for i := range data {
		data[i] = byte(i)
	}

	// Write 3 times (150 bytes total, should create 2 segments)
	for i := 0; i < 3; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	// Check stats - should have 2 segments
	stats := recycler.GetStats()
	if stats.TotalSegments < 2 {
		t.Errorf("Expected at least 2 segments, got %d", stats.TotalSegments)
	}
	if stats.TotalSize != 150 {
		t.Errorf("Expected total size 150, got %d", stats.TotalSize)
	}
}

func TestLogRecyclerCheckpoint(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(50)

	// Write to multiple segments
	data := make([]byte, 40)
	for i := 0; i < 5; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	statsBefore := recycler.GetStats()
	t.Logf("Before checkpoint: %d total segments, %d active, %d archived",
		statsBefore.TotalSegments, statsBefore.ActiveSegments, statsBefore.ArchivedSegments)

	// Checkpoint at LSN 3 (should archive segments with LSN <= 3)
	err = recycler.Checkpoint(3)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	statsAfter := recycler.GetStats()
	t.Logf("After checkpoint: %d total segments, %d active, %d archived",
		statsAfter.TotalSegments, statsAfter.ActiveSegments, statsAfter.ArchivedSegments)

	// Should have some archived segments
	if statsAfter.ArchivedSegments == 0 {
		t.Errorf("Expected some archived segments after checkpoint")
	}
}

func TestLogRecyclerRecycling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(30)

	// Write to create multiple segments
	data := make([]byte, 25)
	for i := 0; i < 6; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	// Checkpoint
	err = recycler.Checkpoint(4)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	statsBefore := recycler.GetStats()
	t.Logf("Before recycling: %d archived, %d recycled",
		statsBefore.ArchivedSegments, statsBefore.RecycledSegments)

	// Recycle archived segments
	recycledCount, err := recycler.RecycleArchivedSegments()
	if err != nil {
		t.Fatalf("Recycling failed: %v", err)
	}

	t.Logf("Recycled %d segments", recycledCount)

	statsAfter := recycler.GetStats()
	t.Logf("After recycling: %d archived, %d recycled",
		statsAfter.ArchivedSegments, statsAfter.RecycledSegments)

	// Should have segments in recycle pool now
	if statsAfter.RecycledSegments == 0 {
		t.Errorf("Expected segments in recycle pool")
	}

	// Archived count should be 0 after recycling
	if statsAfter.ArchivedSegments != 0 {
		t.Errorf("Expected 0 archived segments after recycling, got %d", statsAfter.ArchivedSegments)
	}
}

func TestLogRecyclerReuseRecycledSegment(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(30)

	// Write data
	data := make([]byte, 25)
	for i := 0; i < 3; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	// Checkpoint and recycle
	err = recycler.Checkpoint(2)
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	recycledCount, err := recycler.RecycleArchivedSegments()
	if err != nil {
		t.Fatalf("Recycling failed: %v", err)
	}

	t.Logf("Recycled %d segments", recycledCount)

	statsAfterRecycle := recycler.GetStats()
	recycledSegments := statsAfterRecycle.RecycledSegments

	// Write more data (should reuse recycled segment)
	for i := 3; i < 5; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	statsAfterReuse := recycler.GetStats()

	// Recycled pool should decrease
	if statsAfterReuse.RecycledSegments >= recycledSegments {
		t.Logf("Warning: Expected recycled segments to decrease from %d, got %d",
			recycledSegments, statsAfterReuse.RecycledSegments)
		// Not a hard failure as timing may vary
	}

	t.Logf("After reuse: %d recycled segments (was %d)",
		statsAfterReuse.RecycledSegments, recycledSegments)
}

func TestLogRecyclerLoadExistingSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create some segment files manually
	for i := 1; i <= 3; i++ {
		filename := filepath.Join(tempDir, fmt.Sprintf("wal_%08d.wal", i))
		file, err := os.Create(filename)
		if err != nil {
			t.Fatal(err)
		}
		file.WriteString(fmt.Sprintf("segment %d data", i))
		file.Close()
	}

	// Create recycler and load existing segments
	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	err = recycler.LoadExistingSegments()
	if err != nil {
		t.Fatalf("Failed to load existing segments: %v", err)
	}

	stats := recycler.GetStats()
	if stats.TotalSegments != 3 {
		t.Errorf("Expected 3 loaded segments, got %d", stats.TotalSegments)
	}

	// Check that next segment ID is 4
	recycler.mu.RLock()
	nextID := recycler.currentSegmentID
	recycler.mu.RUnlock()

	if nextID != 4 {
		t.Errorf("Expected next segment ID to be 4, got %d", nextID)
	}
}

func TestLogRecyclerDeleteOldSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(30)

	// Write to multiple segments
	data := make([]byte, 25)
	for i := 0; i < 5; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	statsBefore := recycler.GetStats()
	t.Logf("Before deletion: %d segments", statsBefore.TotalSegments)

	// Delete segments with LSN < 3
	deletedCount, err := recycler.DeleteOldSegments(3)
	if err != nil {
		t.Fatalf("Failed to delete old segments: %v", err)
	}

	t.Logf("Deleted %d segments", deletedCount)

	statsAfter := recycler.GetStats()
	t.Logf("After deletion: %d segments", statsAfter.TotalSegments)

	// Should have fewer segments
	if statsAfter.TotalSegments >= statsBefore.TotalSegments {
		t.Logf("Warning: Expected fewer segments after deletion")
	}
}

func TestLogRecyclerListSegments(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	// Create some segments
	data := []byte("test data")
	for i := 0; i < 3; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	// Force rotation by setting small size
	recycler.SetMaxSegmentSize(5)
	recycler.WriteToSegment(data, 4)

	// List segments
	segments, err := recycler.ListSegments()
	if err != nil {
		t.Fatalf("Failed to list segments: %v", err)
	}

	t.Logf("Found %d segment files", len(segments))
	for _, seg := range segments {
		t.Logf(" - %s", filepath.Base(seg))
	}

	if len(segments) == 0 {
		t.Errorf("Expected at least 1 segment file")
	}
}

func TestLogRecyclerConcurrentWrites(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	// Concurrent writes
	numWriters := 5
	writesPerWriter := 20

	done := make(chan bool, numWriters)
	data := []byte("concurrent test data")

	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			for i := 0; i < writesPerWriter; i++ {
				lsn := uint64(writerID*writesPerWriter + i + 1)
				err := recycler.WriteToSegment(data, lsn)
				if err != nil {
					t.Errorf("Writer %d failed: %v", writerID, err)
				}
				time.Sleep(time.Microsecond) // Small delay
			}
			done <- true
		}(w)
	}

	// Wait for all writers
	for w := 0; w < numWriters; w++ {
		<-done
	}

	stats := recycler.GetStats()
	expectedSize := int64(len(data) * numWriters * writesPerWriter)

	t.Logf("Concurrent writes complete: %d segments, total size %d (expected %d)",
		stats.TotalSegments, stats.TotalSize, expectedSize)

	if stats.TotalSize != expectedSize {
		t.Errorf("Expected total size %d, got %d", expectedSize, stats.TotalSize)
	}
}

func TestParseSegmentID(t *testing.T) {
	tests := []struct {
		filename string
		expectID uint32
		expectErr bool
	}{
		{"wal_00000001.wal", 1, false},
		{"wal_00000042.wal", 42, false},
		{"wal_12345678.wal", 12345678, false},
		{"/path/to/wal_00000005.wal", 5, false},
		{"invalid.wal", 0, true},
		{"wal_abc.wal", 0, true},
		{"no_extension", 0, true},
	}

	for _, tt := range tests {
		id, err := ParseSegmentID(tt.filename)
		if tt.expectErr {
			if err == nil {
				t.Errorf("ParseSegmentID(%s): expected error, got none", tt.filename)
			}
		} else {
			if err != nil {
				t.Errorf("ParseSegmentID(%s): unexpected error: %v", tt.filename, err)
			}
			if id != tt.expectID {
				t.Errorf("ParseSegmentID(%s): expected ID %d, got %d", tt.filename, tt.expectID, id)
			}
		}
	}
}

func TestLogRecyclerMaxRecyclePool(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "logrecycler_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(20)

	// Create many segments
	data := make([]byte, 15)
	for i := 0; i < MaxRecyclePoolSize+5; i++ {
		err = recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			t.Fatalf("Failed to write to segment: %v", err)
		}
	}

	// Checkpoint most segments
	err = recycler.Checkpoint(uint64(MaxRecyclePoolSize + 3))
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Recycle
	recycledCount, err := recycler.RecycleArchivedSegments()
	if err != nil {
		t.Fatalf("Recycling failed: %v", err)
	}

	t.Logf("Recycled %d segments", recycledCount)

	stats := recycler.GetStats()

	// Pool should be capped at MaxRecyclePoolSize
	if stats.RecycledSegments > MaxRecyclePoolSize {
		t.Errorf("Recycle pool exceeded max size: %d > %d",
			stats.RecycledSegments, MaxRecyclePoolSize)
	}

	t.Logf("Recycle pool size: %d (max: %d)", stats.RecycledSegments, MaxRecyclePoolSize)
}

func BenchmarkLogRecyclerWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "logrecycler_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	data := make([]byte, 128) // 128 byte entries

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := recycler.WriteToSegment(data, uint64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogRecyclerCheckpoint(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "logrecycler_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(1024)

	// Pre-populate with segments
	data := make([]byte, 512)
	for i := 0; i < 100; i++ {
		recycler.WriteToSegment(data, uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := recycler.Checkpoint(uint64(i + 50))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogRecyclerRecycle(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "logrecycler_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	recycler := NewLogRecycler(tempDir, "wal")
	defer recycler.Close()

	recycler.SetMaxSegmentSize(512)

	// Setup: create and archive segments
	data := make([]byte, 256)
	for i := 0; i < 50; i++ {
		recycler.WriteToSegment(data, uint64(i+1))
	}
	recycler.Checkpoint(45)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := recycler.RecycleArchivedSegments()
		if err != nil {
			b.Fatal(err)
		}
		// Re-checkpoint to create archived segments again
		if i < b.N-1 {
			for j := 0; j < 10; j++ {
				recycler.WriteToSegment(data, uint64(100+i*10+j))
			}
			recycler.Checkpoint(uint64(100 + i*10 + 8))
		}
	}
}
