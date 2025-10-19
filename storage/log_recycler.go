package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// LogSegment represents a WAL segment file
type LogSegment struct {
	SegmentID uint32
	FilePath string
	File *os.File
	Size int64
	MinLSN uint64 // Minimum LSN in this segment
	MaxLSN uint64 // Maximum LSN in this segment
	IsActive bool // Currently being written to
	IsArchived bool // Can be recycled after checkpoint
}

// LogRecycler manages WAL segment recycling
type LogRecycler struct {
	baseDir string
	baseFilename string
	maxSegmentSize int64 // Max size per segment (default 64MB)
	currentSegmentID uint32
	currentSegment *LogSegment
	segments map[uint32]*LogSegment // All known segments
	archivedSegments []uint32 // Segments ready for recycling
	checkpointLSN uint64 // LSN of last checkpoint
	recyclePool []string // Recycled segment files ready for reuse
	mu sync.RWMutex
}

const (
	DefaultMaxSegmentSize = 64 * 1024 * 1024 // 64MB
	MaxRecyclePoolSize = 10 // Keep max 10 recycled segments
)

// NewLogRecycler creates a new log recycler
func NewLogRecycler(baseDir string, baseFilename string) *LogRecycler {
	return &LogRecycler{
		baseDir: baseDir,
		baseFilename: baseFilename,
		maxSegmentSize: DefaultMaxSegmentSize,
		currentSegmentID: 1,
		segments: make(map[uint32]*LogSegment),
		archivedSegments: make([]uint32, 0),
		recyclePool: make([]string, 0, MaxRecyclePoolSize),
		checkpointLSN: 0,
	}
}

// GetSegmentFilename generates a segment filename
func (lr *LogRecycler) GetSegmentFilename(segmentID uint32) string {
	return filepath.Join(lr.baseDir, fmt.Sprintf("%s_%08d.wal", lr.baseFilename, segmentID))
}

// GetOrCreateActiveSegment returns the current active segment, creating new one if needed
func (lr *LogRecycler) GetOrCreateActiveSegment() (*LogSegment, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	// If current segment exists and not full, return it
	if lr.currentSegment != nil && lr.currentSegment.Size < lr.maxSegmentSize {
		return lr.currentSegment, nil
	}

	// Need to create new segment
	return lr.createNewSegment()
}

// createNewSegment creates a new log segment (must be called with lock held)
func (lr *LogRecycler) createNewSegment() (*LogSegment, error) {
	// Close current segment if exists
	if lr.currentSegment != nil {
		lr.currentSegment.IsActive = false
		if lr.currentSegment.File != nil {
			lr.currentSegment.File.Close()
		}
	}

	// Try to reuse a recycled segment first
	var filePath string
	if len(lr.recyclePool) > 0 {
		// Pop from recycle pool
		filePath = lr.recyclePool[0]
		lr.recyclePool = lr.recyclePool[1:]

		// Truncate the file to clear old data
		err := os.Truncate(filePath, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to truncate recycled segment: %w", err)
		}
	} else {
		// Create new segment file
		filePath = lr.GetSegmentFilename(lr.currentSegmentID)
	}

	// Open file for writing
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	segment := &LogSegment{
		SegmentID: lr.currentSegmentID,
		FilePath: filePath,
		File: file,
		Size: 0,
		MinLSN: 0,
		MaxLSN: 0,
		IsActive: true,
		IsArchived: false,
	}

	lr.segments[lr.currentSegmentID] = segment
	lr.currentSegment = segment
	lr.currentSegmentID++

	return segment, nil
}

// WriteToSegment writes data to the current active segment
func (lr *LogRecycler) WriteToSegment(data []byte, lsn uint64) error {
	segment, err := lr.GetOrCreateActiveSegment()
	if err != nil {
		return err
	}

	lr.mu.Lock()
	defer lr.mu.Unlock()

	// Write data
	n, err := segment.File.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to segment: %w", err)
	}

	// Update segment metadata
	segment.Size += int64(n)
	if segment.MinLSN == 0 {
		segment.MinLSN = lsn
	}
	segment.MaxLSN = lsn

	return nil
}

// Checkpoint marks a checkpoint LSN, allowing older segments to be archived
func (lr *LogRecycler) Checkpoint(checkpointLSN uint64) error {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	lr.checkpointLSN = checkpointLSN

	// Archive segments that are fully before checkpoint
	for segmentID, segment := range lr.segments {
		if !segment.IsActive && !segment.IsArchived && segment.MaxLSN < checkpointLSN {
			segment.IsArchived = true
			lr.archivedSegments = append(lr.archivedSegments, segmentID)
		}
	}

	return nil
}

// RecycleArchivedSegments moves archived segments to recycle pool
func (lr *LogRecycler) RecycleArchivedSegments() (int, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	recycledCount := 0

	for _, segmentID := range lr.archivedSegments {
		segment := lr.segments[segmentID]
		if segment == nil {
			continue
		}

		// Close file if open
		if segment.File != nil {
			segment.File.Close()
			segment.File = nil
		}

		// Add to recycle pool if not full
		if len(lr.recyclePool) < MaxRecyclePoolSize {
			lr.recyclePool = append(lr.recyclePool, segment.FilePath)
			recycledCount++
		} else {
			// Pool full, delete the file
			os.Remove(segment.FilePath)
			recycledCount++
		}

		// Remove from segments map
		delete(lr.segments, segmentID)
	}

	// Clear archived list
	lr.archivedSegments = lr.archivedSegments[:0]

	return recycledCount, nil
}

// GetSegmentStats returns statistics about segments
type SegmentStats struct {
	TotalSegments int
	ActiveSegments int
	ArchivedSegments int
	RecycledSegments int
	TotalSize int64
	OldestLSN uint64
	NewestLSN uint64
}

func (lr *LogRecycler) GetStats() SegmentStats {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	stats := SegmentStats{
		TotalSegments: len(lr.segments),
		ArchivedSegments: len(lr.archivedSegments),
		RecycledSegments: len(lr.recyclePool),
	}

	var oldestLSN uint64 = ^uint64(0) // Max uint64
	var newestLSN uint64 = 0

	for _, segment := range lr.segments {
		if segment.IsActive {
			stats.ActiveSegments++
		}
		stats.TotalSize += segment.Size

		if segment.MinLSN < oldestLSN && segment.MinLSN > 0 {
			oldestLSN = segment.MinLSN
		}
		if segment.MaxLSN > newestLSN {
			newestLSN = segment.MaxLSN
		}
	}

	if oldestLSN != ^uint64(0) {
		stats.OldestLSN = oldestLSN
	}
	stats.NewestLSN = newestLSN

	return stats
}

// ListSegments returns all segment files in the directory
func (lr *LogRecycler) ListSegments() ([]string, error) {
	pattern := filepath.Join(lr.baseDir, fmt.Sprintf("%s_*.wal", lr.baseFilename))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	sort.Strings(files) // Sort by filename (which includes segment ID)
	return files, nil
}

// LoadExistingSegments scans directory and loads existing segments
func (lr *LogRecycler) LoadExistingSegments() error {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	files, err := lr.ListSegments()
	if err != nil {
		return err
	}

	if len(files) == 0 {
		// No existing segments, start fresh
		return nil
	}

	// Parse segment IDs from filenames
	maxSegmentID := uint32(0)
	for _, filePath := range files {
		filename := filepath.Base(filePath)
		// Extract segment ID from filename (e.g., "wal_00000001.wal" -> 1)
		var segmentID uint32
		_, err := fmt.Sscanf(filename, lr.baseFilename+"_%08d.wal", &segmentID)
		if err != nil {
			continue // Skip malformed filenames
		}

		// Get file info
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			continue
		}

		// Create segment record (don't open file yet)
		segment := &LogSegment{
			SegmentID: segmentID,
			FilePath: filePath,
			File: nil, // Will be opened when needed
			Size: fileInfo.Size(),
			MinLSN: 0, // Would need to scan file to determine
			MaxLSN: 0,
			IsActive: false,
			IsArchived: false,
		}

		lr.segments[segmentID] = segment

		if segmentID > maxSegmentID {
			maxSegmentID = segmentID
		}
	}

	// Set next segment ID
	lr.currentSegmentID = maxSegmentID + 1

	return nil
}

// Close closes all open segments
func (lr *LogRecycler) Close() error {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	for _, segment := range lr.segments {
		if segment.File != nil {
			segment.File.Close()
			segment.File = nil
		}
	}

	return nil
}

// DeleteOldSegments permanently deletes segments older than the given LSN
func (lr *LogRecycler) DeleteOldSegments(beforeLSN uint64) (int, error) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	deletedCount := 0
	segmentsToDelete := make([]uint32, 0)

	for segmentID, segment := range lr.segments {
		if !segment.IsActive && segment.MaxLSN < beforeLSN {
			// Close file if open
			if segment.File != nil {
				segment.File.Close()
				segment.File = nil
			}

			// Delete file
			err := os.Remove(segment.FilePath)
			if err != nil {
				return deletedCount, fmt.Errorf("failed to delete segment %d: %w", segmentID, err)
			}

			segmentsToDelete = append(segmentsToDelete, segmentID)
			deletedCount++
		}
	}

	// Remove from map
	for _, segmentID := range segmentsToDelete {
		delete(lr.segments, segmentID)
	}

	// Also remove from recycle pool if present
	newRecyclePool := make([]string, 0, len(lr.recyclePool))
	for _, path := range lr.recyclePool {
		found := false
		for _, segment := range lr.segments {
			if segment.FilePath == path {
				found = true
				break
			}
		}
		if found {
			newRecyclePool = append(newRecyclePool, path)
		}
	}
	lr.recyclePool = newRecyclePool

	return deletedCount, nil
}

// SetMaxSegmentSize configures the maximum segment size
func (lr *LogRecycler) SetMaxSegmentSize(maxSize int64) {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	lr.maxSegmentSize = maxSize
}

// Helper function to parse segment ID from filename
func ParseSegmentID(filename string) (uint32, error) {
	// Remove directory path
	base := filepath.Base(filename)

	// Remove extension
	name := strings.TrimSuffix(base, ".wal")

	// Extract ID (format: prefix_NNNNNNNN)
	parts := strings.Split(name, "_")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid segment filename format: %s", filename)
	}

	var segmentID uint32
	_, err := fmt.Sscanf(parts[len(parts)-1], "%08d", &segmentID)
	if err != nil {
		return 0, fmt.Errorf("failed to parse segment ID: %w", err)
	}

	return segmentID, nil
}
