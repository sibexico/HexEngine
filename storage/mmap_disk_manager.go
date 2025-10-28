package storage

import (
	"fmt"
	"os"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// MmapDiskManager provides zero-copy disk access using memory-mapped files
type MmapDiskManager struct {
	file          *os.File
	fileHandle    windows.Handle
	mappingHandle windows.Handle
	mmapData      []byte
	fileSize      int64
	nextPageId    uint32
	mutex         sync.RWMutex
	growMutex     sync.Mutex // Separate mutex for file growth operations
}

const (
	// Initial file size: 1GB (256K pages * 4KB)
	InitialFileSize = 1024 * 1024 * 1024
	// Grow by 256MB when we run out of space
	FileGrowSize = 256 * 1024 * 1024
)

// NewMmapDiskManager creates a new memory-mapped disk manager
func NewMmapDiskManager(fileName string) (*MmapDiskManager, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create file %s: %w", fileName, err)
	}

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := fileInfo.Size()

	// If file is new or too small, grow it to initial size
	if fileSize < InitialFileSize {
		err = file.Truncate(InitialFileSize)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to grow file: %w", err)
		}
		fileSize = InitialFileSize
	}

	dm := &MmapDiskManager{
		file:       file,
		fileSize:   fileSize,
		nextPageId: 0,
	}

	// Create memory mapping
	err = dm.createMapping()
	if err != nil {
		file.Close()
		return nil, err
	}

	// Calculate next page ID based on file size
	dm.nextPageId = uint32(fileSize / PageSize)

	return dm, nil
}

// createMapping creates or recreates the memory mapping
func (dm *MmapDiskManager) createMapping() error {
	// Convert os.File to Windows handle
	dm.fileHandle = windows.Handle(dm.file.Fd())

	// Create file mapping object
	maxSizeHigh := uint32(dm.fileSize >> 32)
	maxSizeLow := uint32(dm.fileSize & 0xFFFFFFFF)

	mappingHandle, err := windows.CreateFileMapping(
		dm.fileHandle,
		nil,
		windows.PAGE_READWRITE,
		maxSizeHigh,
		maxSizeLow,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to create file mapping: %w", err)
	}
	dm.mappingHandle = mappingHandle

	// Map view of file
	addr, err := windows.MapViewOfFile(
		mappingHandle,
		windows.FILE_MAP_READ|windows.FILE_MAP_WRITE,
		0, // offset high
		0, // offset low
		uintptr(dm.fileSize),
	)
	if err != nil {
		windows.CloseHandle(mappingHandle)
		return fmt.Errorf("failed to map view of file: %w", err)
	}

	// Create byte slice backed by mmap memory
	// Use intermediate uintptr conversion to satisfy unsafe pointer rules
	dm.mmapData = unsafe.Slice((*byte)(unsafe.Pointer(uintptr(addr))), dm.fileSize)

	return nil
}

// AllocatePage allocates a new page and returns its page ID
func (dm *MmapDiskManager) AllocatePage() (uint32, error) {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	pageId := dm.nextPageId

	// Check if we need to grow the file
	requiredSize := int64(pageId+1) * PageSize
	if requiredSize > dm.fileSize {
		dm.mutex.Unlock()
		err := dm.growFile()
		dm.mutex.Lock()
		if err != nil {
			return 0, err
		}
	}

	dm.nextPageId++
	return pageId, nil
}

// growFile expands the file and recreates the mapping
func (dm *MmapDiskManager) growFile() error {
	dm.growMutex.Lock()
	defer dm.growMutex.Unlock()

	// Unmap current mapping
	if dm.mmapData != nil {
		err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&dm.mmapData[0])))
		if err != nil {
			return fmt.Errorf("failed to unmap view: %w", err)
		}
		dm.mmapData = nil
	}

	if dm.mappingHandle != 0 {
		windows.CloseHandle(dm.mappingHandle)
		dm.mappingHandle = 0
	}

	// Grow file
	newSize := dm.fileSize + FileGrowSize
	err := dm.file.Truncate(newSize)
	if err != nil {
		// Try to recreate old mapping
		dm.createMapping()
		return fmt.Errorf("failed to grow file: %w", err)
	}

	dm.fileSize = newSize

	// Recreate mapping
	err = dm.createMapping()
	if err != nil {
		return err
	}

	return nil
}

// ReadPage reads a page from the memory-mapped region (zero-copy)
func (dm *MmapDiskManager) ReadPage(pageId uint32) ([]byte, error) {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	offset := int64(pageId) * PageSize

	if offset+PageSize > dm.fileSize {
		return nil, fmt.Errorf("page %d out of bounds (file size: %d)", pageId, dm.fileSize)
	}

	// Zero-copy read: return a slice of the mmap region
	// Caller should copy if they need to modify
	return dm.mmapData[offset : offset+PageSize], nil
}

// ReadPageCopy reads a page and returns a copy (safe for modification)
func (dm *MmapDiskManager) ReadPageCopy(pageId uint32) ([]byte, error) {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	offset := int64(pageId) * PageSize

	if offset+PageSize > dm.fileSize {
		return nil, fmt.Errorf("page %d out of bounds (file size: %d)", pageId, dm.fileSize)
	}

	// Create a copy for safe modification
	data := make([]byte, PageSize)
	copy(data, dm.mmapData[offset:offset+PageSize])
	return data, nil
}

// WritePage writes a page to the memory-mapped region
func (dm *MmapDiskManager) WritePage(pageId uint32, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(data))
	}

	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	offset := int64(pageId) * PageSize

	if offset+PageSize > dm.fileSize {
		return fmt.Errorf("page %d out of bounds (file size: %d)", pageId, dm.fileSize)
	}

	// Copy data to mmap region
	copy(dm.mmapData[offset:offset+PageSize], data)

	return nil
}

// WritePagesV writes multiple pages in a single batch operation
func (dm *MmapDiskManager) WritePagesV(writes []PageWrite) error {
	if len(writes) == 0 {
		return nil
	}

	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	// Write all pages to mmap region
	for _, pw := range writes {
		if len(pw.Data) != PageSize {
			return fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(pw.Data))
		}

		offset := int64(pw.PageID) * PageSize

		if offset+PageSize > dm.fileSize {
			return fmt.Errorf("page %d out of bounds (file size: %d)", pw.PageID, dm.fileSize)
		}

		copy(dm.mmapData[offset:offset+PageSize], pw.Data)
	}

	return nil
}

// Flush ensures all dirty pages are written to disk
func (dm *MmapDiskManager) Flush() error {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	if dm.mmapData == nil {
		return nil
	}

	// FlushViewOfFile to ensure data is written to disk
	err := windows.FlushViewOfFile(
		uintptr(unsafe.Pointer(&dm.mmapData[0])),
		uintptr(len(dm.mmapData)),
	)
	if err != nil {
		return fmt.Errorf("failed to flush view: %w", err)
	}

	// Also fsync the file descriptor
	return dm.file.Sync()
}

// FlushPage flushes a specific page to disk
func (dm *MmapDiskManager) FlushPage(pageId uint32) error {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	offset := int64(pageId) * PageSize

	if offset+PageSize > dm.fileSize {
		return fmt.Errorf("page %d out of bounds (file size: %d)", pageId, dm.fileSize)
	}

	// Flush only this page
	err := windows.FlushViewOfFile(
		uintptr(unsafe.Pointer(&dm.mmapData[offset])),
		PageSize,
	)
	if err != nil {
		return fmt.Errorf("failed to flush page %d: %w", pageId, err)
	}

	return nil
}

// FlushPages flushes multiple pages to disk
func (dm *MmapDiskManager) FlushPages(pageIds []uint32) error {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	for _, pageId := range pageIds {
		offset := int64(pageId) * PageSize

		if offset+PageSize > dm.fileSize {
			return fmt.Errorf("page %d out of bounds (file size: %d)", pageId, dm.fileSize)
		}

		err := windows.FlushViewOfFile(
			uintptr(unsafe.Pointer(&dm.mmapData[offset])),
			PageSize,
		)
		if err != nil {
			return fmt.Errorf("failed to flush page %d: %w", pageId, err)
		}
	}

	return nil
}

// Advise provides hints to the OS about memory access patterns
func (dm *MmapDiskManager) Advise(pageId uint32, advice AdviceType) error {
	// Windows doesn't have direct equivalent to madvise
	// PrefetchVirtualMemory is available on Windows 8+
	if advice == AdviceWillNeed {
		dm.mutex.RLock()
		offset := int64(pageId) * PageSize
		if offset+PageSize <= dm.fileSize {
			// Prefetch by touching the page
			_ = dm.mmapData[offset]
		}
		dm.mutex.RUnlock()
	}
	return nil
}

// AdviceType represents memory access advice
type AdviceType int

const (
	AdviceNormal     AdviceType = 0 // No special treatment
	AdviceRandom     AdviceType = 1 // Random access pattern
	AdviceSequential AdviceType = 2 // Sequential access pattern
	AdviceWillNeed   AdviceType = 3 // Will need these pages soon (prefetch)
	AdviceDontNeed   AdviceType = 4 // Won't need these pages (can evict)
)

// GetFileSize returns the current file size
func (dm *MmapDiskManager) GetFileSize() int64 {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()
	return dm.fileSize
}

// GetNextPageId returns the next page ID that will be allocated
func (dm *MmapDiskManager) GetNextPageId() uint32 {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()
	return dm.nextPageId
}

// Close unmaps memory and closes the file
func (dm *MmapDiskManager) Close() error {
	// Flush before closing
	dm.Flush()

	// Unmap memory
	if dm.mmapData != nil {
		err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&dm.mmapData[0])))
		if err != nil {
			return fmt.Errorf("failed to unmap view: %w", err)
		}
		dm.mmapData = nil
	}

	// Close mapping handle
	if dm.mappingHandle != 0 {
		err := windows.CloseHandle(dm.mappingHandle)
		if err != nil {
			return fmt.Errorf("failed to close mapping handle: %w", err)
		}
		dm.mappingHandle = 0
	}

	// Close file
	if dm.file != nil {
		return dm.file.Close()
	}

	return nil
}

// Stats returns statistics about the mmap disk manager
type MmapStats struct {
	FileSize    int64
	MappedSize  int64
	NextPageId  uint32
	UsedPages   uint32
	AllocatedMB int64
	UsedMB      int64
}

func (dm *MmapDiskManager) GetStats() MmapStats {
	dm.mutex.RLock()
	defer dm.mutex.RUnlock()

	return MmapStats{
		FileSize:    dm.fileSize,
		MappedSize:  int64(len(dm.mmapData)),
		NextPageId:  dm.nextPageId,
		UsedPages:   dm.nextPageId,
		AllocatedMB: dm.fileSize / (1024 * 1024),
		UsedMB:      int64(dm.nextPageId) * PageSize / (1024 * 1024),
	}
}
