package storage

import (
	"fmt"
	"os"
	"sync"
)

type DiskManager struct {
	file       *os.File
	nextPageId uint32
	mutex      sync.Mutex
}

// NewDiskManager creates a new disk manager that manages pages in a file
func NewDiskManager(fileName string) (*DiskManager, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create file %s: %w", fileName, err)
	}

	return &DiskManager{
		file:       file,
		nextPageId: 0,
		mutex:      sync.Mutex{},
	}, nil
}

// AllocatePage allocates a new page and returns its page ID
func (dm *DiskManager) AllocatePage() uint32 {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	pageId := dm.nextPageId
	dm.nextPageId++
	return pageId
}

// ReadPage reads a page from disk given its page ID
func (dm *DiskManager) ReadPage(pageId uint32) ([]byte, error) {
	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	offset := int64(pageId) * PageSize
	data := make([]byte, PageSize)

	_, err := dm.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageId, err)
	}

	return data, nil
}

// WritePage writes a page to disk at the specified page ID
func (dm *DiskManager) WritePage(pageId uint32, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(data))
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	offset := int64(pageId) * PageSize
	_, err := dm.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageId, err)
	}

	return dm.file.Sync() // Ensure data is written to disk
}

// PageWrite represents a single page write operation
type PageWrite struct {
	PageID uint32
	Data   []byte
}

// WritePagesV writes multiple pages in a single batch operation.
// More efficient than writing pages one-at-a-time.
func (dm *DiskManager) WritePagesV(writes []PageWrite) error {
	if len(writes) == 0 {
		return nil
	}

	dm.mutex.Lock()
	defer dm.mutex.Unlock()

	// Write all pages
	for _, pw := range writes {
		if len(pw.Data) != PageSize {
			return fmt.Errorf("page data must be exactly %d bytes, got %d", PageSize, len(pw.Data))
		}

		offset := int64(pw.PageID) * PageSize
		_, err := dm.file.WriteAt(pw.Data, offset)
		if err != nil {
			return fmt.Errorf("failed to write page %d: %w", pw.PageID, err)
		}
	}

	// Single fsync for all pages (amortize cost)
	return dm.file.Sync()
}

// Close closes the disk manager and its underlying file
func (dm *DiskManager) Close() error {
	if dm.file != nil {
		return dm.file.Close()
	}
	return nil
}
