package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// PageHeader contains metadata about the page
type PageHeader struct {
	SlotCount      uint16 // Number of slots in the directory (including deleted)
	TupleCount     uint16 // Number of active (non-deleted) tuples
	SlotDirEnd     uint16 // End of slot directory (grows right)
	TupleDataStart uint16 // Start of tuple data area (grows left from end)
}

// SlotEntry represents an entry in the slot directory
type SlotEntry struct {
	Offset uint16 // Offset of the tuple in the page
	Length uint16 // Length of the tuple
}

// Tuple represents a tuple with header and data
// Includes MVCC fields for snapshot isolation
type Tuple struct {
	data []byte
	xmin uint64 // Transaction ID that created this tuple
	xmax uint64 // Transaction ID that deleted this tuple (0 = not deleted)
}

// NewTuple creates a new tuple with the given data
func NewTuple(data []byte) *Tuple {
	return &Tuple{
		data: data,
		xmin: 0,
		xmax: 0,
	}
}

// NewTupleWithMVCC creates a new tuple with MVCC transaction ID
func NewTupleWithMVCC(data []byte, xmin uint64) *Tuple {
	return &Tuple{
		data: data,
		xmin: xmin,
		xmax: 0,
	}
}

// GetData returns the tuple's data
func (t *Tuple) GetData() []byte {
	return t.data
}

// GetSize returns the size of the tuple
func (t *Tuple) GetSize() uint16 {
	return uint16(len(t.data))
}

// GetXmin returns the creating transaction ID
func (t *Tuple) GetXmin() uint64 {
	return t.xmin
}

// GetXmax returns the deleting transaction ID (0 if not deleted)
func (t *Tuple) GetXmax() uint64 {
	return t.xmax
}

// SetXmax marks the tuple as deleted by the given transaction
func (t *Tuple) SetXmax(xmax uint64) {
	t.xmax = xmax
}

// IsDeleted returns true if the tuple has been marked as deleted
func (t *Tuple) IsDeleted() bool {
	return t.xmax != 0
}

// SlottedPage represents a page with slotted page layout
type SlottedPage struct {
	mu     sync.RWMutex // Protects concurrent access to page data
	data   [PageSize]byte
	header *PageHeader
	slots  []SlotEntry
}

const (
	PageHeaderSize = 8 // 4 * uint16 = 8 bytes
	SlotEntrySize  = 4 // 2 * uint16 = 4 bytes
)

// NewSlottedPage creates a new slotted page
func NewSlottedPage() *SlottedPage {
	page := &SlottedPage{
		header: &PageHeader{
			SlotCount:      0,
			TupleCount:     0,
			SlotDirEnd:     PageHeaderSize,
			TupleDataStart: PageSize,
		},
		slots: make([]SlotEntry, 0),
	}

	// Initialize header in the page data
	page.serializeHeader()
	return page
}

// serializeHeader writes the header to the page data
func (sp *SlottedPage) serializeHeader() {
	binary.LittleEndian.PutUint16(sp.data[0:2], sp.header.SlotCount)
	binary.LittleEndian.PutUint16(sp.data[2:4], sp.header.TupleCount)
	binary.LittleEndian.PutUint16(sp.data[4:6], sp.header.SlotDirEnd)
	binary.LittleEndian.PutUint16(sp.data[6:8], sp.header.TupleDataStart)
}

// deserializeHeader reads the header from the page data
func (sp *SlottedPage) deserializeHeader() {
	sp.header.SlotCount = binary.LittleEndian.Uint16(sp.data[0:2])
	sp.header.TupleCount = binary.LittleEndian.Uint16(sp.data[2:4])
	sp.header.SlotDirEnd = binary.LittleEndian.Uint16(sp.data[4:6])
	sp.header.TupleDataStart = binary.LittleEndian.Uint16(sp.data[6:8])
}

// GetTupleCount returns the number of tuples in the page
func (sp *SlottedPage) GetTupleCount() uint16 {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.header.TupleCount
}

// GetFreeSpaceSize returns the amount of free space in the page
func (sp *SlottedPage) GetFreeSpaceSize() uint16 {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.getFreeSpaceSizeUnsafe()
}

// getFreeSpaceSizeUnsafe returns free space without locking (for internal use)
func (sp *SlottedPage) getFreeSpaceSizeUnsafe() uint16 {
	// Free space is between slot directory end and tuple data start
	if sp.header.TupleDataStart <= sp.header.SlotDirEnd {
		return 0
	}
	return sp.header.TupleDataStart - sp.header.SlotDirEnd
}

// InsertTuple inserts a new tuple into the page and returns its slot ID
func (sp *SlottedPage) InsertTuple(tuple *Tuple) (uint16, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	tupleSize := tuple.GetSize()
	requiredSpace := tupleSize + SlotEntrySize

	// Check if there's enough space
	freeSpace := sp.getFreeSpaceSizeUnsafe()
	if freeSpace < requiredSpace {
		return 0, fmt.Errorf("not enough space in page: need %d, have %d", requiredSpace, freeSpace)
	}

	// Find an available slot (look for deleted slots first)
	slotId := uint16(len(sp.slots))
	for i, slot := range sp.slots {
		if slot.Length == 0 { // Deleted slot
			slotId = uint16(i)
			break
		}
	}

	// Calculate tuple offset (tuples grow from end of page backwards)
	tupleOffset := sp.header.TupleDataStart - tupleSize

	// Copy tuple data to page
	copy(sp.data[tupleOffset:tupleOffset+tupleSize], tuple.data)

	// Create/update slot entry
	slotEntry := SlotEntry{
		Offset: tupleOffset,
		Length: tupleSize,
	}

	if slotId < uint16(len(sp.slots)) {
		sp.slots[slotId] = slotEntry
	} else {
		sp.slots = append(sp.slots, slotEntry)
	}

	// Update header
	if slotId == uint16(len(sp.slots))-1 { // New slot
		sp.header.SlotCount++
		sp.header.TupleCount++
		sp.header.SlotDirEnd += SlotEntrySize
	} else {
		sp.header.TupleCount++ // Reusing deleted slot
	}
	sp.header.TupleDataStart = tupleOffset

	// Serialize slot directory and header
	sp.serializeSlots()
	sp.serializeHeader()

	return slotId, nil
}

// serializeSlots writes the slot directory to the page data
func (sp *SlottedPage) serializeSlots() {
	offset := PageHeaderSize
	for _, slot := range sp.slots {
		binary.LittleEndian.PutUint16(sp.data[offset:offset+2], slot.Offset)
		binary.LittleEndian.PutUint16(sp.data[offset+2:offset+4], slot.Length)
		offset += SlotEntrySize
	}
}

// ReadTuple reads a tuple from the page given its slot ID
func (sp *SlottedPage) ReadTuple(slotId uint16) (*Tuple, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if slotId >= uint16(len(sp.slots)) {
		return nil, fmt.Errorf("invalid slot ID %d", slotId)
	}

	slot := sp.slots[slotId]
	if slot.Length == 0 {
		return nil, fmt.Errorf("slot %d is empty (tuple deleted)", slotId)
	}

	// Copy data to avoid data race on returned slice
	tupleData := make([]byte, slot.Length)
	copy(tupleData, sp.data[slot.Offset:slot.Offset+slot.Length])

	return NewTuple(tupleData), nil
}

// DeleteTuple marks a tuple as deleted
func (sp *SlottedPage) DeleteTuple(slotId uint16) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if slotId >= uint16(len(sp.slots)) {
		return fmt.Errorf("invalid slot ID %d", slotId)
	}

	slot := sp.slots[slotId]
	if slot.Length == 0 {
		return fmt.Errorf("slot %d is already empty", slotId)
	}

	// Mark slot as deleted by setting length to 0
	sp.slots[slotId].Length = 0
	sp.slots[slotId].Offset = 0

	// Update header
	sp.header.TupleCount--
	// We don't reclaim the tuple data space immediately (requires compaction)

	// Serialize changes
	sp.serializeSlots()
	sp.serializeHeader()

	return nil
}

// UpdateTuple updates an existing tuple
func (sp *SlottedPage) UpdateTuple(slotId uint16, newTuple *Tuple) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if slotId >= uint16(len(sp.slots)) {
		return fmt.Errorf("invalid slot ID %d", slotId)
	}

	slot := sp.slots[slotId]
	if slot.Length == 0 {
		return fmt.Errorf("slot %d is empty (tuple deleted)", slotId)
	}

	newSize := newTuple.GetSize()
	oldSize := slot.Length

	// Check if we have enough space for the larger tuple
	if newSize > oldSize {
		additionalSpace := newSize - oldSize
		freeSpace := sp.getFreeSpaceSizeUnsafe()
		if freeSpace < additionalSpace {
			return fmt.Errorf("not enough space to update tuple: need %d more bytes, have %d",
				additionalSpace, freeSpace)
		}
	}

	// Calculate new tuple offset (place at current tuple data start)
	tupleOffset := sp.header.TupleDataStart - newSize

	// Copy new tuple data
	copy(sp.data[tupleOffset:tupleOffset+newSize], newTuple.data)

	// Update slot
	sp.slots[slotId] = SlotEntry{
		Offset: tupleOffset,
		Length: newSize,
	}

	// Update tuple data start
	sp.header.TupleDataStart = tupleOffset

	// Serialize changes
	sp.serializeSlots()
	sp.serializeHeader()

	return nil
}

// Serialize converts the SlottedPage to a byte array
func (sp *SlottedPage) Serialize() []byte {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	// The page data already contains everything we need
	// Header is at the beginning, slots follow, then tuple data
	result := make([]byte, PageSize)
	copy(result, sp.data[:])
	return result
}

// DeserializeSlottedPage reconstructs a SlottedPage from bytes
func DeserializeSlottedPage(data []byte) (*SlottedPage, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("invalid page data size: expected %d, got %d", PageSize, len(data))
	}

	page := &SlottedPage{
		header: &PageHeader{},
		slots:  make([]SlotEntry, 0),
	}

	// Copy data
	copy(page.data[:], data)

	// Deserialize header
	page.deserializeHeader()

	// Deserialize slot directory - use SlotCount, not TupleCount
	slotCount := page.header.SlotCount
	slotsOffset := uint16(PageHeaderSize)

	for i := uint16(0); i < slotCount; i++ {
		offset := slotsOffset + (i * SlotEntrySize)
		if offset+SlotEntrySize > PageSize {
			return nil, fmt.Errorf("invalid slot directory: offset %d exceeds page size", offset)
		}

		slot := SlotEntry{
			Offset: binary.LittleEndian.Uint16(page.data[offset : offset+2]),
			Length: binary.LittleEndian.Uint16(page.data[offset+2 : offset+4]),
		}
		page.slots = append(page.slots, slot)
	}

	return page, nil
}
