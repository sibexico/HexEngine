package storage

import (
	"fmt"
	"sync"
	"time"
)

// VacuumManager handles garbage collection of old MVCC tuple versions
type VacuumManager struct {
	bpm            *BufferPoolManager
	transactionMgr *TransactionManager
	minHorizon     uint64 // Minimum transaction ID that might still need old versions
	mutex          sync.RWMutex
	running        bool
	stopCh         chan struct{}
	wg             sync.WaitGroup

	// Statistics
	totalScanned   uint64
	totalReclaimed uint64
	totalRuns      uint64
	lastRunTime    time.Time
}

// VacuumStats contains statistics about vacuum operations
type VacuumStats struct {
	TotalScanned   uint64
	TotalReclaimed uint64
	TotalRuns      uint64
	LastRunTime    time.Time
	IsRunning      bool
}

// NewVacuumManager creates a new vacuum manager
func NewVacuumManager(bpm *BufferPoolManager, txnMgr *TransactionManager) *VacuumManager {
	return &VacuumManager{
		bpm:            bpm,
		transactionMgr: txnMgr,
		minHorizon:     0,
		running:        false,
		stopCh:         make(chan struct{}),
	}
}

// UpdateHorizon updates the minimum transaction ID horizon
// Tuples with xmax < minHorizon can be safely reclaimed
func (vm *VacuumManager) UpdateHorizon() uint64 {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	// Get the oldest active transaction from all active transactions
	// For now, use a simple approach: assume horizon is the minimum active txn
	// In production, this would scan all active transactions
	vm.transactionMgr.mutex.RLock()
	minActive := uint64(^uint64(0)) // Max uint64
	for txnID := range vm.transactionMgr.activeTxns {
		if txnID < minActive {
			minActive = txnID
		}
	}
	vm.transactionMgr.mutex.RUnlock()

	if minActive != ^uint64(0) {
		vm.minHorizon = minActive
	}

	return vm.minHorizon
}

// VacuumPage performs garbage collection on a single page
// Returns the number of tuples reclaimed
func (vm *VacuumManager) VacuumPage(pageID uint32) (int, error) {
	// Fetch the page
	page, err := vm.bpm.FetchPage(pageID)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch page %d: %w", pageID, err)
	}

	slottedPage := page.GetData()
	reclaimed := 0
	isDirty := false

	// Get current horizon
	vm.mutex.RLock()
	horizon := vm.minHorizon
	vm.mutex.RUnlock()

	// Scan all tuples in the page
	tupleCount := slottedPage.GetTupleCount()
	for slotID := uint16(0); slotID < tupleCount; slotID++ {
		tuple, err := slottedPage.ReadTuple(slotID)
		if err != nil || tuple == nil {
			continue
		}

		// Check if tuple can be reclaimed
		// A tuple can be removed if:
		// 1. It has been deleted (xmax != 0)
		// 2. The deleting transaction is committed and before horizon
		xmax := tuple.GetXmax()
		if xmax != 0 && xmax < horizon {
			// This tuple is dead and can be reclaimed
			err := slottedPage.DeleteTuple(slotID)
			if err == nil {
				reclaimed++
				isDirty = true
			}
		}
	}

	// Set dirty flag if we modified the page
	if isDirty {
		page.SetDirty(true)
	}

	// Unpin the page
	vm.bpm.UnpinPage(pageID, isDirty)

	return reclaimed, nil
}

// VacuumAll performs garbage collection on all pages in the buffer pool
func (vm *VacuumManager) VacuumAll() (*VacuumStats, error) {
	vm.mutex.Lock()
	if vm.running {
		vm.mutex.Unlock()
		return nil, fmt.Errorf("vacuum already running")
	}
	vm.running = true
	vm.mutex.Unlock()

	defer func() {
		vm.mutex.Lock()
		vm.running = false
		vm.totalRuns++
		vm.lastRunTime = time.Now()
		vm.mutex.Unlock()
	}()

	// Update the horizon before starting
	vm.UpdateHorizon()

	scanned := 0
	reclaimed := 0

	// Get all pages from the page table
	pages := vm.bpm.pageTable.GetAll()

	for _, page := range pages {
		pageID := page.GetPageId()

		reclaimedInPage, err := vm.VacuumPage(pageID)
		if err != nil {
			// Log error but continue with other pages
			continue
		}

		scanned++
		reclaimed += reclaimedInPage
	}

	// Update statistics
	vm.mutex.Lock()
	vm.totalScanned += uint64(scanned)
	vm.totalReclaimed += uint64(reclaimed)

	// Capture final stats while holding the lock (after totalRuns incremented by defer)
	finalStats := VacuumStats{
		TotalScanned:   vm.totalScanned,
		TotalReclaimed: vm.totalReclaimed,
		TotalRuns:      vm.totalRuns + 1, // Add 1 because defer hasn't run yet
	}
	vm.mutex.Unlock()

	return &finalStats, nil
}

// StartAutoVacuum starts automatic vacuum in the background
func (vm *VacuumManager) StartAutoVacuum(interval time.Duration) {
	vm.wg.Add(1)
	go func() {
		defer vm.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Run vacuum
				_, _ = vm.VacuumAll()
			case <-vm.stopCh:
				return
			}
		}
	}()
}

// StopAutoVacuum stops the automatic vacuum process
func (vm *VacuumManager) StopAutoVacuum() {
	close(vm.stopCh)
	vm.wg.Wait()
}

// GetStats returns current vacuum statistics
func (vm *VacuumManager) GetStats() VacuumStats {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	return VacuumStats{
		TotalScanned:   vm.totalScanned,
		TotalReclaimed: vm.totalReclaimed,
		TotalRuns:      vm.totalRuns,
		LastRunTime:    vm.lastRunTime,
		IsRunning:      vm.running,
	}
}

// ResetStats resets all vacuum statistics
func (vm *VacuumManager) ResetStats() {
	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	vm.totalScanned = 0
	vm.totalReclaimed = 0
	vm.totalRuns = 0
	vm.lastRunTime = time.Time{}
}
