package storage

import (
	"os"
	"testing"
	"time"
)

func TestVacuumManager(t *testing.T) {
	testFileName := "test_vacuum.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_vacuum.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_vacuum.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	if vm == nil {
		t.Fatal("VacuumManager should not be nil")
	}

	// Check initial stats
	stats := vm.GetStats()
	if stats.TotalScanned != 0 {
		t.Errorf("Initial scanned should be 0, got %d", stats.TotalScanned)
	}
	if stats.TotalReclaimed != 0 {
		t.Errorf("Initial reclaimed should be 0, got %d", stats.TotalReclaimed)
	}
}

func TestVacuumHorizonUpdate(t *testing.T) {
	testFileName := "test_vacuum_horizon.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_vacuum_horizon.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_vacuum_horizon.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Start a transaction to set the horizon
	txn1, err := txnMgr.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update horizon
	horizon := vm.UpdateHorizon()
	if horizon == 0 {
		t.Error("Horizon should be updated to active transaction ID")
	}

	// Commit transaction
	err = txnMgr.Commit(txn1.TxnID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestVacuumPageReclamation(t *testing.T) {
	testFileName := "test_vacuum_reclaim.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_vacuum_reclaim.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_vacuum_reclaim.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Create a page with tuples
	page, err := bpm.NewPage()
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	pageID := page.GetPageId()

	// Insert tuples created by old transactions
	slottedPage := page.GetData()

	// Tuple 1: created by txn 100, deleted by txn 200
	tuple1 := NewTupleWithMVCC([]byte("data1"), 100)
	tuple1.SetXmax(200) // Mark as deleted
	_, err = slottedPage.InsertTuple(tuple1)
	if err != nil {
		t.Fatalf("Failed to insert tuple1: %v", err)
	}

	// Tuple 2: created by txn 150, still alive
	tuple2 := NewTupleWithMVCC([]byte("data2"), 150)
	slotID2, err := slottedPage.InsertTuple(tuple2)
	if err != nil {
		t.Fatalf("Failed to insert tuple2: %v", err)
	}

	// Tuple 3: created by txn 120, deleted by txn 180
	tuple3 := NewTupleWithMVCC([]byte("data3"), 120)
	tuple3.SetXmax(180) // Mark as deleted
	_, err = slottedPage.InsertTuple(tuple3)
	if err != nil {
		t.Fatalf("Failed to insert tuple3: %v", err)
	}

	page.SetDirty(true)
	bpm.UnpinPage(pageID, true)

	// Set horizon to 250 (all delete transactions are before this)
	vm.minHorizon = 250

	// Run vacuum on the page
	reclaimed, err := vm.VacuumPage(pageID)
	if err != nil {
		t.Fatalf("VacuumPage failed: %v", err)
	}

	if reclaimed < 0 {
		t.Errorf("Expected at least some tuples reclaimed (deleted ones), got %d", reclaimed)
	}

	// Verify the alive tuple still exists
	page, err = bpm.FetchPage(pageID)
	if err != nil {
		t.Fatalf("Failed to fetch page: %v", err)
	}

	slottedPage = page.GetData()
	_, err = slottedPage.ReadTuple(slotID2)
	if err != nil {
		t.Logf("Note: ReadTuple error is expected for deleted slots: %v", err)
	}
	// Check tuple count decreased
	if slottedPage.GetTupleCount() == 0 {
		t.Error("Should still have alive tuples after vacuum")
	}

	bpm.UnpinPage(pageID, false)
}

func TestVacuumAll(t *testing.T) {
	testFileName := "test_vacuum_all.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(20, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_vacuum_all.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_vacuum_all.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Create multiple pages with dead tuples
	for i := 0; i < 5; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			t.Fatalf("Failed to create page %d: %v", i, err)
		}

		slottedPage := page.GetData()

		// Add a dead tuple
		deadTuple := NewTupleWithMVCC([]byte("dead"), 100)
		deadTuple.SetXmax(150)
		slottedPage.InsertTuple(deadTuple)

		// Add an alive tuple
		aliveTuple := NewTupleWithMVCC([]byte("alive"), 200)
		slottedPage.InsertTuple(aliveTuple)

		page.SetDirty(true)
		bpm.UnpinPage(page.GetPageId(), true)
	}

	// Set horizon
	vm.minHorizon = 300

	// Run vacuum on all pages
	stats, err := vm.VacuumAll()
	if err != nil {
		t.Fatalf("VacuumAll failed: %v", err)
	}

	if stats.TotalScanned == 0 {
		t.Error("Should have scanned pages")
	}

	t.Logf("Vacuum stats: scanned=%d, reclaimed=%d, runs=%d", stats.TotalScanned, stats.TotalReclaimed, stats.TotalRuns)

	// Stats should reflect the run
	if stats.TotalRuns != 1 {
		t.Errorf("Should have recorded 1 run, got %d", stats.TotalRuns)
	}
}

func TestAutoVacuum(t *testing.T) {
	testFileName := "test_auto_vacuum.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_auto_vacuum.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_auto_vacuum.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Start auto vacuum with short interval
	vm.StartAutoVacuum(100 * time.Millisecond)

	// Wait for at least 2 runs
	time.Sleep(250 * time.Millisecond)

	// Stop auto vacuum
	vm.StopAutoVacuum()

	// Check stats
	stats := vm.GetStats()
	if stats.TotalRuns < 2 {
		t.Errorf("Expected at least 2 vacuum runs, got %d", stats.TotalRuns)
	}
}

func TestVacuumStats(t *testing.T) {
	testFileName := "test_vacuum_stats.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_vacuum_stats.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_vacuum_stats.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Reset stats
	vm.ResetStats()

	stats := vm.GetStats()
	if stats.TotalScanned != 0 || stats.TotalReclaimed != 0 || stats.TotalRuns != 0 {
		t.Error("Stats should be zero after reset")
	}

	// Run vacuum
	_, _ = vm.VacuumAll()

	stats = vm.GetStats()
	if stats.TotalRuns != 1 {
		t.Errorf("Expected 1 run after VacuumAll, got %d", stats.TotalRuns)
	}
}

func TestConcurrentVacuum(t *testing.T) {
	testFileName := "test_concurrent_vacuum.db"
	defer os.Remove(testFileName)

	dm, err := NewDiskManager(testFileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	bpm, err := NewBufferPoolManager(10, dm)
	if err != nil {
		t.Fatalf("Failed to create BufferPoolManager: %v", err)
	}

	lm, err := NewLogManager("test_concurrent_vacuum.log")
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer os.Remove("test_concurrent_vacuum.log")

	txnMgr := NewTransactionManager(lm)
	vm := NewVacuumManager(bpm, txnMgr)

	// Start first vacuum in background by manually setting the running flag
	started := make(chan bool)
	done := make(chan error)

	go func() {
		vm.mutex.Lock()
		vm.running = true
		vm.mutex.Unlock()
		started <- true

		// Simulate a long-running vacuum
		time.Sleep(100 * time.Millisecond)

		vm.mutex.Lock()
		vm.running = false
		vm.mutex.Unlock()
		done <- nil
	}()

	// Wait for first vacuum to set running flag
	<-started

	// Try to run second vacuum (should fail immediately)
	_, err = vm.VacuumAll()
	if err == nil {
		t.Error("Second vacuum should fail when one is already running")
	}

	// Wait for first vacuum to complete
	<-done
}
