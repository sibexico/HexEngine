package storage

import (
	"testing"
)

// TestTupleWithMVCC tests tuple creation with MVCC fields
func TestTupleWithMVCC(t *testing.T) {
	data := []byte("test data")
	xmin := uint64(100)

	tuple := NewTupleWithMVCC(data, xmin)

	if tuple == nil {
		t.Fatal("Tuple should not be nil")
	}

	if string(tuple.GetData()) != "test data" {
		t.Errorf("Expected data 'test data', got '%s'", string(tuple.GetData()))
	}

	if tuple.GetXmin() != xmin {
		t.Errorf("Expected xmin %d, got %d", xmin, tuple.GetXmin())
	}

	if tuple.GetXmax() != 0 {
		t.Errorf("Expected xmax 0 (not deleted), got %d", tuple.GetXmax())
	}
}

// TestTupleDelete tests marking a tuple as deleted
func TestTupleDelete(t *testing.T) {
	tuple := NewTupleWithMVCC([]byte("data"), 100)

	// Initially not deleted
	if tuple.IsDeleted() {
		t.Error("New tuple should not be deleted")
	}

	// Mark as deleted by transaction 200
	tuple.SetXmax(200)

	if tuple.GetXmax() != 200 {
		t.Errorf("Expected xmax 200, got %d", tuple.GetXmax())
	}

	if !tuple.IsDeleted() {
		t.Error("Tuple with xmax should be marked as deleted")
	}
}

// TestSnapshotVisibility tests MVCC visibility logic
func TestSnapshotVisibility(t *testing.T) {
	// Tuple created by transaction 100
	tuple := NewTupleWithMVCC([]byte("data"), 100)

	// Snapshot taken at transaction 150
	snapshot := &Snapshot{
		XminSnapshot: 90, // Oldest active transaction
		XmaxSnapshot: 150, // Next transaction ID
		ActiveTxns: []uint64{120, 130}, // Active transactions
	}

	// Test 1: Tuple visible (created before snapshot, not deleted)
	visible := snapshot.IsVisible(tuple, 150)
	if !visible {
		t.Error("Tuple created by committed txn should be visible")
	}

	// Test 2: Tuple created by active transaction (not visible)
	tuple2 := NewTupleWithMVCC([]byte("data2"), 120) // Active in snapshot
	visible = snapshot.IsVisible(tuple2, 150)
	if visible {
		t.Error("Tuple created by active txn should not be visible")
	}

	// Test 3: Tuple created after snapshot (not visible)
	tuple3 := NewTupleWithMVCC([]byte("data3"), 160)
	visible = snapshot.IsVisible(tuple3, 150)
	if visible {
		t.Error("Tuple created after snapshot should not be visible")
	}

	// Test 4: Tuple deleted by committed transaction (not visible)
	tuple4 := NewTupleWithMVCC([]byte("data4"), 100)
	tuple4.SetXmax(110) // Deleted by committed txn
	visible = snapshot.IsVisible(tuple4, 150)
	if visible {
		t.Error("Deleted tuple should not be visible")
	}

	// Test 5: Tuple deleted by active transaction (visible)
	tuple5 := NewTupleWithMVCC([]byte("data5"), 100)
	tuple5.SetXmax(120) // Deleted by active txn
	visible = snapshot.IsVisible(tuple5, 150)
	if !visible {
		t.Error("Tuple deleted by active txn should still be visible")
	}

	// Test 6: Tuple deleted after snapshot (visible)
	tuple6 := NewTupleWithMVCC([]byte("data6"), 100)
	tuple6.SetXmax(160) // Deleted after snapshot
	visible = snapshot.IsVisible(tuple6, 150)
	if !visible {
		t.Error("Tuple deleted after snapshot should be visible")
	}
}

// TestSnapshotVisibilityOwnTransaction tests visibility of own changes
func TestSnapshotVisibilityOwnTransaction(t *testing.T) {
	snapshot := &Snapshot{
		XminSnapshot: 100,
		XmaxSnapshot: 110,
		ActiveTxns: []uint64{105},
	}

	// Tuple created by current transaction (105)
	tuple := NewTupleWithMVCC([]byte("data"), 105)
	visible := snapshot.IsVisible(tuple, 105)
	if !visible {
		t.Error("Transaction should see its own inserts")
	}

	// Tuple deleted by current transaction (105)
	tuple2 := NewTupleWithMVCC([]byte("data2"), 100)
	tuple2.SetXmax(105)
	visible = snapshot.IsVisible(tuple2, 105)
	if visible {
		t.Error("Transaction should not see tuples it deleted")
	}
}

// TestIsVisibleConcurrent tests visibility checks under various scenarios
func TestIsVisibleConcurrent(t *testing.T) {
	tests := []struct {
		name string
		tupleXmin uint64
		tupleXmax uint64
		snapshotXmin uint64
		snapshotXmax uint64
		activeTxns []uint64
		currentTxn uint64
		expected bool
	}{
		{
			name: "Tuple from committed transaction",
			tupleXmin: 10,
			tupleXmax: 0,
			snapshotXmin: 10,
			snapshotXmax: 20,
			activeTxns: []uint64{},
			currentTxn: 20,
			expected: true,
		},
		{
			name: "Tuple from future transaction",
			tupleXmin: 30,
			tupleXmax: 0,
			snapshotXmin: 10,
			snapshotXmax: 20,
			activeTxns: []uint64{},
			currentTxn: 20,
			expected: false,
		},
		{
			name: "Tuple from active transaction",
			tupleXmin: 15,
			tupleXmax: 0,
			snapshotXmin: 10,
			snapshotXmax: 20,
			activeTxns: []uint64{15},
			currentTxn: 20,
			expected: false,
		},
		{
			name: "Deleted by committed transaction",
			tupleXmin: 10,
			tupleXmax: 12,
			snapshotXmin: 10,
			snapshotXmax: 20,
			activeTxns: []uint64{},
			currentTxn: 20,
			expected: false,
		},
		{
			name: "Deleted by active transaction - still visible",
			tupleXmin: 10,
			tupleXmax: 15,
			snapshotXmin: 10,
			snapshotXmax: 20,
			activeTxns: []uint64{15},
			currentTxn: 20,
			expected: true,
		},
		{
			name: "Own insert",
			tupleXmin: 20,
			tupleXmax: 0,
			snapshotXmin: 10,
			snapshotXmax: 25,
			activeTxns: []uint64{20},
			currentTxn: 20,
			expected: true,
		},
		{
			name: "Own delete",
			tupleXmin: 10,
			tupleXmax: 20,
			snapshotXmin: 10,
			snapshotXmax: 25,
			activeTxns: []uint64{20},
			currentTxn: 20,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := NewTupleWithMVCC([]byte("data"), tt.tupleXmin)
			if tt.tupleXmax > 0 {
				tuple.SetXmax(tt.tupleXmax)
			}

			snapshot := &Snapshot{
				XminSnapshot: tt.snapshotXmin,
				XmaxSnapshot: tt.snapshotXmax,
				ActiveTxns: tt.activeTxns,
			}

			visible := snapshot.IsVisible(tuple, tt.currentTxn)
			if visible != tt.expected {
				t.Errorf("Expected visibility %v, got %v", tt.expected, visible)
			}
		})
	}
}
