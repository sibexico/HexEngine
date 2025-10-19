package storage

import (
	"bytes"
	"testing"
)

func TestSlottedPage(t *testing.T) {
	page := NewSlottedPage()

	if page == nil {
		t.Fatal("NewSlottedPage returned nil")
	}

	// Verify initial state
	if page.GetTupleCount() != 0 {
		t.Errorf("Expected initial tuple count to be 0, got %d", page.GetTupleCount())
	}

	if page.GetFreeSpaceSize() <= 0 {
		t.Errorf("Expected free space to be positive, got %d", page.GetFreeSpaceSize())
	}
}

func TestInsertTuple(t *testing.T) {
	page := NewSlottedPage()

	// Test data
	testData := []byte("Hello, World! This is test tuple data.")
	tuple := NewTuple(testData)

	// Insert tuple
	slotId, err := page.InsertTuple(tuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	if slotId != 0 {
		t.Errorf("Expected first slot ID to be 0, got %d", slotId)
	}

	// Verify tuple count updated
	if page.GetTupleCount() != 1 {
		t.Errorf("Expected tuple count to be 1 after insertion, got %d", page.GetTupleCount())
	}

	// Verify tuple can be read back
	readTuple, err := page.ReadTuple(slotId)
	if err != nil {
		t.Fatalf("Failed to read tuple: %v", err)
	}

	if string(readTuple.GetData()) != string(testData) {
		t.Errorf("Read tuple data doesn't match. Expected %s, got %s",
			string(testData), string(readTuple.GetData()))
	}
}

func TestDeleteTuple(t *testing.T) {
	page := NewSlottedPage()

	// Insert a tuple first
	testData := []byte("Test tuple to be deleted")
	tuple := NewTuple(testData)
	slotId, err := page.InsertTuple(tuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Delete the tuple
	err = page.DeleteTuple(slotId)
	if err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}

	// Verify tuple count updated
	if page.GetTupleCount() != 0 {
		t.Errorf("Expected tuple count to be 0 after deletion, got %d", page.GetTupleCount())
	}

	// Verify tuple can't be read anymore
	_, err = page.ReadTuple(slotId)
	if err == nil {
		t.Error("Expected error when reading deleted tuple, but got none")
	}

	// Verify space can be reused - insert another tuple and it should get the same slot
	newData := []byte("New tuple reusing space")
	newTuple := NewTuple(newData)
	newSlotId, err := page.InsertTuple(newTuple)
	if err != nil {
		t.Fatalf("Failed to insert new tuple: %v", err)
	}

	if newSlotId != slotId {
		t.Errorf("Expected new tuple to reuse slot %d, but got %d", slotId, newSlotId)
	}
}

func TestUpdateTuple(t *testing.T) {
	page := NewSlottedPage()

	// Insert original tuple
	originalData := []byte("Original tuple data")
	tuple := NewTuple(originalData)
	slotId, err := page.InsertTuple(tuple)
	if err != nil {
		t.Fatalf("Failed to insert tuple: %v", err)
	}

	// Update the tuple
	updatedData := []byte("Updated tuple data with more content")
	updatedTuple := NewTuple(updatedData)
	err = page.UpdateTuple(slotId, updatedTuple)
	if err != nil {
		t.Fatalf("Failed to update tuple: %v", err)
	}

	// Verify the updated data can be read back
	readTuple, err := page.ReadTuple(slotId)
	if err != nil {
		t.Fatalf("Failed to read updated tuple: %v", err)
	}

	if string(readTuple.GetData()) != string(updatedData) {
		t.Errorf("Updated tuple data doesn't match. Expected %s, got %s",
			string(updatedData), string(readTuple.GetData()))
	}

	// Tuple count should remain the same
	if page.GetTupleCount() != 1 {
		t.Errorf("Expected tuple count to remain 1 after update, got %d", page.GetTupleCount())
	}
}

// TestPageSerialization tests that a page can be serialized and deserialized
func TestPageSerialization(t *testing.T) {
	page := NewSlottedPage()

	// Insert multiple tuples
	tuples := [][]byte{
		[]byte("First tuple with some data"),
		[]byte("Second tuple"),
		[]byte("Third tuple with more content here"),
	}

	slotIds := make([]uint16, 0)
	for _, data := range tuples {
		tuple := NewTuple(data)
		slotId, err := page.InsertTuple(tuple)
		if err != nil {
			t.Fatalf("Failed to insert tuple: %v", err)
		}
		slotIds = append(slotIds, slotId)
	}

	// Serialize the page
	serialized := page.Serialize()
	if len(serialized) != PageSize {
		t.Errorf("Expected serialized page to be %d bytes, got %d", PageSize, len(serialized))
	}

	// Deserialize into new page
	deserializedPage, err := DeserializeSlottedPage(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize page: %v", err)
	}

	// Verify tuple count matches
	if deserializedPage.GetTupleCount() != page.GetTupleCount() {
		t.Errorf("Tuple count mismatch. Expected %d, got %d",
			page.GetTupleCount(), deserializedPage.GetTupleCount())
	}

	// Verify free space matches
	if deserializedPage.GetFreeSpaceSize() != page.GetFreeSpaceSize() {
		t.Errorf("Free space mismatch. Expected %d, got %d",
			page.GetFreeSpaceSize(), deserializedPage.GetFreeSpaceSize())
	}

	// Verify all tuples can be read back correctly
	for i, slotId := range slotIds {
		originalTuple, err := page.ReadTuple(slotId)
		if err != nil {
			t.Fatalf("Failed to read original tuple %d: %v", i, err)
		}

		deserializedTuple, err := deserializedPage.ReadTuple(slotId)
		if err != nil {
			t.Fatalf("Failed to read deserialized tuple %d: %v", i, err)
		}

		if !bytes.Equal(originalTuple.GetData(), deserializedTuple.GetData()) {
			t.Errorf("Tuple %d data mismatch. Expected %s, got %s",
				i, string(originalTuple.GetData()), string(deserializedTuple.GetData()))
		}
	}
}

// TestPageSerializationWithDeletes tests serialization with deleted tuples
func TestPageSerializationWithDeletes(t *testing.T) {
	page := NewSlottedPage()

	// Insert tuples
	tuple1 := NewTuple([]byte("Tuple 1"))
	tuple2 := NewTuple([]byte("Tuple 2"))
	tuple3 := NewTuple([]byte("Tuple 3"))

	slotId1, _ := page.InsertTuple(tuple1)
	slotId2, _ := page.InsertTuple(tuple2)
	slotId3, _ := page.InsertTuple(tuple3)

	// Delete middle tuple
	err := page.DeleteTuple(slotId2)
	if err != nil {
		t.Fatalf("Failed to delete tuple: %v", err)
	}

	// Serialize and deserialize
	serialized := page.Serialize()
	deserializedPage, err := DeserializeSlottedPage(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify tuple 1 and 3 exist
	tuple1Read, err := deserializedPage.ReadTuple(slotId1)
	if err != nil {
		t.Errorf("Failed to read tuple 1: %v", err)
	} else if string(tuple1Read.GetData()) != "Tuple 1" {
		t.Errorf("Tuple 1 data mismatch")
	}

	tuple3Read, err := deserializedPage.ReadTuple(slotId3)
	if err != nil {
		t.Errorf("Failed to read tuple 3: %v", err)
	} else if string(tuple3Read.GetData()) != "Tuple 3" {
		t.Errorf("Tuple 3 data mismatch")
	}

	// Verify tuple 2 is deleted
	_, err = deserializedPage.ReadTuple(slotId2)
	if err == nil {
		t.Error("Expected error when reading deleted tuple, got nil")
	}
}

// TestEmptyPageSerialization tests serialization of an empty page
func TestEmptyPageSerialization(t *testing.T) {
	page := NewSlottedPage()

	serialized := page.Serialize()
	if len(serialized) != PageSize {
		t.Errorf("Expected serialized page to be %d bytes, got %d", PageSize, len(serialized))
	}

	deserializedPage, err := DeserializeSlottedPage(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize empty page: %v", err)
	}

	if deserializedPage.GetTupleCount() != 0 {
		t.Errorf("Expected tuple count to be 0, got %d", deserializedPage.GetTupleCount())
	}

	if deserializedPage.GetFreeSpaceSize() != page.GetFreeSpaceSize() {
		t.Errorf("Free space mismatch. Expected %d, got %d",
			page.GetFreeSpaceSize(), deserializedPage.GetFreeSpaceSize())
	}
}
