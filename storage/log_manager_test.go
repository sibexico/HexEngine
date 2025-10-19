package storage

import (
	"os"
	"testing"
)

func TestLogRecord(t *testing.T) {
	// Test log record creation
	record := &LogRecord{
		LSN: 1,
		PrevLSN: 0,
		TxnID: 100,
		Type: LogInsert,
		PageID: 5,
		Offset: 100,
		Length: 20,
		BeforeData: []byte("old value"),
		AfterData: []byte("new value"),
	}

	if record.LSN != 1 {
		t.Errorf("Expected LSN 1, got %d", record.LSN)
	}

	if record.Type != LogInsert {
		t.Errorf("Expected LogInsert type, got %v", record.Type)
	}
}

func TestLogRecordSerialization(t *testing.T) {
	// Test serialization and deserialization
	original := &LogRecord{
		LSN: 42,
		PrevLSN: 41,
		TxnID: 200,
		Type: LogUpdate,
		PageID: 10,
		Offset: 50,
		Length: 15,
		BeforeData: []byte("before data"),
		AfterData: []byte("after data"),
	}

	// Serialize
	data := original.Serialize()
	if len(data) == 0 {
		t.Fatal("Serialization produced empty data")
	}

	// Deserialize
	deserialized, err := DeserializeLogRecord(data)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify all fields match
	if deserialized.LSN != original.LSN {
		t.Errorf("LSN mismatch: expected %d, got %d", original.LSN, deserialized.LSN)
	}
	if deserialized.PrevLSN != original.PrevLSN {
		t.Errorf("PrevLSN mismatch: expected %d, got %d", original.PrevLSN, deserialized.PrevLSN)
	}
	if deserialized.TxnID != original.TxnID {
		t.Errorf("TxnID mismatch: expected %d, got %d", original.TxnID, deserialized.TxnID)
	}
	if deserialized.Type != original.Type {
		t.Errorf("Type mismatch: expected %v, got %v", original.Type, deserialized.Type)
	}
	if deserialized.PageID != original.PageID {
		t.Errorf("PageID mismatch: expected %d, got %d", original.PageID, deserialized.PageID)
	}
	if string(deserialized.BeforeData) != string(original.BeforeData) {
		t.Errorf("BeforeData mismatch: expected %s, got %s", original.BeforeData, deserialized.BeforeData)
	}
	if string(deserialized.AfterData) != string(original.AfterData) {
		t.Errorf("AfterData mismatch: expected %s, got %s", original.AfterData, deserialized.AfterData)
	}
}

func TestLogManager(t *testing.T) {
	logFile := "test_log_manager.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	if lm == nil {
		t.Fatal("NewLogManager returned nil")
	}

	// Initial state checks
	if lm.GetCurrentLSN() != 0 {
		t.Errorf("Expected initial LSN to be 0, got %d", lm.GetCurrentLSN())
	}
}

func TestAppendLog(t *testing.T) {
	logFile := "test_append.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Append first log record
	record1 := &LogRecord{
		TxnID: 1,
		Type: LogInsert,
		PageID: 100,
		AfterData: []byte("test data 1"),
	}

	lsn1, err := lm.AppendLog(record1)
	if err != nil {
		t.Fatalf("Failed to append log: %v", err)
	}

	if lsn1 != 1 {
		t.Errorf("Expected LSN 1, got %d", lsn1)
	}

	// Append second log record
	record2 := &LogRecord{
		TxnID: 1,
		Type: LogUpdate,
		PageID: 100,
		BeforeData: []byte("old"),
		AfterData: []byte("new"),
	}

	lsn2, err := lm.AppendLog(record2)
	if err != nil {
		t.Fatalf("Failed to append second log: %v", err)
	}

	if lsn2 != 2 {
		t.Errorf("Expected LSN 2, got %d", lsn2)
	}

	// Verify LSNs are monotonically increasing
	if lsn2 <= lsn1 {
		t.Error("LSNs should be monotonically increasing")
	}
}

func TestLogFlush(t *testing.T) {
	logFile := "test_flush.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Append several log records
	for i := 0; i < 5; i++ {
		record := &LogRecord{
			TxnID: uint64(i + 1),
			Type: LogInsert,
			PageID: uint32(i),
			AfterData: []byte("test data"),
		}
		_, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log %d: %v", i, err)
		}
	}

	// Flush logs to disk
	err = lm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify flushed LSN
	flushedLSN := lm.GetFlushedLSN()
	if flushedLSN != 5 {
		t.Errorf("Expected flushed LSN 5, got %d", flushedLSN)
	}
}

func TestLogBuffering(t *testing.T) {
	logFile := "test_buffering.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Append logs without explicit flush
	for i := 0; i < 3; i++ {
		record := &LogRecord{
			TxnID: uint64(i + 1),
			Type: LogInsert,
			PageID: uint32(i),
			AfterData: []byte("buffered data"),
		}
		_, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append log: %v", err)
		}
	}

	// Flushed LSN should be 0 (nothing flushed yet)
	if lm.GetFlushedLSN() != 0 {
		t.Errorf("Expected flushed LSN 0 before flush, got %d", lm.GetFlushedLSN())
	}

	// After flush, flushed LSN should match current LSN
	lm.Flush()
	if lm.GetFlushedLSN() != lm.GetCurrentLSN() {
		t.Errorf("After flush, flushed LSN should equal current LSN")
	}
}

func TestLogRecovery(t *testing.T) {
	logFile := "test_recovery.wal"
	defer os.Remove(logFile)

	// Write logs and simulate crash
	{
		lm, err := NewLogManager(logFile)
		if err != nil {
			t.Fatalf("Failed to create LogManager: %v", err)
		}

		// Transaction 1: Insert and commit
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, PageID: 10, AfterData: []byte("txn1 data")})
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})

		// Transaction 2: Insert but don't commit
		lm.AppendLog(&LogRecord{TxnID: 2, Type: LogInsert, PageID: 20, AfterData: []byte("txn2 data")})

		// Transaction 3: Update and commit
		lm.AppendLog(&LogRecord{TxnID: 3, Type: LogUpdate, PageID: 30, BeforeData: []byte("old"), AfterData: []byte("new")})
		lm.AppendLog(&LogRecord{TxnID: 3, Type: LogCommit})

		// Flush logs
		lm.Flush()
		lm.Close()
	}

	// Recover from log file
	{
		lm, err := NewLogManager(logFile)
		if err != nil {
			t.Fatalf("Failed to reopen LogManager: %v", err)
		}
		defer lm.Close()

		// Read and analyze logs
		records, err := lm.ReadAllLogs()
		if err != nil {
			t.Fatalf("Failed to read logs: %v", err)
		}

		if len(records) != 5 {
			t.Errorf("Expected 5 log records, got %d", len(records))
		}

		// Verify recovery logic
		committedTxns := make(map[uint64]bool)
		for _, record := range records {
			if record.Type == LogCommit {
				committedTxns[record.TxnID] = true
			}
		}

		// Transaction 1 and 3 should be committed
		if !committedTxns[1] {
			t.Error("Transaction 1 should be committed")
		}
		if !committedTxns[3] {
			t.Error("Transaction 3 should be committed")
		}

		// Transaction 2 should not be committed
		if committedTxns[2] {
			t.Error("Transaction 2 should not be committed")
		}
	}
}

func TestLogTypes(t *testing.T) {
	logFile := "test_types.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Test all log types
	types := []LogType{LogInsert, LogDelete, LogUpdate, LogCommit, LogAbort, LogCheckpoint}
	typeNames := []string{"Insert", "Delete", "Update", "Commit", "Abort", "Checkpoint"}

	for i, logType := range types {
		record := &LogRecord{
			TxnID: uint64(i + 1),
			Type: logType,
			PageID: uint32(i),
			AfterData: []byte("data"),
		}

		lsn, err := lm.AppendLog(record)
		if err != nil {
			t.Fatalf("Failed to append %s log: %v", typeNames[i], err)
		}

		if lsn != uint64(i+1) {
			t.Errorf("Expected LSN %d for %s, got %d", i+1, typeNames[i], lsn)
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	logFile := "test_concurrent.wal"
	defer os.Remove(logFile)

	lm, err := NewLogManager(logFile)
	if err != nil {
		t.Fatalf("Failed to create LogManager: %v", err)
	}
	defer lm.Close()

	// Simulate concurrent appends
	done := make(chan bool)
	numGoroutines := 5
	recordsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < recordsPerGoroutine; j++ {
				record := &LogRecord{
					TxnID: uint64(id*100 + j),
					Type: LogInsert,
					PageID: uint32(id),
					AfterData: []byte("concurrent data"),
				}
				_, err := lm.AppendLog(record)
				if err != nil {
					t.Errorf("Failed to append from goroutine %d: %v", id, err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Flush and verify
	lm.Flush()

	expectedLSN := uint64(numGoroutines * recordsPerGoroutine)
	if lm.GetCurrentLSN() != expectedLSN {
		t.Errorf("Expected final LSN %d, got %d", expectedLSN, lm.GetCurrentLSN())
	}
}

func TestLogPersistence(t *testing.T) {
	logFile := "test_persistence.wal"
	defer os.Remove(logFile)

	// Write logs
	{
		lm, _ := NewLogManager(logFile)
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogInsert, AfterData: []byte("persistent data")})
		lm.AppendLog(&LogRecord{TxnID: 1, Type: LogCommit})
		lm.Flush()
		lm.Close()
	}

	// Reopen and verify
	{
		lm, _ := NewLogManager(logFile)
		defer lm.Close()

		records, err := lm.ReadAllLogs()
		if err != nil {
			t.Fatalf("Failed to read persisted logs: %v", err)
		}

		if len(records) != 2 {
			t.Errorf("Expected 2 persisted records, got %d", len(records))
		}

		if records[0].TxnID != 1 || records[0].Type != LogInsert {
			t.Error("First record corrupted")
		}

		if records[1].Type != LogCommit {
			t.Error("Second record corrupted")
		}
	}
}
