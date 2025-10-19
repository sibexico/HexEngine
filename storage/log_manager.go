package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// LogType represents the type of log record
type LogType byte

const (
	LogInsert LogType = iota
	LogDelete
	LogUpdate
	LogCommit
	LogAbort
	LogCheckpoint
)

// String returns string representation of LogType
func (lt LogType) String() string {
	switch lt {
	case LogInsert:
		return "INSERT"
	case LogDelete:
		return "DELETE"
	case LogUpdate:
		return "UPDATE"
	case LogCommit:
		return "COMMIT"
	case LogAbort:
		return "ABORT"
	case LogCheckpoint:
		return "CHECKPOINT"
	default:
		return "UNKNOWN"
	}
}

// LogRecord represents a single WAL entry
type LogRecord struct {
	LSN uint64 // Log Sequence Number (unique, monotonic)
	PrevLSN uint64 // Previous LSN for this transaction
	TxnID uint64 // Transaction ID
	Type LogType // Type of operation
	PageID uint32 // Affected page
	Offset uint16 // Offset within page
	Length uint16 // Length of data
	BeforeData []byte // Old value (for UNDO)
	AfterData []byte // New value (for REDO)
}

// Serialize converts LogRecord to bytes
// Format: LSN(8) | PrevLSN(8) | TxnID(8) | Type(1) | PageID(4) | Offset(2) | Length(2) |
//
//	BeforeDataLen(2) | BeforeData | AfterDataLen(2) | AfterData
func (lr *LogRecord) Serialize() []byte {
	// Calculate size: fixed header (33 bytes) + 2 length fields + variable data
	beforeLen := len(lr.BeforeData)
	afterLen := len(lr.AfterData)
	size := 33 + 2 + beforeLen + 2 + afterLen // 33 fixed + 2 for beforeLen + data + 2 for afterLen + data

	buf := make([]byte, size)
	offset := 0

	// Fixed header
	binary.LittleEndian.PutUint64(buf[offset:], lr.LSN)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], lr.PrevLSN)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], lr.TxnID)
	offset += 8
	buf[offset] = byte(lr.Type)
	offset += 1
	binary.LittleEndian.PutUint32(buf[offset:], lr.PageID)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], lr.Offset)
	offset += 2
	binary.LittleEndian.PutUint16(buf[offset:], lr.Length)
	offset += 2

	// Variable length BeforeData
	binary.LittleEndian.PutUint16(buf[offset:], uint16(beforeLen))
	offset += 2
	if beforeLen > 0 {
		copy(buf[offset:], lr.BeforeData)
		offset += beforeLen
	}

	// Variable length AfterData
	binary.LittleEndian.PutUint16(buf[offset:], uint16(afterLen))
	offset += 2
	if afterLen > 0 {
		copy(buf[offset:], lr.AfterData)
	}

	return buf
}

// DeserializeLogRecord creates LogRecord from bytes
func DeserializeLogRecord(data []byte) (*LogRecord, error) {
	minSize := 33 + 2 + 2 // Fixed header + 2 length fields
	if len(data) < minSize {
		return nil, fmt.Errorf("data too short for log record: %d bytes (need at least %d)", len(data), minSize)
	}

	lr := &LogRecord{}
	offset := 0

	// Fixed header
	lr.LSN = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	lr.PrevLSN = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	lr.TxnID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	lr.Type = LogType(data[offset])
	offset += 1
	lr.PageID = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	lr.Offset = binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	lr.Length = binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	// Variable length BeforeData
	if offset+2 > len(data) {
		return nil, fmt.Errorf("data too short for before data length")
	}
	beforeLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if beforeLen > 0 {
		if offset+int(beforeLen) > len(data) {
			return nil, fmt.Errorf("invalid before data length: need %d bytes, have %d", beforeLen, len(data)-offset)
		}
		lr.BeforeData = make([]byte, beforeLen)
		copy(lr.BeforeData, data[offset:offset+int(beforeLen)])
		offset += int(beforeLen)
	}

	// Variable length AfterData
	if offset+2 > len(data) {
		return nil, fmt.Errorf("data too short for after data length")
	}
	afterLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if afterLen > 0 {
		if offset+int(afterLen) > len(data) {
			return nil, fmt.Errorf("invalid after data length: need %d bytes, have %d", afterLen, len(data)-offset)
		}
		lr.AfterData = make([]byte, afterLen)
		copy(lr.AfterData, data[offset:offset+int(afterLen)])
	}

	return lr, nil
}

// LogManager manages the write-ahead log
// Now supports both serial and parallel modes for better concurrency
type LogManager struct {
	logFile *os.File
	currentLSN uint64
	flushedLSN uint64
	buffer []byte
	bufferSize int
	maxBufferSize int
	mutex sync.Mutex

	// Parallel WAL support
	useParallel bool
	parallelLog *ParallelLogManager

	// Compression support
	useCompression bool
	compressionAlg string // "delta", "snappy", "none"
}

const DefaultLogBufferSize = 4096 // 4KB buffer

// NewLogManager creates a new log manager
func NewLogManager(logFileName string) (*LogManager, error) {
	return NewLogManagerWithConfig(logFileName, false, false, "none")
}

// NewLogManagerWithConfig creates a log manager with specific configuration
func NewLogManagerWithConfig(logFileName string, useParallel bool, useCompression bool, compressionAlg string) (*LogManager, error) {
	// If parallel mode requested, use ParallelLogManager
	if useParallel {
		plm, err := NewParallelLogManager(logFileName)
		if err != nil {
			return nil, err
		}

		return &LogManager{
			useParallel: true,
			parallelLog: plm,
			useCompression: useCompression,
			compressionAlg: compressionAlg,
		}, nil
	}

	// Standard serial mode
	// Open or create log file in append mode
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	lm := &LogManager{
		logFile: file,
		currentLSN: 0,
		flushedLSN: 0,
		buffer: make([]byte, 0, DefaultLogBufferSize),
		bufferSize: 0,
		maxBufferSize: DefaultLogBufferSize,
		mutex: sync.Mutex{},
		useParallel: false,
		parallelLog: nil,
		useCompression: useCompression,
		compressionAlg: compressionAlg,
	}

	// If file exists and has content, determine current LSN
	fileInfo, err := file.Stat()
	if err == nil && fileInfo.Size() > 0 {
		// Read existing logs to find max LSN
		records, err := lm.readLogsFromFile()
		if err == nil && len(records) > 0 {
			lastRecord := records[len(records)-1]
			lm.currentLSN = lastRecord.LSN
			lm.flushedLSN = lastRecord.LSN
		}
	}

	return lm, nil
}

// AppendLog adds a log record and returns its LSN
func (lm *LogManager) AppendLog(record *LogRecord) (uint64, error) {
	// Delegate to parallel log manager if enabled
	if lm.useParallel {
		return lm.parallelLog.AppendLog(record)
	}

	// Standard serial implementation
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Assign LSN
	lm.currentLSN++
	record.LSN = lm.currentLSN

	// Serialize (with compression if enabled)
	var data []byte
	if lm.useCompression && lm.compressionAlg == "delta" && record.Type == LogUpdate {
		// Use delta compression for updates
		compressed := CompressLogRecord(record)
		data = compressed.Serialize()
	} else {
		// Standard serialization
		data = record.Serialize()
	}

	// Write record size first (for reading back)
	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, uint32(len(data)))
	lm.buffer = append(lm.buffer, sizeBytes...)
	lm.buffer = append(lm.buffer, data...)
	lm.bufferSize += len(sizeBytes) + len(data)

	// Flush if buffer is full
	if lm.bufferSize >= lm.maxBufferSize {
		return record.LSN, lm.flushInternal()
	}

	return record.LSN, nil
}

// Flush writes buffered log records to disk
func (lm *LogManager) Flush() error {
	// Delegate to parallel log manager if enabled
	if lm.useParallel {
		return lm.parallelLog.Flush()
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	return lm.flushInternal()
}

// FlushToLSN flushes all log records up to and including the specified LSN
func (lm *LogManager) FlushToLSN(lsn uint64) error {
	// Delegate to parallel log manager if enabled
	if lm.useParallel {
		return lm.parallelLog.FlushToLSN(lsn)
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// If already flushed, nothing to do
	if lsn <= lm.flushedLSN {
		return nil
	}

	// If LSN hasn't been written yet, wait is not needed - just flush what we have
	if lsn > lm.currentLSN {
		return fmt.Errorf("cannot flush to LSN %d: current LSN is %d", lsn, lm.currentLSN)
	}

	return lm.flushInternal()
}

// flushInternal performs actual flush (caller must hold lock)
func (lm *LogManager) flushInternal() error {
	if lm.bufferSize == 0 {
		return nil
	}

	_, err := lm.logFile.Write(lm.buffer)
	if err != nil {
		return fmt.Errorf("failed to write to log file: %w", err)
	}

	err = lm.logFile.Sync() // Force to disk
	if err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}

	lm.flushedLSN = lm.currentLSN
	lm.buffer = lm.buffer[:0]
	lm.bufferSize = 0

	return nil
}

// GetCurrentLSN returns the current LSN
func (lm *LogManager) GetCurrentLSN() uint64 {
	if lm.useParallel {
		return lm.parallelLog.GetCurrentLSN()
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	return lm.currentLSN
}

// GetFlushedLSN returns the last flushed LSN
func (lm *LogManager) GetFlushedLSN() uint64 {
	if lm.useParallel {
		return lm.parallelLog.GetFlushedLSN()
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	return lm.flushedLSN
}

// ReadAllLogs reads all log records from the file
func (lm *LogManager) ReadAllLogs() ([]*LogRecord, error) {
	if lm.useParallel {
		return lm.parallelLog.ReadAllLogs()
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Flush any buffered records first
	err := lm.flushInternal()
	if err != nil {
		return nil, err
	}

	return lm.readLogsFromFile()
}

// readLogsFromFile reads logs from file (caller must hold lock or file access)
func (lm *LogManager) readLogsFromFile() ([]*LogRecord, error) {
	// Seek to beginning
	_, err := lm.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	records := make([]*LogRecord, 0)

	for {
		// Read record size
		sizeBytes := make([]byte, 4)
		n, err := lm.logFile.Read(sizeBytes)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read record size: %w", err)
		}
		if n != 4 {
			break // Incomplete read
		}

		recordSize := binary.LittleEndian.Uint32(sizeBytes)
		if recordSize == 0 || recordSize > 1024*1024 { // Sanity check: max 1MB per record
			break
		}

		// Read record data
		recordData := make([]byte, recordSize)
		n, err = lm.logFile.Read(recordData)
		if err != nil {
			return nil, fmt.Errorf("failed to read record data: %w", err)
		}
		if n != int(recordSize) {
			break // Incomplete read
		}

		// Deserialize
		record, err := DeserializeLogRecord(recordData)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize record: %w", err)
		}

		records = append(records, record)
	}

	// Seek back to end for appending
	_, err = lm.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return records, nil
}

// Close closes the log manager
func (lm *LogManager) Close() error {
	if lm.useParallel {
		return lm.parallelLog.Close()
	}

	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	// Flush any remaining buffered records
	err := lm.flushInternal()
	if err != nil {
		return err
	}

	if lm.logFile != nil {
		return lm.logFile.Close()
	}
	return nil
}
