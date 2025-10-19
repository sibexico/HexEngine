package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

const (
	ParallelLogBufferSize = 16384 // 16KB buffer
)

// ParallelLogManager provides lock-free LSN allocation using atomic operations,
// allowing multiple threads to append log records with reduced contention.
type ParallelLogManager struct {
	logFile    *os.File
	currentLSN atomic.Uint64
	flushedLSN atomic.Uint64

	buffer     *logBuffer
	flushMutex sync.Mutex
}

// logBuffer represents a single log buffer with its own lock
type logBuffer struct {
	data     []byte
	size     int
	capacity int
	mutex    sync.Mutex
}

func newLogBuffer(capacity int) *logBuffer {
	return &logBuffer{
		data:     make([]byte, 0, capacity),
		capacity: capacity,
		size:     0,
	}
}

func (lb *logBuffer) append(data []byte) bool {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	if lb.size+len(data) > lb.capacity {
		return false // Buffer full
	}

	lb.data = append(lb.data, data...)
	lb.size += len(data)
	return true
}

func (lb *logBuffer) flush(writer io.Writer) (int, error) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	if lb.size == 0 {
		return 0, nil
	}

	n, err := writer.Write(lb.data)
	if err != nil {
		return n, err
	}

	// Clear buffer
	lb.data = lb.data[:0]
	lb.size = 0

	return n, nil
}

func (lb *logBuffer) getSize() int {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()
	return lb.size
}

// NewParallelLogManager creates a new parallel log manager
func NewParallelLogManager(logFileName string) (*ParallelLogManager, error) {
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	plm := &ParallelLogManager{
		logFile: file,
		buffer:  newLogBuffer(ParallelLogBufferSize),
	}

	// Initialize LSN from existing log file
	fileInfo, err := file.Stat()
	if err == nil && fileInfo.Size() > 0 {
		_, err := file.Seek(0, io.SeekStart)
		if err == nil {
			var maxLSN uint64
			for {
				// Read record size
				sizeBytes := make([]byte, 4)
				n, err := file.Read(sizeBytes)
				if err == io.EOF || n != 4 {
					break
				}
				recordSize := binary.LittleEndian.Uint32(sizeBytes)
				if recordSize == 0 || recordSize > 1024*1024 {
					break
				}
				// Read record data
				recordData := make([]byte, recordSize)
				n, err = file.Read(recordData)
				if err != nil || n != int(recordSize) {
					break
				}
				// Deserialize just to get LSN
				record, err := DeserializeLogRecord(recordData)
				if err == nil && record.LSN > maxLSN {
					maxLSN = record.LSN
				}
			}
			if maxLSN > 0 {
				plm.currentLSN.Store(maxLSN)
				plm.flushedLSN.Store(maxLSN)
			}
			file.Seek(0, io.SeekEnd)
		}
	}

	return plm, nil
}

// AppendLog adds a log record using atomic LSN allocation
func (plm *ParallelLogManager) AppendLog(record *LogRecord) (uint64, error) {
	// Atomically allocate LSN (lock-free!)
	lsn := plm.currentLSN.Add(1)
	record.LSN = lsn

	// Serialize record
	data := record.Serialize()

	// Prepare record with size prefix
	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, uint32(len(data)))
	fullData := append(sizeBytes, data...)

	// Append to buffer
	if !plm.buffer.append(fullData) {
		// Buffer full - flush and retry
		err := plm.Flush()
		if err != nil {
			return 0, fmt.Errorf("failed to flush before append: %w", err)
		}
		if !plm.buffer.append(fullData) {
			return 0, fmt.Errorf("failed to append after flush - record too large")
		}
	}

	return lsn, nil
}

// Flush writes all buffered log records to disk
func (plm *ParallelLogManager) Flush() error {
	plm.flushMutex.Lock()
	defer plm.flushMutex.Unlock()

	n, err := plm.buffer.flush(plm.logFile)
	if err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if n > 0 {
		// Fsync to ensure durability
		err = plm.logFile.Sync()
		if err != nil {
			return fmt.Errorf("failed to sync log file: %w", err)
		}

		// Update flushed LSN
		plm.flushedLSN.Store(plm.currentLSN.Load())
	}

	return nil
}

// FlushToLSN flushes all log records up to the specified LSN
func (plm *ParallelLogManager) FlushToLSN(lsn uint64) error {
	// If already flushed, return immediately
	if lsn <= plm.flushedLSN.Load() {
		return nil
	}

	// If LSN hasn't been written yet, wait
	if lsn > plm.currentLSN.Load() {
		return fmt.Errorf("cannot flush to LSN %d: current LSN is %d", lsn, plm.currentLSN.Load())
	}

	return plm.Flush()
}

// GetCurrentLSN returns the current LSN
func (plm *ParallelLogManager) GetCurrentLSN() uint64 {
	return plm.currentLSN.Load()
}

// GetFlushedLSN returns the last flushed LSN
func (plm *ParallelLogManager) GetFlushedLSN() uint64 {
	return plm.flushedLSN.Load()
}

// ReadAllLogs reads all log records from the file
func (plm *ParallelLogManager) ReadAllLogs() ([]*LogRecord, error) {
	// First flush all buffers
	err := plm.Flush()
	if err != nil {
		return nil, err
	}

	// Now read with a separate lock to avoid deadlock
	return plm.readLogsFromFile()
}

// readLogsFromFile reads logs from file
func (plm *ParallelLogManager) readLogsFromFile() ([]*LogRecord, error) {
	// Seek to beginning
	_, err := plm.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	records := make([]*LogRecord, 0)

	for {
		// Read record size
		sizeBytes := make([]byte, 4)
		n, err := plm.logFile.Read(sizeBytes)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read record size: %w", err)
		}
		if n != 4 {
			break
		}

		recordSize := binary.LittleEndian.Uint32(sizeBytes)
		if recordSize == 0 || recordSize > 1024*1024 {
			break
		}

		// Read record data
		recordData := make([]byte, recordSize)
		n, err = plm.logFile.Read(recordData)
		if err != nil {
			return nil, fmt.Errorf("failed to read record data: %w", err)
		}
		if n != int(recordSize) {
			break
		}

		// Deserialize
		record, err := DeserializeLogRecord(recordData)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize record: %w", err)
		}

		records = append(records, record)
	}

	// Seek back to end
	_, err = plm.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return records, nil
}

// Close closes the parallel log manager
func (plm *ParallelLogManager) Close() error {
	plm.flushMutex.Lock()
	defer plm.flushMutex.Unlock()

	// Flush remaining buffered records
	_, err := plm.buffer.flush(plm.logFile)
	if err != nil {
		return fmt.Errorf("failed to flush buffer on close: %w", err)
	}

	err = plm.logFile.Sync()
	if err != nil {
		return err
	}

	if plm.logFile != nil {
		return plm.logFile.Close()
	}
	return nil
}

// GetBufferStats returns statistics about buffer usage
func (plm *ParallelLogManager) GetBufferStats() int {
	return plm.buffer.getSize()
}
