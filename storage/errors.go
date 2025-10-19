package storage

import (
	"fmt"
)

// ErrorCode represents different types of storage errors
type ErrorCode int

const (
	// Generic errors
	ErrCodeUnknown ErrorCode = iota
	ErrCodeInternal

	// Page errors
	ErrCodePageNotFound
	ErrCodePageFull
	ErrCodeInvalidPageID
	ErrCodePageCorrupted

	// Buffer pool errors
	ErrCodeNoFreePages
	ErrCodePagePinned
	ErrCodeInvalidPin

	// Transaction errors
	ErrCodeTxnNotFound
	ErrCodeTxnAlreadyCommitted
	ErrCodeTxnAlreadyAborted
	ErrCodeInvalidTxnState

	// Recovery errors
	ErrCodeRecoveryFailed
	ErrCodeLogCorrupted
	ErrCodeCheckpointFailed

	// B+ Tree errors
	ErrCodeKeyNotFound
	ErrCodeDuplicateKey
	ErrCodeInvalidKey

	// Disk errors
	ErrCodeDiskFull
	ErrCodeDiskReadFailed
	ErrCodeDiskWriteFailed
	ErrCodeFileNotFound
)

// StorageError represents a storage engine error with context
type StorageError struct {
	Code ErrorCode
	Message string
	Op string // Operation that failed
	Err error // Underlying error (if any)
}

// Error implements the error interface
func (e *StorageError) Error() string {
	if e.Op != "" {
		if e.Err != nil {
			return fmt.Sprintf("%s: %s: %v", e.Op, e.Message, e.Err)
		}
		return fmt.Sprintf("%s: %s", e.Op, e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the underlying error
func (e *StorageError) Unwrap() error {
	return e.Err
}

// Is checks if the error matches a specific error code
func (e *StorageError) Is(target error) bool {
	if t, ok := target.(*StorageError); ok {
		return e.Code == t.Code
	}
	return false
}

// NewStorageError creates a new storage error
func NewStorageError(code ErrorCode, op, message string, err error) *StorageError {
	return &StorageError{
		Code: code,
		Message: message,
		Op: op,
		Err: err,
	}
}

// Helper functions for common errors

func ErrPageNotFound(op string, pageID uint32) *StorageError {
	return NewStorageError(
		ErrCodePageNotFound,
		op,
		fmt.Sprintf("page %d not found", pageID),
		nil,
	)
}

func ErrPageFull(op string, pageID uint32) *StorageError {
	return NewStorageError(
		ErrCodePageFull,
		op,
		fmt.Sprintf("page %d is full", pageID),
		nil,
	)
}

func ErrNoFreePages(op string) *StorageError {
	return NewStorageError(
		ErrCodeNoFreePages,
		op,
		"no free pages available in buffer pool",
		nil,
	)
}

func ErrPagePinned(op string, pageID uint32, pinCount int) *StorageError {
	return NewStorageError(
		ErrCodePagePinned,
		op,
		fmt.Sprintf("page %d is pinned (pin count: %d)", pageID, pinCount),
		nil,
	)
}

func ErrTxnNotFound(op string, txnID uint64) *StorageError {
	return NewStorageError(
		ErrCodeTxnNotFound,
		op,
		fmt.Sprintf("transaction %d not found", txnID),
		nil,
	)
}

func ErrInvalidTxnState(op string, txnID uint64, state string) *StorageError {
	return NewStorageError(
		ErrCodeInvalidTxnState,
		op,
		fmt.Sprintf("transaction %d in invalid state: %s", txnID, state),
		nil,
	)
}

func ErrKeyNotFound(op string, key int) *StorageError {
	return NewStorageError(
		ErrCodeKeyNotFound,
		op,
		fmt.Sprintf("key %d not found", key),
		nil,
	)
}

func ErrDuplicateKey(op string, key int) *StorageError {
	return NewStorageError(
		ErrCodeDuplicateKey,
		op,
		fmt.Sprintf("duplicate key %d", key),
		nil,
	)
}

func ErrDiskOperation(op string, err error) *StorageError {
	return NewStorageError(
		ErrCodeDiskWriteFailed,
		op,
		"disk operation failed",
		err,
	)
}

func ErrLogCorrupted(op string, lsn uint64) *StorageError {
	return NewStorageError(
		ErrCodeLogCorrupted,
		op,
		fmt.Sprintf("log corrupted at LSN %d", lsn),
		nil,
	)
}

// IsErrorCode checks if an error has a specific error code
func IsErrorCode(err error, code ErrorCode) bool {
	if se, ok := err.(*StorageError); ok {
		return se.Code == code
	}
	return false
}

// GetErrorCode returns the error code from an error, or ErrCodeUnknown
func GetErrorCode(err error) ErrorCode {
	if se, ok := err.(*StorageError); ok {
		return se.Code
	}
	return ErrCodeUnknown
}
