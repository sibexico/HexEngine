package storage

import (
	"errors"
	"fmt"
	"testing"
)

func TestStorageError(t *testing.T) {
	err := NewStorageError(
		ErrCodePageNotFound,
		"FetchPage",
		"page not found",
		nil,
	)

	if err.Code != ErrCodePageNotFound {
		t.Errorf("Expected error code %d, got %d", ErrCodePageNotFound, err.Code)
	}

	if err.Op != "FetchPage" {
		t.Errorf("Expected op 'FetchPage', got '%s'", err.Op)
	}

	expected := "FetchPage: page not found"
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

func TestStorageErrorWithUnderlying(t *testing.T) {
	underlying := fmt.Errorf("disk read failed")
	err := NewStorageError(
		ErrCodeDiskReadFailed,
		"ReadPage",
		"failed to read page",
		underlying,
	)

	if err.Err != underlying {
		t.Error("Underlying error not set correctly")
	}

	// Test Unwrap
	unwrapped := errors.Unwrap(err)
	if unwrapped != underlying {
		t.Error("Unwrap did not return underlying error")
	}

	// Test error message includes underlying error
	expected := "ReadPage: failed to read page: disk read failed"
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}
}

func TestErrorHelpers(t *testing.T) {
	tests := []struct {
		name string
		err *StorageError
		code ErrorCode
		contains string
	}{
		{
			name: "PageNotFound",
			err: ErrPageNotFound("test", 123),
			code: ErrCodePageNotFound,
			contains: "page 123 not found",
		},
		{
			name: "PageFull",
			err: ErrPageFull("test", 456),
			code: ErrCodePageFull,
			contains: "page 456 is full",
		},
		{
			name: "NoFreePages",
			err: ErrNoFreePages("test"),
			code: ErrCodeNoFreePages,
			contains: "no free pages",
		},
		{
			name: "PagePinned",
			err: ErrPagePinned("test", 789, 3),
			code: ErrCodePagePinned,
			contains: "page 789 is pinned (pin count: 3)",
		},
		{
			name: "TxnNotFound",
			err: ErrTxnNotFound("test", 100),
			code: ErrCodeTxnNotFound,
			contains: "transaction 100 not found",
		},
		{
			name: "InvalidTxnState",
			err: ErrInvalidTxnState("test", 200, "COMMITTED"),
			code: ErrCodeInvalidTxnState,
			contains: "transaction 200 in invalid state: COMMITTED",
		},
		{
			name: "KeyNotFound",
			err: ErrKeyNotFound("test", 42),
			code: ErrCodeKeyNotFound,
			contains: "key 42 not found",
		},
		{
			name: "DuplicateKey",
			err: ErrDuplicateKey("test", 42),
			code: ErrCodeDuplicateKey,
			contains: "duplicate key 42",
		},
		{
			name: "LogCorrupted",
			err: ErrLogCorrupted("test", 1234),
			code: ErrCodeLogCorrupted,
			contains: "log corrupted at LSN 1234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code != tt.code {
				t.Errorf("Expected error code %d, got %d", tt.code, tt.err.Code)
			}

			errMsg := tt.err.Error()
			if errMsg == "" {
				t.Error("Error message should not be empty")
			}

			// Check if error message contains expected substring
			found := false
			for i := 0; i <= len(errMsg)-len(tt.contains); i++ {
				if errMsg[i:i+len(tt.contains)] == tt.contains {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Error message '%s' does not contain '%s'", errMsg, tt.contains)
			}
		})
	}
}

func TestIsErrorCode(t *testing.T) {
	err := ErrPageNotFound("test", 123)

	if !IsErrorCode(err, ErrCodePageNotFound) {
		t.Error("IsErrorCode should return true for matching code")
	}

	if IsErrorCode(err, ErrCodePageFull) {
		t.Error("IsErrorCode should return false for non-matching code")
	}

	// Test with non-StorageError
	genericErr := fmt.Errorf("generic error")
	if IsErrorCode(genericErr, ErrCodePageNotFound) {
		t.Error("IsErrorCode should return false for non-StorageError")
	}
}

func TestGetErrorCode(t *testing.T) {
	err := ErrTxnNotFound("test", 100)

	code := GetErrorCode(err)
	if code != ErrCodeTxnNotFound {
		t.Errorf("Expected error code %d, got %d", ErrCodeTxnNotFound, code)
	}

	// Test with non-StorageError
	genericErr := fmt.Errorf("generic error")
	code = GetErrorCode(genericErr)
	if code != ErrCodeUnknown {
		t.Errorf("Expected error code %d for generic error, got %d", ErrCodeUnknown, code)
	}
}

func TestErrorIs(t *testing.T) {
	err1 := ErrPageNotFound("test", 123)
	err2 := ErrPageNotFound("test", 456)

	// Different page IDs but same error code
	if !errors.Is(err1, err2) {
		t.Error("errors.Is should return true for same error code")
	}

	err3 := ErrPageFull("test", 123)
	if errors.Is(err1, err3) {
		t.Error("errors.Is should return false for different error codes")
	}
}

func TestErrorWrapping(t *testing.T) {
	baseErr := fmt.Errorf("underlying IO error")
	wrappedErr := ErrDiskOperation("WritePage", baseErr)

	// Test Unwrap
	unwrapped := errors.Unwrap(wrappedErr)
	if unwrapped != baseErr {
		t.Error("Unwrap should return the underlying error")
	}

	// Test errors.Is with wrapped error
	if !errors.Is(wrappedErr, baseErr) {
		t.Error("errors.Is should find underlying error")
	}
}

func TestErrorCodeConstants(t *testing.T) {
	// Ensure error codes are unique
	codes := map[ErrorCode]bool{
		ErrCodeUnknown: true,
		ErrCodeInternal: true,
		ErrCodePageNotFound: true,
		ErrCodePageFull: true,
		ErrCodeInvalidPageID: true,
		ErrCodePageCorrupted: true,
		ErrCodeNoFreePages: true,
		ErrCodePagePinned: true,
		ErrCodeInvalidPin: true,
		ErrCodeTxnNotFound: true,
		ErrCodeTxnAlreadyCommitted: true,
		ErrCodeTxnAlreadyAborted: true,
		ErrCodeInvalidTxnState: true,
		ErrCodeRecoveryFailed: true,
		ErrCodeLogCorrupted: true,
		ErrCodeCheckpointFailed: true,
		ErrCodeKeyNotFound: true,
		ErrCodeDuplicateKey: true,
		ErrCodeInvalidKey: true,
		ErrCodeDiskFull: true,
		ErrCodeDiskReadFailed: true,
		ErrCodeDiskWriteFailed: true,
		ErrCodeFileNotFound: true,
	}

	if len(codes) != 23 {
		t.Errorf("Expected 23 unique error codes, got %d", len(codes))
	}
}
