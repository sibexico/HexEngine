package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.BufferPoolSize != 100 {
		t.Errorf("Expected buffer pool size 100, got %d", config.BufferPoolSize)
	}

	if config.PageSize != PageSize {
		t.Errorf("Expected page size %d, got %d", PageSize, config.PageSize)
	}

	if !config.WALEnabled {
		t.Error("Expected WAL to be enabled by default")
	}

	if !config.EnableMetrics {
		t.Error("Expected metrics to be enabled by default")
	}

	if config.LogLevel != "info" {
		t.Errorf("Expected log level 'info', got '%s'", config.LogLevel)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		config *Config
		expectError bool
	}{
		{
			name: "valid config",
			config: DefaultConfig(),
			expectError: false,
		},
		{
			name: "zero buffer pool size",
			config: &Config{
				BufferPoolSize: 0,
				PageSize: 4096,
				DataDirectory: "./data",
			},
			expectError: true,
		},
		{
			name: "zero page size",
			config: &Config{
				BufferPoolSize: 100,
				PageSize: 0,
				DataDirectory: "./data",
			},
			expectError: true,
		},
		{
			name: "invalid page size",
			config: &Config{
				BufferPoolSize: 100,
				PageSize: 4000, // Not a multiple of 512
				DataDirectory: "./data",
			},
			expectError: true,
		},
		{
			name: "empty data directory",
			config: &Config{
				BufferPoolSize: 100,
				PageSize: 4096,
				DataDirectory: "",
			},
			expectError: true,
		},
		{
			name: "WAL enabled but no directory",
			config: &Config{
				BufferPoolSize: 100,
				PageSize: 4096,
				DataDirectory: "./data",
				WALEnabled: true,
				WALDirectory: "",
			},
			expectError: true,
		},
		{
			name: "invalid log level",
			config: &Config{
				BufferPoolSize: 100,
				PageSize: 4096,
				DataDirectory: "./data",
				WALDirectory: "./wal",
				MaxTransactions: 1000,
				LogLevel: "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestConfigSaveAndLoad(t *testing.T) {
	// Create temp directory for test
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "config.json")

	// Create and save config
	originalConfig := DefaultConfig()
	originalConfig.BufferPoolSize = 200
	originalConfig.LogLevel = "debug"

	err := originalConfig.SaveToFile(configPath)
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Load config back
	loadedConfig, err := LoadConfigFromFile(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify values
	if loadedConfig.BufferPoolSize != 200 {
		t.Errorf("Expected buffer pool size 200, got %d", loadedConfig.BufferPoolSize)
	}

	if loadedConfig.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", loadedConfig.LogLevel)
	}
}

func TestLoadConfigFromInvalidFile(t *testing.T) {
	_, err := LoadConfigFromFile("/nonexistent/config.json")
	if err == nil {
		t.Error("Expected error when loading nonexistent file")
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// Save original env vars
	originalVars := map[string]string{
		"HEXENGINE_BUFFER_POOL_SIZE": os.Getenv("HEXENGINE_BUFFER_POOL_SIZE"),
		"HEXENGINE_WAL_ENABLED": os.Getenv("HEXENGINE_WAL_ENABLED"),
		"HEXENGINE_LOG_LEVEL": os.Getenv("HEXENGINE_LOG_LEVEL"),
	}

	// Clean up env vars after test
	defer func() {
		for key, val := range originalVars {
			if val == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, val)
			}
		}
	}()

	// Set test env vars
	os.Setenv("HEXENGINE_BUFFER_POOL_SIZE", "500")
	os.Setenv("HEXENGINE_WAL_ENABLED", "false")
	os.Setenv("HEXENGINE_LOG_LEVEL", "debug")

	// Load config from env
	config := LoadConfigFromEnv()

	if config.BufferPoolSize != 500 {
		t.Errorf("Expected buffer pool size 500, got %d", config.BufferPoolSize)
	}

	if config.WALEnabled {
		t.Error("Expected WAL to be disabled")
	}

	if config.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", config.LogLevel)
	}
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.BufferPoolSize = 500
	original.LogLevel = "debug"

	clone := original.Clone()

	// Verify values match
	if clone.BufferPoolSize != original.BufferPoolSize {
		t.Errorf("Clone buffer pool size mismatch: got %d, want %d",
			clone.BufferPoolSize, original.BufferPoolSize)
	}

	if clone.LogLevel != original.LogLevel {
		t.Errorf("Clone log level mismatch: got %s, want %s",
			clone.LogLevel, original.LogLevel)
	}

	// Modify clone and verify original unchanged
	clone.BufferPoolSize = 1000

	if original.BufferPoolSize == 1000 {
		t.Error("Modifying clone should not affect original")
	}
}

func TestEnvVarBooleanParsing(t *testing.T) {
	tests := []struct {
		name string
		value string
		expected bool
	}{
		{"true string", "true", true},
		{"1 string", "1", true},
		{"false string", "false", false},
		{"0 string", "0", false},
		{"other string", "other", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("HEXENGINE_WAL_ENABLED", tt.value)
			defer os.Unsetenv("HEXENGINE_WAL_ENABLED")

			config := LoadConfigFromEnv()
			if config.WALEnabled != tt.expected {
				t.Errorf("Expected WALEnabled=%v for value '%s', got %v",
					tt.expected, tt.value, config.WALEnabled)
			}
		})
	}
}
