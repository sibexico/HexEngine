package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// Config holds storage engine configuration
type Config struct {
	// Buffer Pool Configuration
	BufferPoolSize uint32 `json:"buffer_pool_size"` // Number of pages in buffer pool
	CacheReplacer string `json:"cache_replacer"` // Cache replacement policy (lru, 2q, arc)
	EnablePrefetching bool `json:"enable_prefetching"` // Enable sequential prefetching

	// Disk Configuration
	DataDirectory string `json:"data_directory"` // Directory for data files
	PageSize uint32 `json:"page_size"` // Page size in bytes (default: 4096)

	// WAL Configuration
	WALDirectory string `json:"wal_directory"` // Directory for WAL files
	WALEnabled bool `json:"wal_enabled"` // Whether WAL is enabled
	WALParallel bool `json:"wal_parallel"` // Use parallel WAL for better concurrency
	WALCompression bool `json:"wal_compression"` // Enable WAL compression
	WALCompressionAlg string `json:"wal_compression_alg"` // Compression algorithm (delta, snappy, none)

	// Transaction Configuration
	MaxTransactions uint32 `json:"max_transactions"` // Maximum concurrent transactions

	// Performance Configuration
	EnableMetrics bool `json:"enable_metrics"` // Whether to collect performance metrics
	LogLevel string `json:"log_level"` // Log level (debug, info, warn, error)

	// Recovery Configuration
	AutoRecovery bool `json:"auto_recovery"` // Whether to perform recovery on startup
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		BufferPoolSize: 100,
		CacheReplacer: "2q", // Use 2Q by default for better hit rates
		EnablePrefetching: true, // Enable prefetching
		DataDirectory: "./data",
		PageSize: PageSize,
		WALDirectory: "./wal",
		WALEnabled: true,
		WALParallel: true, // Enable parallel WAL by default
		WALCompression: false, // Compression disabled by default
		WALCompressionAlg: "none", // No compression
		MaxTransactions: 1000,
		EnableMetrics: true,
		LogLevel: "info",
		AutoRecovery: true,
	}
}

// LoadConfigFromFile loads configuration from a JSON file
func LoadConfigFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig()
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// LoadConfigFromEnv loads configuration from environment variables
// Falls back to default values if environment variables are not set
func LoadConfigFromEnv() *Config {
	config := DefaultConfig()

	// Buffer Pool
	if val := os.Getenv("HEXENGINE_BUFFER_POOL_SIZE"); val != "" {
		if size, err := strconv.ParseUint(val, 10, 32); err == nil {
			config.BufferPoolSize = uint32(size)
		}
	}

	// Disk
	if val := os.Getenv("HEXENGINE_DATA_DIRECTORY"); val != "" {
		config.DataDirectory = val
	}

	if val := os.Getenv("HEXENGINE_PAGE_SIZE"); val != "" {
		if size, err := strconv.ParseUint(val, 10, 32); err == nil {
			config.PageSize = uint32(size)
		}
	}

	// WAL
	if val := os.Getenv("HEXENGINE_WAL_DIRECTORY"); val != "" {
		config.WALDirectory = val
	}

	if val := os.Getenv("HEXENGINE_WAL_ENABLED"); val != "" {
		config.WALEnabled = val == "true" || val == "1"
	}

	// Transactions
	if val := os.Getenv("HEXENGINE_MAX_TRANSACTIONS"); val != "" {
		if max, err := strconv.ParseUint(val, 10, 32); err == nil {
			config.MaxTransactions = uint32(max)
		}
	}

	// Performance
	if val := os.Getenv("HEXENGINE_ENABLE_METRICS"); val != "" {
		config.EnableMetrics = val == "true" || val == "1"
	}

	if val := os.Getenv("HEXENGINE_LOG_LEVEL"); val != "" {
		config.LogLevel = val
	}

	// Recovery
	if val := os.Getenv("HEXENGINE_AUTO_RECOVERY"); val != "" {
		config.AutoRecovery = val == "true" || val == "1"
	}

	return config
}

// SaveToFile saves the configuration to a JSON file
func (c *Config) SaveToFile(path string) error {
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.BufferPoolSize == 0 {
		return fmt.Errorf("buffer pool size must be greater than 0")
	}

	if c.PageSize == 0 {
		return fmt.Errorf("page size must be greater than 0")
	}

	if c.PageSize%512 != 0 {
		return fmt.Errorf("page size must be a multiple of 512")
	}

	if c.DataDirectory == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	if c.WALEnabled && c.WALDirectory == "" {
		return fmt.Errorf("WAL directory cannot be empty when WAL is enabled")
	}

	if c.MaxTransactions == 0 {
		return fmt.Errorf("max transactions must be greater than 0")
	}

	// Validate log level
	validLogLevels := map[string]bool{
		"debug": true,
		"info": true,
		"warn": true,
		"error": true,
	}

	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("invalid log level: %s (must be debug, info, warn, or error)", c.LogLevel)
	}

	return nil
}

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	return &Config{
		BufferPoolSize: c.BufferPoolSize,
		DataDirectory: c.DataDirectory,
		PageSize: c.PageSize,
		WALDirectory: c.WALDirectory,
		WALEnabled: c.WALEnabled,
		MaxTransactions: c.MaxTransactions,
		EnableMetrics: c.EnableMetrics,
		LogLevel: c.LogLevel,
		AutoRecovery: c.AutoRecovery,
	}
}
