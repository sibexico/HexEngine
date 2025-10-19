package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveFlusher dynamically adjusts dirty page flushing based on workload
//
// Design:
// - Monitors dirty page ratio, write rate, and buffer pool pressure
// - Adjusts flush rate to prevent checkpoint stalls
// - Uses PID controller for smooth adjustment
// - Background goroutine performs adaptive flushing
//
// Goals:
// 1. Prevent sudden checkpoint storms
// 2. Maintain dirty ratio below target threshold
// 3. Adapt to workload changes
// 4. Minimize I/O impact on foreground transactions

// FlushableBufferPool is the interface required by adaptive flusher
type FlushableBufferPool interface {
	GetDirtyPageCount() int
	GetCapacity() int
	GetDirtyPages(maxPages int) []uint32
	FlushPage(pageID uint32) error
}

type AdaptiveFlusher struct {
	// Buffer pool reference
	bufferPool FlushableBufferPool

	// Configuration
	config AdaptiveFlushConfig

	// State (atomic)
	running atomic.Bool
	flushesIssued atomic.Uint64
	pagesFlushed atomic.Uint64

	// PID controller state (protected by mutex)
	mu sync.Mutex
	integral float64
	lastError float64
	lastFlushRate float64

	// Statistics
	stats AdaptiveFlushStats

	// Control
	stopCh chan struct{}
	doneCh chan struct{}
}

// AdaptiveFlushConfig contains configuration for adaptive flushing
type AdaptiveFlushConfig struct {
	// Target dirty page ratio (0.0 - 1.0)
	TargetDirtyRatio float64

	// Maximum dirty ratio before aggressive flushing (0.0 - 1.0)
	MaxDirtyRatio float64

	// Flush check interval
	CheckInterval time.Duration

	// Minimum pages to flush per interval
	MinFlushPages int

	// Maximum pages to flush per interval
	MaxFlushPages int

	// PID controller gains
	Kp float64 // Proportional gain
	Ki float64 // Integral gain
	Kd float64 // Derivative gain

	// Adaptive parameters
	EnableAdaptive bool // Enable adaptive adjustment
	WriteRateThreshold float64 // Pages/sec to trigger adaptive mode
	CheckpointInterval time.Duration
}

// AdaptiveFlushStats contains statistics about adaptive flushing
type AdaptiveFlushStats struct {
	FlushesIssued uint64
	PagesFlushed uint64
	CurrentRate float64 // Pages per second
	DirtyRatio float64
	AvgFlushTime time.Duration
	LastAdjustment time.Time
}

// DefaultAdaptiveFlushConfig returns default configuration
func DefaultAdaptiveFlushConfig() AdaptiveFlushConfig {
	return AdaptiveFlushConfig{
		TargetDirtyRatio: 0.60, // Target 60% dirty
		MaxDirtyRatio: 0.80, // Max 80% before aggressive flush
		CheckInterval: 100 * time.Millisecond,
		MinFlushPages: 10,
		MaxFlushPages: 100,
		Kp: 2.0, // Proportional gain
		Ki: 0.5, // Integral gain
		Kd: 0.1, // Derivative gain
		EnableAdaptive: true,
		WriteRateThreshold: 100.0, // pages/sec
		CheckpointInterval: 30 * time.Second,
	}
}

// NewAdaptiveFlusher creates a new adaptive flusher
func NewAdaptiveFlusher(bp FlushableBufferPool, config AdaptiveFlushConfig) *AdaptiveFlusher {
	// Validate configuration
	if config.TargetDirtyRatio <= 0 || config.TargetDirtyRatio >= 1 {
		config.TargetDirtyRatio = 0.60
	}
	if config.MaxDirtyRatio <= config.TargetDirtyRatio || config.MaxDirtyRatio >= 1 {
		config.MaxDirtyRatio = 0.80
	}
	if config.CheckInterval < 10*time.Millisecond {
		config.CheckInterval = 100 * time.Millisecond
	}

	af := &AdaptiveFlusher{
		bufferPool: bp,
		config: config,
		lastFlushRate: float64(config.MinFlushPages),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	return af
}

// Start starts the adaptive flusher background goroutine
func (af *AdaptiveFlusher) Start() error {
	if af.running.Load() {
		return fmt.Errorf("adaptive flusher already running")
	}

	af.running.Store(true)
	go af.flushLoop()

	return nil
}

// Stop stops the adaptive flusher
func (af *AdaptiveFlusher) Stop() error {
	if !af.running.Load() {
		return nil
	}

	close(af.stopCh)
	<-af.doneCh
	af.running.Store(false)

	return nil
}

// flushLoop is the main background loop
func (af *AdaptiveFlusher) flushLoop() {
	defer close(af.doneCh)

	ticker := time.NewTicker(af.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-af.stopCh:
			return
		case <-ticker.C:
			af.performAdaptiveFlush()
		}
	}
}

// performAdaptiveFlush performs one iteration of adaptive flushing
func (af *AdaptiveFlusher) performAdaptiveFlush() {
	// Get current buffer pool state
	dirtyCount := af.bufferPool.GetDirtyPageCount()
	totalPages := af.bufferPool.GetCapacity()

	if totalPages == 0 {
		return // Empty buffer pool
	}

	dirtyRatio := float64(dirtyCount) / float64(totalPages)

	// Calculate error: how far we are from target
	error := dirtyRatio - af.config.TargetDirtyRatio

	// Determine flush rate using PID controller
	flushPages := af.calculateFlushRate(error, dirtyRatio)

	// Perform flush if needed
	if flushPages > 0 {
		start := time.Now()
		flushed := af.flushDirtyPages(flushPages)
		elapsed := time.Since(start)

		// Update statistics
		af.flushesIssued.Add(1)
		af.pagesFlushed.Add(uint64(flushed))

		af.mu.Lock()
		af.stats.FlushesIssued = af.flushesIssued.Load()
		af.stats.PagesFlushed = af.pagesFlushed.Load()
		af.stats.CurrentRate = af.lastFlushRate
		af.stats.DirtyRatio = dirtyRatio
		af.stats.LastAdjustment = time.Now()

		// Update average flush time (exponential moving average)
		if af.stats.AvgFlushTime == 0 {
			af.stats.AvgFlushTime = elapsed
		} else {
			af.stats.AvgFlushTime = time.Duration(
				0.9*float64(af.stats.AvgFlushTime) + 0.1*float64(elapsed),
			)
		}
		af.mu.Unlock()
	}
}

// calculateFlushRate uses PID controller to determine flush rate
func (af *AdaptiveFlusher) calculateFlushRate(error float64, dirtyRatio float64) int {
	af.mu.Lock()
	defer af.mu.Unlock()

	// PID controller:
	// output = Kp * error + Ki * integral + Kd * derivative

	// Update integral (with anti-windup)
	af.integral += error
	maxIntegral := 10.0
	if af.integral > maxIntegral {
		af.integral = maxIntegral
	} else if af.integral < -maxIntegral {
		af.integral = -maxIntegral
	}

	// Calculate derivative
	derivative := error - af.lastError
	af.lastError = error

	// PID output
	pidOutput := af.config.Kp*error + af.config.Ki*af.integral + af.config.Kd*derivative

	// Aggressive mode if dirty ratio exceeds max
	if dirtyRatio >= af.config.MaxDirtyRatio {
		pidOutput = float64(af.config.MaxFlushPages)
	}

	// Convert to flush page count
	baseRate := float64(af.config.MinFlushPages)
	flushRate := baseRate + pidOutput*float64(af.config.MaxFlushPages-af.config.MinFlushPages)

	// Clamp to valid range
	if flushRate < float64(af.config.MinFlushPages) {
		flushRate = float64(af.config.MinFlushPages)
	} else if flushRate > float64(af.config.MaxFlushPages) {
		flushRate = float64(af.config.MaxFlushPages)
	}

	// Only flush if dirty ratio exceeds target
	if dirtyRatio < af.config.TargetDirtyRatio {
		flushRate = 0
	}

	af.lastFlushRate = flushRate

	return int(flushRate)
}

// flushDirtyPages flushes up to maxPages dirty pages
func (af *AdaptiveFlusher) flushDirtyPages(maxPages int) int {
	flushed := 0

	// Get dirty pages from buffer pool
	dirtyPages := af.bufferPool.GetDirtyPages(maxPages)

	for _, pageID := range dirtyPages {
		// Flush the page
		err := af.bufferPool.FlushPage(pageID)
		if err == nil {
			flushed++
		}

		if flushed >= maxPages {
			break
		}
	}

	return flushed
}

// GetStats returns current statistics
func (af *AdaptiveFlusher) GetStats() AdaptiveFlushStats {
	af.mu.Lock()
	defer af.mu.Unlock()
	return af.stats
}

// SetTargetDirtyRatio dynamically adjusts the target dirty ratio
func (af *AdaptiveFlusher) SetTargetDirtyRatio(ratio float64) error {
	if ratio <= 0 || ratio >= 1 {
		return fmt.Errorf("invalid dirty ratio: %f (must be between 0 and 1)", ratio)
	}

	af.mu.Lock()
	defer af.mu.Unlock()

	if ratio >= af.config.MaxDirtyRatio {
		return fmt.Errorf("target ratio %f must be less than max ratio %f",
			ratio, af.config.MaxDirtyRatio)
	}

	af.config.TargetDirtyRatio = ratio
	return nil
}

// SetMaxDirtyRatio dynamically adjusts the maximum dirty ratio
func (af *AdaptiveFlusher) SetMaxDirtyRatio(ratio float64) error {
	if ratio <= 0 || ratio >= 1 {
		return fmt.Errorf("invalid max dirty ratio: %f (must be between 0 and 1)", ratio)
	}

	af.mu.Lock()
	defer af.mu.Unlock()

	if ratio <= af.config.TargetDirtyRatio {
		return fmt.Errorf("max ratio %f must be greater than target ratio %f",
			ratio, af.config.TargetDirtyRatio)
	}

	af.config.MaxDirtyRatio = ratio
	return nil
}

// TriggerFlush manually triggers a flush cycle
func (af *AdaptiveFlusher) TriggerFlush(maxPages int) int {
	if maxPages <= 0 {
		maxPages = af.config.MaxFlushPages
	}
	return af.flushDirtyPages(maxPages)
}

// IsRunning returns whether the flusher is currently running
func (af *AdaptiveFlusher) IsRunning() bool {
	return af.running.Load()
}

// GetConfig returns the current configuration
func (af *AdaptiveFlusher) GetConfig() AdaptiveFlushConfig {
	af.mu.Lock()
	defer af.mu.Unlock()
	return af.config
}
