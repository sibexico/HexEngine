# HexEngine Usage Guide

Complete API documentation and usage examples for HexEngine storage engine.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Core Components](#core-components)
3. [Basic Operations](#basic-operations)
4. [Advanced Features](#advanced-features)
5. [Configuration](#configuration)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Installation

```bash
# Using go get
go get github.com/sibexico/HexEngine

# Or clone the repository
git clone https://github.com/sibexico/HexEngine.git
cd HexEngine
go mod download
```

### Hello World Example

```go
package main

import (
    "fmt"
    "github.com/sibexico/HexEngine/storage"
)

func main() {
    // 1. Create disk manager
    dm, err := storage.NewDiskManager("hello.db")
    if err != nil {
        panic(err)
    }
    defer dm.Close()
    
    // 2. Create buffer pool
    bpm, err := storage.NewBufferPoolManager(100, dm)
    if err != nil {
        panic(err)
    }
    
    // 3. Create a new page
    page, err := bpm.NewPage()
    if err != nil {
        panic(err)
    }
    
    // 4. Insert data
    tuple := storage.NewTuple([]byte("Hello, HexEngine!"))
    slotID, err := page.GetData().InsertTuple(tuple)
    if err != nil {
        panic(err)
    }
    
    // 5. Mark page as dirty and unpin
    page.SetDirty(true)
    bpm.UnpinPage(page.GetPageId(), true)
    
    // 6. Fetch page back
    page2, err := bpm.FetchPage(page.GetPageId())
    if err != nil {
        panic(err)
    }
    
    // 7. Read data
    retrievedTuple, err := page2.GetData().GetTuple(slotID)
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Data: %s\n", string(retrievedTuple.GetData()))
    bpm.UnpinPage(page2.GetPageId(), false)
}
```

---

## Core Components

### 1. Disk Manager

Manages persistent storage and page I/O.

#### Creation

```go
dm, err := storage.NewDiskManager("database.db")
if err != nil {
    log.Fatal(err)
}
defer dm.Close()
```

#### API Reference

```go
// Allocate a new page
pageID := dm.AllocatePage()

// Write a page to disk
err := dm.WritePage(pageID, pageData)

// Read a page from disk
pageData, err := dm.ReadPage(pageID)

// Batch write multiple pages (15-17% faster)
pages := []*Page{page1, page2, page3}
err := dm.WritePagesV(pages)

// Get database file size
size := dm.GetFileSize()
```

---

### 2. Buffer Pool Manager

In-memory cache for database pages with adaptive replacement.

#### Creation

```go
// Default (with LRU)
bpm, err := storage.NewBufferPoolManager(100, dm)

// With CLOCK-Pro (recommended, 93.5% hit rate)
bpm, err := storage.NewBufferPoolManagerWithReplacer(100, dm, "clockpro")

// With 2Q cache
bpm, err := storage.NewBufferPoolManagerWithReplacer(100, dm, "2q")
```

#### API Reference

```go
// Create a new page
page, err := bpm.NewPage()

// Fetch existing page (from cache or disk)
page, err := bpm.FetchPage(pageID)

// Unpin page when done
err := bpm.UnpinPage(pageID, isDirty)

// Flush specific page
err := bpm.FlushPage(pageID)

// Flush all dirty pages
err := bpm.FlushAllPages()

// Delete page
err := bpm.DeletePage(pageID)

// Get cache statistics
cacheHits := bpm.GetMetrics().GetCacheHits()
cacheMisses := bpm.GetMetrics().GetCacheMisses()
hitRate := bpm.GetMetrics().GetCacheHitRate()
```

#### Page Operations

```go
// Get page ID
pageID := page.GetPageId()

// Get page data (slotted page)
data := page.GetData()

// Mark as dirty
page.SetDirty(true)

// Check if dirty
isDirty := page.IsDirty()

// Pin/Unpin (manual control)
page.Pin()
page.Unpin()
```

---

### 3. Page Layout (Slotted Page)

Each page uses a slotted layout for efficient tuple storage.

#### API Reference

```go
// Get slotted page from buffer page
slotted := page.GetData()

// Insert a tuple
tuple := storage.NewTuple([]byte("my data"))
slotID, err := slotted.InsertTuple(tuple)

// Get a tuple
tuple, err := slotted.GetTuple(slotID)
data := tuple.GetData()

// Update a tuple (in-place if space allows)
newTuple := storage.NewTuple([]byte("updated data"))
err := slotted.UpdateTuple(slotID, newTuple)

// Delete a tuple
err := slotted.DeleteTuple(slotID)

// Get page statistics
tupleCount := slotted.GetTupleCount()
freeSpace := slotted.GetFreeSpace()
```

---

### 4. Transaction Manager

ACID transactions with MVCC and snapshot isolation.

#### Creation

```go
// Create log manager first
lm, err := storage.NewLogManager("wal.log")
if err != nil {
    log.Fatal(err)
}
defer lm.Close()

// Create transaction manager
tm := storage.NewTransactionManager(lm)
defer tm.Close() // Flushes pending commits
```

#### Basic Transaction API

```go
// Begin transaction
txn := tm.Begin()
txnID := txn.GetTxnID()

// ... perform operations ...

// Commit transaction
err := tm.Commit(txnID)

// Or abort
err := tm.Abort(txnID)

// Get transaction state
txn, exists := tm.GetTransaction(txnID)
if exists {
    state := txn.GetState() // RUNNING, COMMITTED, ABORTED
}
```

#### Group Commit (Automatic)

Group commit is **automatically enabled** and batches multiple commits together for better throughput.

```go
// Just commit normally - batching happens automatically!
for i := 0; i < 100; i++ {
    txn := tm.Begin()
    // ... do work ...
    tm.Commit(txn.GetTxnID())
}

// Check group commit effectiveness
stats := tm.GetGroupCommitStats()
fmt.Printf("Average commits per fsync: %.2f\n", stats.AvgCommitsPerFsync)
fmt.Printf("Total fsyncs avoided: %d\n", stats.TotalFsyncs - stats.TotalCommits)
```

#### MVCC Snapshot Isolation

```go
// Transaction automatically gets snapshot
txn := tm.Begin()
startTS := txn.GetStartTS()

// Read operations see snapshot as of startTS
// Writes are isolated until commit

// Visibility check (automatic in queries)
isVisible := txn.IsVisible(rowTimestamp)
```

---

### 5. B+ Tree Index

Persistent B+ tree index for efficient key-value storage and range queries.

#### Creation

```go
tree, err := storage.NewBPlusTree(bpm)
if err != nil {
    log.Fatal(err)
}

// With custom order
tree, err := storage.NewBPlusTreeWithOrder(bpm, 8) // order=8
```

#### API Reference

```go
// Insert key-value pair
err := tree.Insert(42, 420)

// Search for key
value, found, err := tree.Search(42)
if found {
    fmt.Printf("Value: %d\n", value)
}

// Delete key
err := tree.Delete(42)

// Range scan (iterate all keys in order)
iter, err := tree.Iterator()
if err != nil {
    log.Fatal(err)
}

for iter.HasNext() {
    key, value, err := iter.Next()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Key: %d, Value: %d\n", key, value)
}

// Parallel range scan (22.1x speedup!)
parallelTree := storage.NewParallelBPlusTree(tree, 4) // 4 workers
results, err := parallelTree.ParallelScan(startKey, endKey)
```

---

### 6. Skip List Index

Lock-free skip list for concurrent indexing.

#### Creation

```go
config := storage.DefaultSkipListConfig()
config.MaxLevel = 16
skiplist := storage.NewLockFreeSkipList(config)
```

#### API Reference

```go
// Insert (937K ops/sec)
err := skiplist.Insert([]byte("key"), []byte("value"))

// Search (1.14M ops/sec)
value, found := skiplist.Search([]byte("key"))

// Delete
err := skiplist.Delete([]byte("key"))

// Range scan
results := skiplist.RangeScan([]byte("start"), []byte("end"))
for _, result := range results {
    fmt.Printf("%s -> %s\n", result.Key, result.Value)
}
```

---

### 7. Write-Ahead Log (WAL)

Durable logging for ACID transactions and crash recovery.

#### Creation

```go
lm, err := storage.NewLogManager("wal.log")
if err != nil {
    log.Fatal(err)
}
defer lm.Close()

// With compression (4.2x compression ratio)
lm, err := storage.NewLogManagerWithCompression("wal.log", true)
```

#### API Reference

```go
// Append log record
record := &storage.LogRecord{
    Type:  storage.LogUpdate,
    TxnID: txnID,
    PageID: pageID,
    // ... other fields ...
}
lsn, err := lm.AppendLog(record)

// Flush to disk (durability!)
err := lm.Flush()

// Read log record
record, err := lm.ReadLog(lsn)

// Iterate all logs
lm.Scan(func(record *LogRecord) error {
    // Process each record
    return nil
})
```

#### Parallel WAL (Higher Throughput)

```go
// For high-concurrency workloads
plm := storage.NewParallelLogManager(dm, "wal.log")
defer plm.Close()

// Same API, but 41% faster appends
lsn, err := plm.AppendLog(record)
```

---

### 8. Recovery Manager

ARIES-based crash recovery with parallel replay.

#### API Reference

```go
// Create recovery manager
rm := storage.NewRecoveryManager(lm, bpm)

// Perform recovery after crash
err := rm.Recover()
if err != nil {
    log.Fatal(err)
}

// Parallel recovery (1.48x faster)
prm := storage.NewParallelRecoveryManager(lm, bpm, 4) // 4 workers
err := prm.Recover()
```

---

## Advanced Features

### 1. Adaptive Prefetching

ML-based sequential access detection with 96% accuracy.

#### Configuration

```go
prefetcher := storage.NewPrefetcher(bpm)

// Configure detection threshold and prefetch distance
prefetcher.Configure(
    3,  // Trigger after 3 sequential accesses
    8,  // Prefetch 8 pages ahead
)

// Record accesses (integrate in your scan logic)
for pageID := range scanRange {
    prefetcher.RecordAccess(txnID, pageID)
    page, _ := bpm.FetchPage(pageID)
    // ... use page ...
}

// Check effectiveness
stats := prefetcher.GetStats()
fmt.Printf("Patterns detected: %d\n", stats.PatternsDetected)
fmt.Printf("Prefetch accuracy: %.2f%%\n", stats.Accuracy * 100)
```

---

### 2. Bloom Filters

Probabilistic filters for fast negative lookups (73ns, 50-90% read reduction).

#### API Reference

```go
// Create bloom filter
bf := storage.NewBloomFilter(10000, 0.01) // 10K items, 1% FPR

// Add keys
bf.Add([]byte("key1"))
bf.Add([]byte("key2"))

// Check membership (73ns avg)
if bf.MightContain([]byte("key1")) {
    // Probably exists - do actual lookup
    value, _ := actualLookup("key1")
} else {
    // Definitely doesn't exist - skip lookup!
}

// Get statistics
fmt.Printf("False positive rate: %.4f%%\n", bf.FalsePositiveRate())
```

---

### 3. Compression

LZ4 compression for WAL and pages.

#### WAL Compression (4.2x ratio)

```go
// Enable during creation
lm, _ := storage.NewLogManagerWithCompression("wal.log", true)

// Compression is transparent - same API
lsn, _ := lm.AppendLog(record)
```

#### Page Compression (3.2x space savings)

```go
// Compress a page
compressor := storage.NewLZ4Compressor()
compressed, ratio, err := compressor.CompressPage(page)
fmt.Printf("Compression ratio: %.2fx\n", ratio)

// Decompress
decompressed, err := compressor.DecompressPage(compressed)
```

---

### 4. Parallel Operations

#### Parallel B+ Tree Scan (22.1x speedup)

```go
tree, _ := storage.NewBPlusTree(bpm)

// Create parallel wrapper with 4 workers
parallelTree := storage.NewParallelBPlusTree(tree, 4)

// Scan range in parallel (2.21M keys/sec)
results, err := parallelTree.ParallelScan(startKey, endKey)
for _, kv := range results {
    fmt.Printf("%d -> %d\n", kv.Key, kv.Value)
}
```

#### Parallel Eviction (3.76x throughput)

```go
// Automatically enabled in buffer pool
// No configuration needed - runs in background
```

---

## Configuration

### Complete Configuration Example

```go
type Config struct {
    // Buffer Pool
    BufferPoolSize     uint32        // Number of pages (default: 1000)
    CacheReplacer      string        // "lru", "2q", "clockpro" (default: "clockpro")
    
    // Prefetching
    EnablePrefetch     bool          // Enable adaptive prefetch (default: true)
    PrefetchThreshold  int           // Sequential accesses to trigger (default: 3)
    PrefetchDistance   int           // Pages to prefetch ahead (default: 8)
    
    // Compression
    EnableWALCompression  bool       // Compress WAL (default: true)
    EnablePageCompression bool       // Compress pages (default: false)
    CompressionLevel      int        // LZ4 level (default: 1)
    
    // Transactions
    GroupCommitTimeout    time.Duration // Max wait time (default: 10ms)
    ParallelWorkers       int           // Workers for parallel ops (default: 4)
    
    // Recovery
    CheckpointInterval    time.Duration // Checkpoint frequency (default: 1min)
}

// Create with config
dm, _ := storage.NewDiskManager("mydb.db")
config := storage.Config{
    BufferPoolSize:        2000,
    CacheReplacer:         "clockpro",
    EnablePrefetch:        true,
    EnableWALCompression:  true,
    GroupCommitTimeout:    5 * time.Millisecond,
    ParallelWorkers:       8,
}
bpm, _ := storage.NewBufferPoolManagerWithConfig(dm, config)
```

### Tuning Guidelines

#### For OLTP Workloads
```go
config := storage.Config{
    BufferPoolSize:       1000,      // Moderate cache
    CacheReplacer:        "clockpro", // Best hit rate
    EnablePrefetch:       false,     // Not needed for random access
    GroupCommitTimeout:   10ms,      // Good latency/throughput balance
}
```

#### For OLAP Workloads
```go
config := storage.Config{
    BufferPoolSize:       5000,      // Large cache
    CacheReplacer:        "2q",      // Good for scans
    EnablePrefetch:       true,      // Critical for sequential scans
    PrefetchDistance:     16,        // Aggressive prefetching
    ParallelWorkers:      8,         // Utilize more cores
}
```

#### For Mixed Workloads
```go
config := storage.Config{
    BufferPoolSize:       2000,      // Balanced
    CacheReplacer:        "clockpro", // Adaptive
    EnablePrefetch:       true,      // Help with scans
    PrefetchDistance:     8,         // Moderate prefetching
    GroupCommitTimeout:   5ms,       // Optimize for throughput
}
```

---

## Best Practices

### 1. Always Close Resources

```go
dm, _ := storage.NewDiskManager("db.db")
defer dm.Close() // Always defer Close()

lm, _ := storage.NewLogManager("wal.log")
defer lm.Close()

tm := storage.NewTransactionManager(lm)
defer tm.Close() // Flushes pending commits!
```

### 2. Pin/Unpin Pages Correctly

```go
// Bad: Forget to unpin
page, _ := bpm.FetchPage(pageID)
// ... use page ...
// Missing unpin! Page stays pinned forever

// Good: Always unpin
page, _ := bpm.FetchPage(pageID)
defer bpm.UnpinPage(pageID, false)
// ... use page ...
```

### 3. Use Transactions for ACID

```go
// Bad: Direct modification without transaction
page, _ := bpm.FetchPage(pageID)
page.GetData().InsertTuple(tuple)
page.SetDirty(true)
// No atomicity or durability!

// Good: Use transactions
txn := tm.Begin()
page, _ := bpm.FetchPage(pageID)
page.GetData().InsertTuple(tuple)
page.SetDirty(true)
bpm.UnpinPage(pageID, true)
tm.Commit(txn.GetTxnID())
// Atomic and durable!
```

### 4. Check Error Returns

```go
// Bad: Ignore errors
page, _ := bpm.FetchPage(pageID)

// Good: Handle errors
page, err := bpm.FetchPage(pageID)
if err != nil {
    log.Printf("Failed to fetch page: %v", err)
    return err
}
```

### 5. Monitor Performance

```go
// Get metrics periodically
metrics := bpm.GetMetrics()
fmt.Printf("Cache hit rate: %.2f%%\n", metrics.GetCacheHitRate() * 100)
fmt.Printf("Cache hits: %d, misses: %d\n", 
    metrics.GetCacheHits(), 
    metrics.GetCacheMisses())

// Group commit stats
stats := tm.GetGroupCommitStats()
fmt.Printf("Commits per fsync: %.2f\n", stats.AvgCommitsPerFsync)
```

---

## Troubleshooting

### Low Cache Hit Rate

```go
// Check hit rate
hitRate := bpm.GetMetrics().GetCacheHitRate()
if hitRate < 0.8 {
    // Solutions:
    // 1. Increase buffer pool size
    // 2. Switch to CLOCK-Pro: "clockpro"
    // 3. Enable prefetching for scans
    // 4. Analyze access patterns
}
```

### High Transaction Latency

```go
// Check group commit stats
stats := tm.GetGroupCommitStats()
if stats.AvgCommitsPerFsync < 2.0 {
    // Low batching - try:
    // 1. Increase GroupCommitTimeout
    // 2. Increase concurrency
    // 3. Use async commit (if durability OK)
}
```

### Out of Memory

```go
// Ensure pages are unpinned
// Check pin counts
for pageID := range pages {
    page, _ := bpm.FetchPage(pageID)
    if page.GetPinCount() > 0 {
        log.Printf("Page %d is still pinned!", pageID)
    }
}

// Flush and shrink buffer pool
bpm.FlushAllPages()
```

### Slow Recovery

```go
// Use parallel recovery
prm := storage.NewParallelRecoveryManager(lm, bpm, 8)
err := prm.Recover() // 1.48x faster

// Or reduce checkpoint interval
config.CheckpointInterval = 30 * time.Second
```

---

## Examples

### Complete OLTP Application

```go
package main

import (
    "fmt"
    "github.com/sibexico/HexEngine/storage"
)

func main() {
    // Initialize
    dm, _ := storage.NewDiskManager("oltp.db")
    defer dm.Close()
    
    bpm, _ := storage.NewBufferPoolManagerWithReplacer(1000, dm, "clockpro")
    lm, _ := storage.NewLogManager("oltp.log")
    defer lm.Close()
    
    tm := storage.NewTransactionManager(lm)
    defer tm.Close()
    
    // Run transactions
    for i := 0; i < 1000; i++ {
        txn := tm.Begin()
        
        // Read
        page, _ := bpm.FetchPage(uint32(i % 100))
        data := page.GetData()
        
        // Modify
        tuple := storage.NewTuple([]byte(fmt.Sprintf("Record %d", i)))
        data.InsertTuple(tuple)
        page.SetDirty(true)
        
        bpm.UnpinPage(page.GetPageId(), true)
        
        // Commit (automatically batched)
        tm.Commit(txn.GetTxnID())
    }
    
    // Check performance
    stats := tm.GetGroupCommitStats()
    fmt.Printf("Throughput: %.0f TPS\n", 
        float64(stats.TotalCommits) / stats.ElapsedTime.Seconds())
    fmt.Printf("Group commit efficiency: %.2f\n", stats.AvgCommitsPerFsync)
}
```

### Analytical Query with Parallel Scan

```go
package main

import (
    "fmt"
    "github.com/sibexico/HexEngine/storage"
)

func main() {
    dm, _ := storage.NewDiskManager("analytics.db")
    defer dm.Close()
    
    // Large buffer pool for analytics
    bpm, _ := storage.NewBufferPoolManagerWithReplacer(5000, dm, "2q")
    
    // Enable prefetching
    prefetcher := storage.NewPrefetcher(bpm)
    prefetcher.Configure(3, 16) // Aggressive prefetching
    
    // Create parallel B+ tree
    tree, _ := storage.NewBPlusTree(bpm)
    parallelTree := storage.NewParallelBPlusTree(tree, 8) // 8 workers
    
    // Parallel range scan (22.1x speedup!)
    results, _ := parallelTree.ParallelScan(0, 1000000)
    
    sum := int64(0)
    for _, kv := range results {
        sum += kv.Value
    }
    
    fmt.Printf("Scanned %d records, sum=%d\n", len(results), sum)
    
    // Check prefetcher effectiveness
    stats := prefetcher.GetStats()
    fmt.Printf("Prefetch accuracy: %.2f%%\n", stats.Accuracy * 100)
}
```

---

## API Summary

### Core Components

| Component | Creation | Key Methods |
|-----------|----------|-------------|
| **DiskManager** | `NewDiskManager(file)` | `WritePage()`, `ReadPage()`, `AllocatePage()` |
| **BufferPoolManager** | `NewBufferPoolManager(size, dm)` | `FetchPage()`, `NewPage()`, `UnpinPage()` |
| **TransactionManager** | `NewTransactionManager(lm)` | `Begin()`, `Commit()`, `Abort()` |
| **LogManager** | `NewLogManager(file)` | `AppendLog()`, `Flush()`, `ReadLog()` |
| **BPlusTree** | `NewBPlusTree(bpm)` | `Insert()`, `Search()`, `Delete()`, `Iterator()` |

### Performance Metrics

- **Concurrent TPS**: 2,100 transactions/sec
- **Lock-Free Ops**: 4.2M operations/sec
- **Cache Hit Rate**: 93.5% (CLOCK-Pro)
- **Parallel Scan**: 22.1x speedup
- **Recovery Time**: 3.0 seconds

---

**For performance benchmarks, see [PERFORMANCE.md](PERFORMANCE.md)**

**For project overview, see [README.md](README.md)**

---

*HexEngine - High-Performance Storage Engine*  
*Complete API Documentation*  
*Version 2.9.0*
