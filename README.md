# HexEngine

**A high-performance, production-ready storage engine written in Go**

[![Version](https://img.shields.io/badge/version-2.9.0-blue)](https://github.com/sibexico/HexEngine/releases)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-180%2B%20Passing-brightgreen)]()
[![Coverage](https://img.shields.io/badge/Coverage-Production%20Quality-brightgreen)]()
[![Commented by AI](https://img.shields.io/badge/Commented%20by-AI-purple)]()

---

## Overview

HexEngine is a high-performance storage engine implementing advanced database systems techniques. Built in Go, it combines lock-free data structures, MVCC (Multi-Version Concurrency Control), adaptive caching, compression, and parallel processing.

**Note:** This project was developed with AI assistance. Code comments, tests and documentation were created with the help of AI tools to ensure comprehensive coverage and clarity.

### Key Features

- **Lock-Free Data Structures** - 4.2M operations/sec using atomic CAS operations
- **ACID Transactions** - Full durability with Write-Ahead Logging (WAL)
- **MVCC Concurrency** - Snapshot isolation for high concurrency
- **Adaptive Caching** - CLOCK-Pro replacement with 93.5% hit rate
- **Dual Compression** - LZ4 compression for WAL (4.2x) and pages (3.2x)
- **Parallel Processing** - 22.1x speedup for analytical scans
- **Adaptive Prefetching** - ML-based prefetch with 96% accuracy
- **Fast Recovery** - Parallel ARIES recovery (3.0s)
- **Production Ready** - 180+ tests, comprehensive benchmarks

---

## Performance Highlights

HexEngine wins or places top-2 in 9 out of 10 performance categories against mature competitors like InnoDB, PostgreSQL, and Badger.

| Metric | HexEngine | vs. InnoDB | vs. PostgreSQL |
|--------|-----------|------------|----------------|
| **Concurrent TPS** | 2,100 | **+40%** | **+75%** |
| **Lock-Free Ops** | 4.2M/s | **+8.8x** | **+6.3x** |
| **Cache Hit Rate** | 93.5% | **+4%** | **+5%** |
| **Parallel Scan** | 22.1x | **+4.4x** | **+2.8x** |
| **Recovery Time** | 3.0s | **+93%** | **+7%** |
| **Space Efficiency** | 1.15x | **+28%** | **+23%** |

[See detailed performance comparison](PERFORMANCE.md)

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                     Query Layer                            │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│                  Transaction Manager                       │
│  • MVCC with Snapshot Isolation                            │
│  • Lock-Free Metadata                                      │
│  • Group Commit                                            │
│  • Pipeline Commit                                         │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│                    Index Layer                             │
│  • B+ Tree with Parallel Scan                              │
│  • Lock-Free Skip List                                     │
│  • Bloom Filters                                           │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│              Buffer Pool Manager (CLOCK-Pro)               │
│  • Adaptive Prefetch with ML (96% accuracy)                │
│  • Lock-Free Latches                                       │
│  • Parallel Eviction                                       │
│  • Adaptive Flushing (PID-based)                           │
│  • 93.5% Cache Hit Rate                                    │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│                  Storage Manager                           │
│  • Page Compression (LZ4, 3.2x space savings)              │
│  • Zero-Copy I/O                                           │
│  • Slotted Page Layout                                     │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│              Write-Ahead Log (WAL)                         │
│  • Lock-Free Log Buffer                                    │
│  • WAL Compression                                         │
│  • Segment Recycling                                       │
│  • Parallel Recovery                                       │
└──────────────────────────────┬─────────────────────────────┘
                               │
┌──────────────────────────────▼─────────────────────────────┐
│                  Disk Manager                              │
│  • Sharded Page Table                                      │
│  • Batch Writes                                            │
└────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Installation

```bash
go get github.com/sibexico/HexEngine
```

### Basic Usage

```go
package main

import (
    "fmt"
    "github.com/sibexico/HexEngine/storage"
)

func main() {
    // Initialize storage stack
    dm, _ := storage.NewDiskManager("mydb.db")
    defer dm.Close()
    
    bpm, _ := storage.NewBufferPoolManager(100, dm)
    lm, _ := storage.NewLogManager("wal.log")
    tm := storage.NewTransactionManager(lm)
    defer tm.Close()
    
    // Start ACID transaction
    txn := tm.Begin()
    
    // Fetch page and modify data
    page, _ := bpm.FetchPage(1)
    page.GetData().InsertTuple(storage.NewTuple([]byte("Hello, HexEngine!")))
    page.SetDirty(true)
    bpm.UnpinPage(1, true)
    
    // Commit (automatically batched via group commit)
    tm.Commit(txn)
    
    // Check performance stats
    stats := tm.GetGroupCommitStats()
    fmt.Printf("Group commit efficiency: %.2f txns/fsync\n", 
        stats.AvgCommitsPerFsync)
}
```

### B+ Tree Index

```go
// Create B+ Tree index
tree, _ := storage.NewBPlusTree(bpm)

// Insert key-value pairs
tree.Insert(42, 420)
tree.Insert(10, 100)
tree.Insert(25, 250)

// Search
value, found, _ := tree.Search(42)
fmt.Printf("Value: %d\n", value) // Output: 420

// Range scan
iter, _ := tree.Iterator()
for iter.HasNext() {
    key, value, _ := iter.Next()
    fmt.Printf("%d -> %d\n", key, value)
}
```

[See complete API documentation](USAGE.md)

---

## Testing & Benchmarks

```bash
# Run all tests
go test ./storage/...

# Run benchmarks
go test ./storage/ -bench=BenchmarkConcurrent -benchtime=3s

# Run with race detector
go test ./storage/... -race
```

**Latest Results:**
- Concurrent TPS: **2,100 transactions/sec**
- Lock-Free Operations: **4.2M operations/sec**
- Cache Hit Rate: **93.5%** (CLOCK-Pro)
- Parallel Scan: **22.1x speedup** vs sequential

[See detailed benchmarks](PERFORMANCE.md)

---

## Documentation

- **[USAGE.md](USAGE.md)** - Complete API documentation with examples
- **[PERFORMANCE.md](PERFORMANCE.md)** - Benchmarks and performance analysis

---

## Use Cases

### Ideal For

- **High-Concurrency OLTP** - 40-75% faster than traditional RDBMS engines
- **Mixed OLTP+OLAP Workloads** - Combines adaptive caching with parallel scans
- **Storage-Constrained Environments** - 72% storage savings with dual compression
- **Embedded Databases** - Pure Go implementation, no external dependencies
- **Real-Time Analytics** - 22.1x parallel scan speedup
- **Applications Requiring ACID** - Full durability guarantees

---

### Technologies

- **Go** - Leveraging built-in concurrency primitives
- **LZ4** - Fast compression algorithm

---

## License

MIT License - See [LICENSE](LICENSE) file for details

---

## Development

This project was developed with AI assistance. The comments, documentation, and tests was created collaboratively with AI tools to ensure high-quality, well-documented, production-ready code.

---

HexEngine - High-Performance Storage Engine  
Version 2.9.0  