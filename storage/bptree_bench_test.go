package storage

import (
	"math/rand"
	"os"
	"testing"
)

// Setup helper for B+ Tree benchmarks
func setupBPTree(b *testing.B) (*BPlusTree, func()) {
	b.Helper()

	// Create temp file for testing
	dbFile := "bench_test.db"
	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}

	bpm, err := NewBufferPoolManager(100, dm)
	if err != nil {
		b.Fatal(err)
	}

	tree, err := NewBPlusTree(bpm)
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		bpm.FlushAllPages()
		dm.Close()
		os.Remove(dbFile)
	}

	return tree, cleanup
}

// Benchmark B+ Tree Insert operations
func BenchmarkBPTreeInsert(b *testing.B) {
	tree, cleanup := setupBPTree(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := int64(i)
		value := int64(i * 100)
		tree.Insert(key, value)
	}
}

// Benchmark B+ Tree Search operations
func BenchmarkBPTreeSearch(b *testing.B) {
	tree, cleanup := setupBPTree(b)
	defer cleanup()

	// Pre-populate with 10k entries
	for i := 0; i < 10000; i++ {
		tree.Insert(int64(i), int64(i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := int64(i % 10000)
		tree.Search(key)
	}
}

// Benchmark B+ Tree Delete operations
func BenchmarkBPTreeDelete(b *testing.B) {
	tree, cleanup := setupBPTree(b)
	defer cleanup()

	// Pre-populate tree
	for i := 0; i < b.N; i++ {
		tree.Insert(int64(i), int64(i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Delete(int64(i))
	}
}

// Benchmark B+ Tree with random operations
func BenchmarkBPTreeMixed(b *testing.B) {
	tree, cleanup := setupBPTree(b)
	defer cleanup()

	r := rand.New(rand.NewSource(42))

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		tree.Insert(int64(i), int64(i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := int64(r.Int63n(10000))

		switch i % 3 {
		case 0:
			tree.Insert(key, key*100)
		case 1:
			tree.Search(key)
		case 2:
			tree.Delete(key)
		}
	}
}

// Benchmark sequential vs random inserts
func BenchmarkBPTreeInsertPatterns(b *testing.B) {
	b.Run("Sequential", func(b *testing.B) {
		tree, cleanup := setupBPTree(b)
		defer cleanup()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tree.Insert(int64(i), int64(i*100))
		}
	})

	b.Run("Random", func(b *testing.B) {
		tree, cleanup := setupBPTree(b)
		defer cleanup()

		r := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := r.Int63()
			tree.Insert(key, key*100)
		}
	})

	b.Run("ReverseSequential", func(b *testing.B) {
		tree, cleanup := setupBPTree(b)
		defer cleanup()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := int64(b.N - i)
			tree.Insert(key, key*100)
		}
	})
}

// Benchmark bulk load vs sequential inserts
func BenchmarkBPTreeBulkLoad(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run("BulkLoad_"+string(rune('0'+size/1000)), func(b *testing.B) {
			entries := make([]BPTreeEntry, size)
			for i := 0; i < size; i++ {
				entries[i] = BPTreeEntry{
					Key: int64(i),
					Value: int64(i * 100),
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tree, cleanup := setupBPTree(b)
				b.StartTimer()

				tree.BulkLoad(entries)

				b.StopTimer()
				cleanup()
				b.StartTimer()
			}
		})

		b.Run("SequentialInsert_"+string(rune('0'+size/1000)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tree, cleanup := setupBPTree(b)
				b.StartTimer()

				for j := 0; j < size; j++ {
					tree.Insert(int64(j), int64(j*100))
				}

				b.StopTimer()
				cleanup()
				b.StartTimer()
			}
		})
	}
}

// Benchmark bulk load with different data sizes
func BenchmarkBulkLoadSizes(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(string(rune('0'+size/1000))+"entries", func(b *testing.B) {
			// Prepare data once
			entries := make([]BPTreeEntry, size)
			for i := 0; i < size; i++ {
				entries[i] = BPTreeEntry{
					Key: int64(i),
					Value: int64(i * 7),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tree, cleanup := setupBPTree(b)
				b.StartTimer()

				err := tree.BulkLoad(entries)
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				cleanup()
				b.StartTimer()
			}
		})
	}
}
