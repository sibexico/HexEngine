package storage

import (
	"math/rand"
	"os"
	"testing"
)

// Setup helper for buffer pool benchmarks
func setupBufferPool(b *testing.B, poolSize uint32) (*BufferPoolManager, func()) {
	b.Helper()

	dbFile := "bench_bpm_test.db"
	dm, err := NewDiskManager(dbFile)
	if err != nil {
		b.Fatal(err)
	}

	bpm, err := NewBufferPoolManager(poolSize, dm)
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		bpm.FlushAllPages()
		dm.Close()
		os.Remove(dbFile)
	}

	return bpm, cleanup
}

// Benchmark page allocation
func BenchmarkBufferPoolNewPage(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 100)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, err := bpm.NewPage()
		if err != nil {
			b.Fatal(err)
		}
		bpm.UnpinPage(page.GetPageId(), false)
	}
}

// Benchmark page fetching (cache hits)
func BenchmarkBufferPoolFetchPageCacheHit(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 100)
	defer cleanup()

	// Create a page to fetch repeatedly
	page, _ := bpm.NewPage()
	pageId := page.GetPageId()
	bpm.UnpinPage(pageId, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fetched, err := bpm.FetchPage(pageId)
		if err != nil {
			b.Fatal(err)
		}
		bpm.UnpinPage(fetched.GetPageId(), false)
	}
}

// Benchmark page fetching (cache misses)
func BenchmarkBufferPoolFetchPageCacheMiss(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 10) // Small pool to force evictions
	defer cleanup()

	// Pre-allocate pages on disk
	pageIds := make([]uint32, 100)
	for i := 0; i < 100; i++ {
		page, _ := bpm.NewPage()
		pageIds[i] = page.GetPageId()
		bpm.UnpinPage(page.GetPageId(), true) // Mark dirty to write to disk
	}
	// Flush all to disk
	bpm.FlushAllPages()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := pageIds[i%100]
		fetched, err := bpm.FetchPage(pageId)
		if err != nil {
			b.Fatal(err)
		}
		bpm.UnpinPage(fetched.GetPageId(), false)
	}
}

// Benchmark buffer pool with different pool sizes
func BenchmarkBufferPoolSizes(b *testing.B) {
	sizes := []uint32{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(benchName("PoolSize", int(size)), func(b *testing.B) {
			bpm, cleanup := setupBufferPool(b, size)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, err := bpm.NewPage()
				if err != nil {
					// Pool full, fetch existing page
					page, err = bpm.FetchPage(1)
					if err != nil {
						b.Fatal(err)
					}
				}
				bpm.UnpinPage(page.GetPageId(), false)
			}
		})
	}
}

// Benchmark dirty page flushes
func BenchmarkBufferPoolFlushDirtyPages(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 100)
	defer cleanup()

	// Create pages and mark them dirty
	pageIds := make([]uint32, 50)
	for i := 0; i < 50; i++ {
		page, _ := bpm.NewPage()
		pageIds[i] = page.GetPageId()
		bpm.UnpinPage(page.GetPageId(), true) // Mark as dirty
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := bpm.FlushAllPages()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark random access patterns
func BenchmarkBufferPoolRandomAccess(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 100)
	defer cleanup()

	// Pre-allocate 500 pages (more than buffer pool)
	pageIds := make([]uint32, 500)
	for i := 0; i < 500; i++ {
		page, _ := bpm.NewPage()
		pageIds[i] = page.GetPageId()
		bpm.UnpinPage(page.GetPageId(), true) // Mark dirty
	}
	// Flush all to disk
	bpm.FlushAllPages()

	r := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pageId := pageIds[r.Intn(500)]
		page, err := bpm.FetchPage(pageId)
		if err != nil {
			b.Fatal(err)
		}
		bpm.UnpinPage(page.GetPageId(), false)
	}
}

// Benchmark sequential access patterns
func BenchmarkBufferPoolSequentialAccess(b *testing.B) {
	bpm, cleanup := setupBufferPool(b, 100)
	defer cleanup()

	// Pre-allocate 500 pages
	pageIds := make([]uint32, 500)
	for i := 0; i < 500; i++ {
		page, _ := bpm.NewPage()
		pageIds[i] = page.GetPageId()
		bpm.UnpinPage(page.GetPageId(), true) // Mark dirty
	}
	// Flush all to disk
	bpm.FlushAllPages()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := pageIds[i%500]
		page, err := bpm.FetchPage(pageId)
		if err != nil {
			b.Fatal(err)
		}
		bpm.UnpinPage(page.GetPageId(), false)
	}
}

// Helper function to create benchmark names
func benchName(prefix string, value int) string {
	return prefix + string(rune('0'+value/1000)) +
		string(rune('0'+(value/100)%10)) +
		string(rune('0'+(value/10)%10)) +
		string(rune('0'+value%10))
}
