package storage

import (
	"sync"
	"testing"
)

func TestShardedPageTableBasic(t *testing.T) {
	spt := NewShardedPageTable(16)

	// Test Put and Get
	page := NewPage(1)
	spt.Put(1, page)

	retrieved, exists := spt.Get(1)
	if !exists {
		t.Error("Expected page to exist")
	}
	if retrieved.GetPageId() != 1 {
		t.Errorf("Expected page ID 1, got %d", retrieved.GetPageId())
	}
}

func TestShardedPageTableDelete(t *testing.T) {
	spt := NewShardedPageTable(16)

	page := NewPage(1)
	spt.Put(1, page)

	spt.Delete(1)

	_, exists := spt.Get(1)
	if exists {
		t.Error("Expected page to be deleted")
	}
}

func TestShardedPageTableSize(t *testing.T) {
	spt := NewShardedPageTable(16)

	// Add multiple pages
	for i := uint32(1); i <= 100; i++ {
		page := NewPage(i)
		spt.Put(i, page)
	}

	if spt.Size() != 100 {
		t.Errorf("Expected size 100, got %d", spt.Size())
	}
}

func TestShardedPageTableConcurrent(t *testing.T) {
	spt := NewShardedPageTable(64)

	var wg sync.WaitGroup
	numGoroutines := 100
	pagesPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < pagesPerGoroutine; j++ {
				pageId := uint32(offset*pagesPerGoroutine + j)
				page := NewPage(pageId)
				spt.Put(pageId, page)
			}
		}(i)
	}

	wg.Wait()

	expectedSize := numGoroutines * pagesPerGoroutine
	if spt.Size() != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, spt.Size())
	}
}

func TestShardedPageTableGetAll(t *testing.T) {
	spt := NewShardedPageTable(16)

	// Add pages
	for i := uint32(1); i <= 50; i++ {
		page := NewPage(i)
		spt.Put(i, page)
	}

	pages := spt.GetAll()
	if len(pages) != 50 {
		t.Errorf("Expected 50 pages, got %d", len(pages))
	}
}

func TestShardedPageTableForEach(t *testing.T) {
	spt := NewShardedPageTable(16)

	// Add pages
	for i := uint32(1); i <= 50; i++ {
		page := NewPage(i)
		spt.Put(i, page)
	}

	count := 0
	spt.ForEach(func(pageId uint32, page *Page) bool {
		count++
		return true
	})

	if count != 50 {
		t.Errorf("Expected to iterate over 50 pages, got %d", count)
	}
}

func TestShardedPageTableClear(t *testing.T) {
	spt := NewShardedPageTable(16)

	// Add pages
	for i := uint32(1); i <= 50; i++ {
		page := NewPage(i)
		spt.Put(i, page)
	}

	spt.Clear()

	if spt.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", spt.Size())
	}
}

// Benchmark: Sharded vs Global Lock

func BenchmarkShardedPageTableGet(b *testing.B) {
	spt := NewShardedPageTable(64)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		spt.Put(i, NewPage(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := uint32(i % 1000)
		spt.Get(pageId)
	}
}

func BenchmarkGlobalLockPageTableGet(b *testing.B) {
	var mu sync.RWMutex
	pages := make(map[uint32]*Page)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		pages[i] = NewPage(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := uint32(i % 1000)
		mu.RLock()
		_ = pages[pageId]
		mu.RUnlock()
	}
}

func BenchmarkShardedPageTablePut(b *testing.B) {
	spt := NewShardedPageTable(64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := uint32(i % 10000)
		spt.Put(pageId, NewPage(pageId))
	}
}

func BenchmarkGlobalLockPageTablePut(b *testing.B) {
	var mu sync.RWMutex
	pages := make(map[uint32]*Page)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pageId := uint32(i % 10000)
		mu.Lock()
		pages[pageId] = NewPage(pageId)
		mu.Unlock()
	}
}

// Concurrent benchmarks
func BenchmarkShardedPageTableConcurrentGet(b *testing.B) {
	spt := NewShardedPageTable(64)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		spt.Put(i, NewPage(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint32(0)
		for pb.Next() {
			pageId := i % 1000
			spt.Get(pageId)
			i++
		}
	})
}

func BenchmarkGlobalLockPageTableConcurrentGet(b *testing.B) {
	var mu sync.RWMutex
	pages := make(map[uint32]*Page)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		pages[i] = NewPage(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint32(0)
		for pb.Next() {
			pageId := i % 1000
			mu.RLock()
			_ = pages[pageId]
			mu.RUnlock()
			i++
		}
	})
}

func BenchmarkShardedPageTableConcurrentMixed(b *testing.B) {
	spt := NewShardedPageTable(64)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		spt.Put(i, NewPage(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint32(0)
		for pb.Next() {
			pageId := i % 1000
			if i%10 == 0 {
				spt.Put(pageId, NewPage(pageId))
			} else {
				spt.Get(pageId)
			}
			i++
		}
	})
}

func BenchmarkGlobalLockPageTableConcurrentMixed(b *testing.B) {
	var mu sync.RWMutex
	pages := make(map[uint32]*Page)

	// Pre-populate
	for i := uint32(0); i < 1000; i++ {
		pages[i] = NewPage(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint32(0)
		for pb.Next() {
			pageId := i % 1000
			if i%10 == 0 {
				mu.Lock()
				pages[pageId] = NewPage(pageId)
				mu.Unlock()
			} else {
				mu.RLock()
				_ = pages[pageId]
				mu.RUnlock()
			}
			i++
		}
	})
}
