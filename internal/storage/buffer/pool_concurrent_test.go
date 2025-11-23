package buffer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Argentum88/godb/internal/storage/page"
)

// TestPool_ConcurrentFetchUnpin проверяет безопасность параллельного
// Fetch/Unpin на разных страницах
func TestPool_ConcurrentFetchUnpin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	pool := NewPool(NewLRUReplacer(), pm, 10)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// Создаем несколько страниц заранее
	pageIDs := make([]page.PageID, 5)
	for i := range pageIDs {
		pin, err := pool.NewPage(ctx)
		if err != nil {
			t.Fatalf("failed to create page %d: %v", i, err)
		}
		pageIDs[i] = pin.pageID
		pin.Unpin()
	}

	// Параллельный доступ к разным страницам
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			pageID := pageIDs[iteration%len(pageIDs)]

			// Fetch в shared режиме
			pin, err := pool.FetchPage(ctx, pageID, LatchShared)
			if err != nil {
				t.Errorf("iteration %d: failed to fetch page: %v", iteration, err)
				return
			}
			_ = pin.Bytes() // Читаем данные
			pin.Unpin()
		}(i)
	}

	wg.Wait()
}

// TestPool_ConcurrentFetchFlush проверяет безопасность параллельного
// Fetch и Flush операций
func TestPool_ConcurrentFetchFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	pool := NewPool(NewLRUReplacer(), pm, 10)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// Создаем страницы
	pageIDs := make([]page.PageID, 3)
	for i := range pageIDs {
		pin, err := pool.NewPage(ctx)
		if err != nil {
			t.Fatalf("failed to create page %d: %v", i, err)
		}
		pageIDs[i] = pin.pageID
		pin.MarkDirty() // Помечаем как dirty
		pin.Unpin()
	}

	var wg sync.WaitGroup

	// Горутины, которые постоянно читают страницы
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				pageID := pageIDs[j%len(pageIDs)]
				pin, err := pool.FetchPage(ctx, pageID, LatchShared)
				if err != nil {
					t.Errorf("reader %d iteration %d: failed to fetch: %v", id, j, err)
					return
				}
				_ = pin.Bytes()
				pin.Unpin()
			}
		}(i)
	}

	// Горутины, которые постоянно флашат
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := pool.FlushAllPages(ctx); err != nil {
					t.Errorf("flusher %d iteration %d: failed to flush: %v", id, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestPool_ConcurrentNewPageFlush проверяет безопасность параллельного
// создания страниц и флаша
func TestPool_ConcurrentNewPageFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	pool := NewPool(NewLRUReplacer(), pm, 20)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	var wg sync.WaitGroup

	// Горутины, создающие новые страницы
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pin, err := pool.NewPage(ctx)
				if err != nil {
					t.Errorf("creator %d iteration %d: failed to create page: %v", id, j, err)
					return
				}
				pin.MarkDirty()
				pin.Unpin()
			}
		}(i)
	}

	// Горутины, которые флашат
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 15; j++ {
				if err := pool.FlushAllPages(ctx); err != nil {
					t.Errorf("flusher %d iteration %d: failed to flush: %v", id, j, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestPool_ConcurrentExclusiveAccess проверяет, что эксклюзивный доступ
// действительно эксклюзивен (только одна горутина может держать exclusive latch)
func TestPool_ConcurrentExclusiveAccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	pool := NewPool(NewLRUReplacer(), pm, 5)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// Создаем одну страницу
	pin, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create page: %v", err)
	}
	pageID := pin.pageID
	pin.Unpin()

	// Счетчик для проверки эксклюзивности
	var activeWriters int32
	var mu sync.Mutex

	var wg sync.WaitGroup

	// Несколько горутин пытаются получить эксклюзивный доступ
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			pin, err := pool.FetchPage(ctx, pageID, LatchExclusive)
			if err != nil {
				t.Errorf("writer %d: failed to fetch: %v", id, err)
				return
			}

			// Проверяем, что мы единственные
			mu.Lock()
			activeWriters++
			current := activeWriters
			mu.Unlock()

			if current != 1 {
				t.Errorf("writer %d: expected 1 active writer, got %d", id, current)
			}

			// "Работаем" со страницей
			_ = pin.Bytes()
			pin.MarkDirty()

			// Освобождаем
			mu.Lock()
			activeWriters--
			mu.Unlock()

			pin.Unpin()
		}(i)
	}

	wg.Wait()
}

// TestPool_ConcurrentMixedAccess проверяет смешанный доступ:
// shared readers и exclusive writers
func TestPool_ConcurrentMixedAccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	pool := NewPool(NewLRUReplacer(), pm, 10)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// Создаем страницы
	pageIDs := make([]page.PageID, 3)
	for i := range pageIDs {
		pin, err := pool.NewPage(ctx)
		if err != nil {
			t.Fatalf("failed to create page %d: %v", i, err)
		}
		pageIDs[i] = pin.pageID
		pin.Unpin()
	}

	var wg sync.WaitGroup

	// Readers (shared access)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				pageID := pageIDs[j%len(pageIDs)]
				pin, err := pool.FetchPage(ctx, pageID, LatchShared)
				if err != nil {
					t.Errorf("reader %d iteration %d: failed to fetch: %v", id, j, err)
					return
				}
				_ = pin.Bytes()
				pin.Unpin()
			}
		}(i)
	}

	// Writers (exclusive access)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				pageID := pageIDs[j%len(pageIDs)]
				pin, err := pool.FetchPage(ctx, pageID, LatchExclusive)
				if err != nil {
					t.Errorf("writer %d iteration %d: failed to fetch: %v", id, j, err)
					return
				}
				_ = pin.Bytes()
				pin.MarkDirty()
				pin.Unpin()
			}
		}(i)
	}

	wg.Wait()
}
