package buffer

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/Argentum88/godb/internal/storage/page"
)

func TestPool_Eviction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Инициализация DiskManager
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	// Инициализация Pool с размером 1, чтобы сразу происходило вытеснение при второй странице
	pool := NewPool(NewLRUReplacer(), pm, 1)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// 1. Создаем первую страницу и заполняем её символами 'A'
	firstPage, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create first page: %v", err)
	}
	firstPageData := bytes.Repeat([]byte{'A'}, page.PageSize)
	copy(firstPage.Bytes(), firstPageData)

	firstPage.MarkDirty()
	firstPage.Unpin() // Теперь страница может быть вытеснена

	// 2. Создаем вторую страницу и заполняем её символами 'B'
	// Так как размер пула 1, это ДОЛЖНО вытеснить первую страницу на диск
	secondPage, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create second page: %v", err)
	}
	secondPageData := bytes.Repeat([]byte{'B'}, page.PageSize)
	copy(secondPage.Bytes(), secondPageData)

	secondPage.MarkDirty()
	secondPage.Unpin()

	// 3. Получаем первую страницу обратно
	// Она должна быть считана с диска, сохраняя 'A', которые мы записали
	fetchedPage, err := pool.FetchPage(ctx, firstPage.pageID, LatchShared)
	if err != nil {
		t.Fatalf("failed to fetch first page: %v", err)
	}
	defer fetchedPage.Unpin()

	if !bytes.Equal(fetchedPage.Bytes(), firstPageData) {
		t.Fatalf("read data does not match written data for page %d", firstPage.pageID)
	}
}
