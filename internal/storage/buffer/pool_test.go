package buffer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

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

	// Инициализация Pool с размером 2, чтобы вытеснение произошло при третьей странице
	pool := NewPool(NewLRUReplacer(), pm, 2)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	// 1. Создаем страницу A и заполняем её символами 'A'
	pageA, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create page A: %v", err)
	}
	dataA := bytes.Repeat([]byte{'A'}, page.PageSize)
	copy(pageA.Bytes(), dataA)

	pageA.MarkDirty()
	pageA.Unpin() // Теперь страница может быть вытеснена

	// 2. Создаем страницу B и заполняем её символами 'B'
	pageB, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create page B: %v", err)
	}
	dataB := bytes.Repeat([]byte{'B'}, page.PageSize)
	copy(pageB.Bytes(), dataB)

	pageB.MarkDirty()
	pageB.Unpin()

	// 3. Создаем страницу C и заполняем её символами 'C'
	// Так как размер пула 2, это ДОЛЖНО вытеснить одну из предыдущих страниц на диск (например, A)
	pageC, err := pool.NewPage(ctx)
	if err != nil {
		t.Fatalf("failed to create page C: %v", err)
	}
	dataC := bytes.Repeat([]byte{'C'}, page.PageSize)
	copy(pageC.Bytes(), dataC)

	pageC.MarkDirty()
	pageC.Unpin()

	// 4. Получаем страницу A обратно
	// Она должна быть считана с диска, сохраняя 'A', которые мы записали
	fetchedPage, err := pool.FetchPage(ctx, pageA.pageID, LatchShared)
	if err != nil {
		t.Fatalf("failed to fetch page A: %v", err)
	}
	defer fetchedPage.Unpin()

	if !bytes.Equal(fetchedPage.Bytes(), dataA) {
		t.Fatalf("read data does not match written data for page %d", pageA.pageID)
	}
}

func TestPool_Concurrency(t *testing.T) {
	const (
		numPages = 10 // Всего страниц в тесте
		readers  = 50
		writers  = 50
		flushers = 10
	)

	t.Parallel()
	ctx := context.Background()

	// Инициализация DiskManager
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pm, err := page.NewDiskManager(ctx, dbPath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}

	// Инициализация Pool
	pool := NewPool(NewLRUReplacer(), pm, 10)
	t.Cleanup(func() {
		pool.Close(ctx)
	})

	t.Log("Phase 1: Initialization (Allocating pages)")
	pageIDs := make([]page.PageID, numPages)
	wgInit := new(sync.WaitGroup)
	for i := range numPages {
		wgInit.Add(1)
		go func(i int) {
			defer wgInit.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			pin, err := pool.NewPage(ctx)
			if err != nil {
				t.Errorf("failed to create new page: %v", err)
				return
			}
			pageIDs[i] = pin.pageID
			writeTestData(pin.Bytes(), rnd)
			pin.MarkDirty()
			pin.Unpin()
		}(i)
	}
	wgInit.Wait()

	t.Log("Phase 2: Concurrent Readers/Writers/Flushers")
	wgChaos := new(sync.WaitGroup)

	for range readers {
		wgChaos.Add(1)
		go func() {
			defer wgChaos.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			targetPageID := pageIDs[rnd.Intn(numPages)]
			pin, err := pool.FetchPage(ctx, targetPageID, LatchShared)
			if err != nil {
				t.Errorf("failed to fetch page %d: %v", targetPageID, err)
				return
			}
			err = checkTestData(pin.Bytes())
			if err != nil {
				t.Errorf("data integrity check failed for page %d: %v", targetPageID, err)
			}
			pin.Unpin()
		}()
	}

	for range writers {
		wgChaos.Add(1)
		go func() {
			defer wgChaos.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			targetPageID := pageIDs[rnd.Intn(numPages)]
			pin, err := pool.FetchPage(ctx, targetPageID, LatchExclusive)
			if err != nil {
				t.Errorf("failed to fetch page %d: %v", targetPageID, err)
				return
			}
			writeTestData(pin.Bytes(), rnd)
			pin.MarkDirty()
			pin.Unpin()
		}()
	}

	for range flushers {
		wgChaos.Add(1)
		go func() {
			defer wgChaos.Done()
			err := pool.FlushAllPages(ctx)
			if err != nil {
				t.Errorf("failed to flush all pages: %v", err)
			}
		}()
	}

	wgChaos.Wait()
}

// writeTestData формирует контент: [RandomBytes..., Checksum] и пишет в p
func writeTestData(p []byte, rng *rand.Rand) {
	rng.Read(p[:page.PageSize-4])
	sum := crc32.ChecksumIEEE(p[:page.PageSize-4])
	binary.BigEndian.PutUint32(p[page.PageSize-4:], sum)
}

// checkTestData проверяет контрольную сумму в p
func checkTestData(p []byte) error {
	storedSum := binary.BigEndian.Uint32(p[page.PageSize-4:])
	sum := crc32.ChecksumIEEE(p[:page.PageSize-4])
	if storedSum != sum {
		return fmt.Errorf("checksum mismatch: stored %d, computed %d", storedSum, sum)
	}

	return nil
}
