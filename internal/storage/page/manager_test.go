package page

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func Test_diskManager_SequentialLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Initialize DiskManager with a temporary file
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test.db")
	pm, err := NewDiskManager(ctx, filePath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}
	t.Cleanup(func() {
		pm.Close(ctx)
	})

	// Allocate a page, write data and sync

	// Allocates three pages, intentionally skipping the first two to operate on the third page (pageID = 2).
	// This ensures that page offset calculations and page arithmetic are handled correctly by the manager.
	if _, err = pm.AllocatePage(ctx); err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	if _, err = pm.AllocatePage(ctx); err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	pageID, err := pm.AllocatePage(ctx)
	if err != nil {
		t.Fatalf("failed to allocate page: %v", err)
	}
	bufForWrite := bytes.Repeat([]byte{'a'}, PageSize)
	err = pm.WritePage(ctx, pageID, bufForWrite)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}
	err = pm.Sync(ctx)
	if err != nil {
		t.Fatalf("failed to sync DiskManager: %v", err)
	}
	
	// Read back the page and verify contents
	bufForRead := make([]byte, PageSize)
	err = pm.ReadPage(ctx, pageID, bufForRead)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}
	if !bytes.Equal(bufForWrite, bufForRead) {
        t.Fatalf("read data does not match written data")
    }
}

func Test_diskManager_ConcurrentWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Initialize DiskManager with a temporary file
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "test.db")
	pm, err := NewDiskManager(ctx, filePath)
	if err != nil {
		t.Fatalf("failed to create DiskManager: %v", err)
	}
	t.Cleanup(func() {
		pm.Close(ctx)
	})

	// Concurrently allocate and write to multiple pages
	wg := new(sync.WaitGroup)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pageID, err := pm.AllocatePage(ctx)
			if err != nil {
				t.Errorf("failed to allocate page: %v", err)
			}
			bufForWrite := bytes.Repeat([]byte{byte(pageID)}, PageSize)
			err = pm.WritePage(ctx, pageID, bufForWrite)
			if err != nil {
				t.Errorf("failed to write page: %v", err)
			}
		}()
	}
	wg.Wait()

	for i := range 10 {
		pageID := PageID(i)
		expectedBuffer := bytes.Repeat([]byte{byte(pageID)}, PageSize)
		bufForRead := make([]byte, PageSize)
		err = pm.ReadPage(ctx, pageID, bufForRead)
		if err != nil {
			t.Fatalf("failed to read page: %v", err)
		}
		if !bytes.Equal(expectedBuffer, bufForRead) {
			t.Fatalf("read data does not match written data")
		}
	}
}
