package page

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func Test_diskManager_ReadPage(t *testing.T) {
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
