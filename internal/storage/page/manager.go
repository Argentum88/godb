package page

import (
	"context"
	"fmt"
	"os"
	"sync"
)

const PageSize = 4 * 1024 // 4KB

type PageID uint64

type Manager interface {
	AllocatePage(ctx context.Context) (PageID, error) // Расширить файл и выделить новую страницу
	ReadPage(ctx context.Context, pageID PageID, p []byte) error
	WritePage(ctx context.Context, pageID PageID, p []byte) error
	Sync(ctx context.Context) error  // Принудительно сбросить буферы на диск
	Close(ctx context.Context) error // Закрыть менеджер и освободить ресурсы
}

type diskManager struct {
	file     *os.File
	nextPage PageID
	mtx      sync.RWMutex
	zeroPage []byte
}

func NewDiskManager(ctx context.Context, filePath string) (*diskManager, error) {
	fd, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	dm := &diskManager{file: fd, zeroPage: make([]byte, PageSize)}
	fileSize, err := dm.getFileSize()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	if (fileSize % PageSize) != 0 {
		return nil, fmt.Errorf("file size %d is not aligned to page size %d", fileSize, PageSize)
	}

	dm.nextPage = PageID(fileSize / PageSize)

	return dm, nil
}

func (dm *diskManager) AllocatePage(ctx context.Context) (PageID, error) {
	dm.mtx.Lock()
	defer dm.mtx.Unlock()

	nextPage := dm.nextPage
	if err := dm.writePage(nextPage, dm.zeroPage); err != nil {
		return 0, fmt.Errorf("failed to allocate page: %w", err)
	}
	dm.nextPage++

	return nextPage, nil
}

func (dm *diskManager) ReadPage(ctx context.Context, pageID PageID, p []byte) error {
	if len(p) != PageSize {
		return fmt.Errorf("invalid page size: got %d, want %d", len(p), PageSize)
	}

	dm.mtx.RLock()
	nextPage := dm.nextPage
	dm.mtx.RUnlock()
	if pageID >= nextPage {
		return fmt.Errorf("pageID %d out of bounds (lastPage: %d)", pageID, nextPage-1)
	}

	_, err := dm.file.ReadAt(p, dm.calculateOffsetByPageID(pageID))
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", pageID, err)
	}
	return nil
}

func (dm *diskManager) WritePage(ctx context.Context, pageID PageID, p []byte) error {
	if len(p) != PageSize {
		return fmt.Errorf("invalid page size: got %d, want %d", len(p), PageSize)
	}

	dm.mtx.RLock()
	nextPage := dm.nextPage
	dm.mtx.RUnlock()
	if pageID >= nextPage {
		return fmt.Errorf("pageID %d out of bounds (lastPage: %d)", pageID, nextPage-1)
	}

	err := dm.writePage(pageID, p)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}
	return nil
}

func (dm *diskManager) Sync(ctx context.Context) error {
	err := dm.file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (dm *diskManager) Close(ctx context.Context) error {
	err := dm.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	return nil
}

func (dm *diskManager) writePage(pageID PageID, p []byte) error {
	_, err := dm.file.WriteAt(p, dm.calculateOffsetByPageID(pageID))
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}
	return nil
}

func (dm *diskManager) calculateOffsetByPageID(pageID PageID) int64 {
	return int64(pageID) * int64(PageSize)
}

func (dm *diskManager) getFileSize() (int64, error) {
	fileInfo, err := dm.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get file stat: %w", err)
	}
	return fileInfo.Size(), nil
}
