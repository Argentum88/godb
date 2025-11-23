package buffer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Argentum88/godb/internal/storage/page"
)

var ErrBufferPoolFull = errors.New("buffer pool is full, all pages are pinned")

type frameID int

type replacer interface {
	// Evict выбирает "жертву" и удаляет ее из Replacer.
	// Возвращает ID фрейма, который был выбран для вытеснения.
	// Возвращает false, если нет кандидатов на вытеснение.
	Evict() (frameID frameID, ok bool)

	// Pin сообщает Replacer, что страница с frameID закреплена и не должна вытесняться.
	// Replacer должен удалить ее из списка кандидатов на вытеснение.
	Pin(frameID frameID)

	// Unpin сообщает Replacer, что страница с frameID больше не закреплена
	// и может быть рассмотрена как кандидат на вытеснение.
	Unpin(frameID frameID)
}

type LatchMode int

const (
	LatchShared LatchMode = iota
	LatchExclusive
)

type frame struct {
	id       frameID
	pageID   page.PageID
	data     []byte
	dirty    bool
	pinCount int
	latch    sync.RWMutex
}

type pagePin struct {
	pageID  page.PageID
	frameID frameID
	mode    LatchMode
	pool    *pool
}

// Bytes возвращает срез байтов, представляющий содержимое страницы.
func (p *pagePin) Bytes() []byte {
	return p.pool.frames[p.frameID].data
}

// MarkDirty помечает страницу как измененную (грязную).
// Это означает, что перед выгрузкой страницы на диск ее содержимое должно быть записано.
func (p *pagePin) MarkDirty() {
	p.pool.mu.Lock()
	p.pool.frames[p.frameID].dirty = true
	p.pool.mu.Unlock()
}

// Unpin снимает закрепление страницы в буферном пуле.
// Делает страницу доступной для вытеснения, если ее pinCount достигает нуля.
// Если страница была закреплена в эксклюзивном режиме, она будет разблокирована для других операций.
func (p *pagePin) Unpin() {
	if p.mode == LatchExclusive {
		p.pool.frames[p.frameID].latch.Unlock()
	} else {
		p.pool.frames[p.frameID].latch.RUnlock()
	}

	p.pool.mu.Lock()
	p.pool.frames[p.frameID].pinCount--
	if p.pool.frames[p.frameID].pinCount == 0 {
		p.pool.replacer.Unpin(p.frameID)
	}
	p.pool.mu.Unlock()
}

type pool struct {
	pageToFrameMap map[page.PageID]frameID
	frames         []frame
	freeFrameIDs   []frameID
	replacer       replacer
	pm             page.Manager
	mu             sync.Mutex
}

func NewPool(replacer replacer, pm page.Manager, size int) *pool {
	// Инициализация фреймов и свободных frameID
	frames := make([]frame, size)
	freeFrameIDs := make([]frameID, size)
	blockOfBytes := make([]byte, size*page.PageSize)
	for i := range size {
		left := i * page.PageSize
		right := left + page.PageSize
		frames[i].id = frameID(i)
		frames[i].data = blockOfBytes[left:right]
		freeFrameIDs[i] = frameID(i)
	}

	return &pool{
		frames:         frames,
		freeFrameIDs:   freeFrameIDs,
		pageToFrameMap: make(map[page.PageID]frameID, size),
		replacer:       replacer,
		pm:             pm,
	}
}

// NewPage создает новую страницу, выделяя для нее место на диске и в пуле.
func (p *pool) NewPage(ctx context.Context) (*pagePin, error) {
	p.mu.Lock()

	freeFrame, err := p.findFreeFrame(ctx)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}

	pageID, err := p.pm.AllocatePage(ctx)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to allocate new page: %w", err)
	}
	err = p.pm.ReadPage(ctx, pageID, freeFrame.data)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}
	p.pageToFrameMap[pageID] = freeFrame.id
	p.replacer.Pin(freeFrame.id)
	freeFrame.pageID = pageID
	freeFrame.pinCount++

	p.mu.Unlock()
	freeFrame.latch.Lock()

	return &pagePin{
		pageID:  pageID,
		frameID: freeFrame.id,
		mode:    LatchExclusive,
		pool:    p,
	}, nil
}

// FetchPage извлекает страницу из буферного пула.
// Если страницы нет в пуле, он загружает ее с диска.
func (p *pool) FetchPage(ctx context.Context, pageID page.PageID, mode LatchMode) (*pagePin, error) {
	p.mu.Lock()

	if frameID, ok := p.pageToFrameMap[pageID]; ok {
		p.frames[frameID].pinCount++
		p.replacer.Pin(frameID)
		p.mu.Unlock()

		if mode == LatchExclusive {
			p.frames[frameID].latch.Lock()
		} else {
			p.frames[frameID].latch.RLock()
		}

		return &pagePin{
			pageID:  pageID,
			frameID: frameID,
			mode:    mode,
			pool:    p,
		}, nil
	}

	freeFrame, err := p.findFreeFrame(ctx)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	err = p.pm.ReadPage(ctx, pageID, freeFrame.data)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}
	p.pageToFrameMap[pageID] = freeFrame.id
	p.replacer.Pin(freeFrame.id)
	freeFrame.pageID = pageID
	freeFrame.pinCount++

	p.mu.Unlock()
	if mode == LatchExclusive {
		freeFrame.latch.Lock()
	} else {
		freeFrame.latch.RLock()
	}

	return &pagePin{
		pageID:  pageID,
		frameID: freeFrame.id,
		mode:    mode,
		pool:    p,
	}, nil
}

func (p *pool) FlushAllPages(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.frames {
		if p.frames[i].dirty {
			if err := p.pm.WritePage(ctx, p.frames[i].pageID, p.frames[i].data); err != nil {
				return fmt.Errorf("failed to write dirty page %d to disk: %w", p.frames[i].pageID, err)
			}
			p.frames[i].dirty = false
		}
	}

	return nil
}

func (p *pool) Close(ctx context.Context) error {
	err := p.FlushAllPages(ctx)
	if err != nil {
		return err
	}

	err = p.pm.Sync(ctx)
	if err != nil {
		return err
	}

	err = p.pm.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (p *pool) findFreeFrame(ctx context.Context) (*frame, error) {
	lenFreeFrameIDs := len(p.freeFrameIDs)
	if lenFreeFrameIDs > 0 {
		freeFrameID := p.freeFrameIDs[lenFreeFrameIDs-1]
		p.freeFrameIDs = p.freeFrameIDs[:lenFreeFrameIDs-1]

		return &p.frames[freeFrameID], nil
	}

	evictedFrameID, ok := p.replacer.Evict()
	if !ok {
		return nil, ErrBufferPoolFull
	}

	evictedFrame := &p.frames[evictedFrameID]
	if evictedFrame.dirty {
		if err := p.pm.WritePage(ctx, evictedFrame.pageID, evictedFrame.data); err != nil {
			return nil, fmt.Errorf("failed to write dirty page %d to disk: %w", evictedFrame.pageID, err)
		}
	}

	delete(p.pageToFrameMap, evictedFrame.pageID)
	evictedFrame.dirty = false

	return evictedFrame, nil
}
