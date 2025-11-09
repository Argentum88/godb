package buffer

import (
	"context"
	"sync"

	"github.com/Argentum88/godb/internal/storage/page"
)

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
	data     []byte
	dirty    bool
	pinCount int
	latch    sync.RWMutex
}

type pagePin struct {
	pageID page.PageID
	mode   LatchMode
	pool   *pool
}

// Bytes возвращает срез байтов, представляющий содержимое страницы.
func (p *pagePin) Bytes() []byte {
	return nil
}

// MarkDirty помечает страницу как измененную (грязную).
// Это означает, что перед выгрузкой страницы на диск ее содержимое должно быть записано.
func (p *pagePin) MarkDirty() {}

// Unpin снимает закрепление страницы в буферном пуле.
// Делает страницу доступной для вытеснения, если ее pinCount достигает нуля.
// Если страница была закреплена в эксклюзивном режиме, она будет разблокирована для других операций.
func (p *pagePin) Unpin() error {
	return nil
}

type pool struct {
	size           int
	pageToFrameMap map[page.PageID]frameID
	frames         []frame
	freeFrameIDs   []frameID
	replacer       replacer
	pm             page.Manager
}

func NewPool(size int) *pool {
	return &pool{
		size: size,
	}
}

// NewPage создает новую страницу, выделяя для нее место на диске и в пуле.
// Возвращает nil, если не может создать новую страницу (например, все фреймы заняты и закреплены).
func (p *pool) NewPage(ctx context.Context, mode LatchMode) (*pagePin, error) {
	return nil, nil
}

// FetchPage извлекает страницу из буферного пула.
// Если страницы нет в пуле, он загружает ее с диска.
// Возвращает nil, если страница не может быть извлечена (например, все фреймы заняты и закреплены).
func (p *pool) FetchPage(ctx context.Context, pageID page.PageID, mode LatchMode) (*pagePin, error) {
	return nil, nil
}
