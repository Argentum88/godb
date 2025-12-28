package page

import (
	"encoding/binary"
	"fmt"
)

var PageFullErr = fmt.Errorf("page is full")

const (
	slotCountOffset        = 0
	freeSpacePointerOffset = 2

	slotCountByteSize    = 2
	freeSpacePointerSize = 2
	headerSize           = slotCountByteSize + freeSpacePointerSize
	slotSize             = 4
)

type slotFlag uint8

const (
	slotUsed   slotFlag = 0
	slotDead   slotFlag = 1
	slotUnused slotFlag = 2
)

type slottedPage struct {
	data []byte
}

// NewSlottedPage создает обертку над срезом байт для работы со слотовой страницей
func NewSlottedPage(data []byte) *slottedPage {
	return &slottedPage{data: data}
}

// Init инициализирует заголовки новой пустой страницы
func (sp *slottedPage) Init(pageSize int) {
	binary.LittleEndian.PutUint16(sp.data[slotCountOffset:slotCountOffset+slotCountByteSize], 0)
	binary.LittleEndian.PutUint16(sp.data[freeSpacePointerOffset:freeSpacePointerOffset+freeSpacePointerSize], uint16(pageSize))
}

// InsertTuple добавляет кортеж и возвращает его SlotID
// Если места на странице не хватает, выполняем compact, если все равно не хватает - ошибка
func (sp *slottedPage) InsertTuple(tuple []byte) (uint16, error) {
	slotCount := binary.LittleEndian.Uint16(sp.data[slotCountOffset : slotCountOffset+slotCountByteSize])
	freeSpacePointer := binary.LittleEndian.Uint16(sp.data[freeSpacePointerOffset : freeSpacePointerOffset+freeSpacePointerSize])
	pointerForNewSlot := headerSize + slotSize*slotCount
	if (freeSpacePointer - pointerForNewSlot) < (uint16(len(tuple)) + slotSize) {
		return 0, PageFullErr
	}

	copy(sp.data[freeSpacePointer-uint16(len(tuple)):freeSpacePointer], tuple)

	slot := newSlot(freeSpacePointer-uint16(len(tuple)), len(tuple))
	copy(sp.data[pointerForNewSlot:pointerForNewSlot+slotSize], slot)

	binary.LittleEndian.PutUint16(sp.data[slotCountOffset:slotCountOffset+slotCountByteSize], slotCount+1)
	binary.LittleEndian.PutUint16(sp.data[freeSpacePointerOffset:freeSpacePointerOffset+freeSpacePointerSize], freeSpacePointer-uint16(len(tuple)))

	return slotCount, nil
}

// GetTuple возвращает данные кортежа по SlotID
func (sp *slottedPage) GetTuple(slotID uint16) ([]byte, error) {
	pointerToSlot := headerSize + slotSize*slotID
	slot := sp.data[pointerToSlot : pointerToSlot+slotSize]
	offset, _, length := unpackSlot(slot)

	return sp.data[offset : offset+length], nil
}

// DeleteTuple помечает слот как пустой (Length = 0)
func (sp *slottedPage) DeleteTuple(slotID uint16) {

}

// newSlot формирует слот
func newSlot(offset uint16, length int) []byte {
	// Схема упаковки:
	// [ Offset (15 бит) ] [ Flags (2 бита) ] [ Length (15 бит) ]
	// Биты: 31.........17 16..............15 14................0
	packed := (uint32(offset) << 17) | (uint32(slotUsed) << 15) | uint32(length)

	buf := make([]byte, slotSize)
	binary.LittleEndian.PutUint32(buf, packed)
	return buf
}

// unpackSlot распаковывает слот
func unpackSlot(data []byte) (offset uint16, flag slotFlag, length uint16) {
	val := binary.LittleEndian.Uint32(data)

	offset = uint16(val >> 17)
	flag = slotFlag(val>>15) & 0x3 // маска для 2 бит
	length = uint16(val) & 0x7FFF  // маска для 15 бит
	return
}
