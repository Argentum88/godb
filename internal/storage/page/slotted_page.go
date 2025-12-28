package page

import (
	"encoding/binary"
	"fmt"
)

var ErrPageFull = fmt.Errorf("page is full")

const (
	slotCountOffset        = 0
	freeSpacePointerOffset = 2

	slotCountSize        = 2
	freeSpacePointerSize = 2
	headerSize           = slotCountSize + freeSpacePointerSize
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
func (sp *slottedPage) Init() {
	sp.setSlotCount(0)
	sp.setFreeSpacePointer(uint16(len(sp.data)))
}

// InsertTuple добавляет кортеж и возвращает его SlotID
// Если места на странице не хватает, выполняем compact, если все равно не хватает - ошибка
func (sp *slottedPage) InsertTuple(tuple []byte) (uint16, error) {
	slotID := sp.findSlotID()
	if !sp.isAvailableSpace(slotID, len(tuple)) {
		if !sp.isAvailableTotalSpace(slotID, len(tuple)) {
			return 0, ErrPageFull
		} else {
			sp.compact()
		}
	}
	sp.insertTuple(slotID, tuple)
	return slotID, nil
}

// findSlotID ищет первый свободный слот или возвращает новый слот в конце
func (sp *slottedPage) findSlotID() uint16 {
	slotCount := sp.slotCount()
	for i := range slotCount {
		_, _, flags := sp.unpackSlot(i)
		if flags == slotUnused {
			return uint16(i)
		}
	}

	return slotCount
}

// isAvailableSpace быстрая проверка наличия свободного места на странице
func (sp *slottedPage) isAvailableSpace(slotID uint16, tupleLen int) bool {
	slotCount := sp.slotCount()
	freeSpacePointer := sp.freeSpacePointer()
	slotsEndPointer := headerSize + slotSize*slotCount
	newSlotSize := slotSize
	if slotID < slotCount {
		newSlotSize = 0
	}
	return (freeSpacePointer - slotsEndPointer) >= uint16(tupleLen+newSlotSize)
}

// isAvailableTotalSpace полная проверка наличия свободного места на странице
func (sp *slottedPage) isAvailableTotalSpace(slotID uint16, tupleLen int) bool {
	slotCount := sp.slotCount()
	var liveTuplesSize uint16
	for i := range slotCount {
		_, length, flags := sp.unpackSlot(i)
		if flags != slotUnused {
			liveTuplesSize += length
		}
	}

	availableTotalSpace := uint16(len(sp.data)) - uint16(headerSize) - (slotCount * slotSize) - liveTuplesSize
	newSlotSize := slotSize
	if slotID < slotCount {
		newSlotSize = 0
	}
	return availableTotalSpace >= uint16(tupleLen+newSlotSize)
}

func (sp *slottedPage) insertTuple(slotID uint16, tuple []byte) {
	slotCount := sp.slotCount()
	freeSpacePointer := sp.freeSpacePointer()
	newSlotPointer := headerSize + slotSize*slotID

	// Вставляем кортеж
	copy(sp.data[freeSpacePointer-uint16(len(tuple)):freeSpacePointer], tuple)

	// Вставляем слот
	writeSlot(freeSpacePointer-uint16(len(tuple)), len(tuple), slotUsed, sp.data[newSlotPointer:newSlotPointer+slotSize])

	// Обновляем заголовки
	sp.setFreeSpacePointer(freeSpacePointer - uint16(len(tuple)))
	if slotID >= slotCount {
		sp.setSlotCount(slotCount + 1)
	}
}

func (sp *slottedPage) compact() {
	type usedTuple struct {
		slotID uint16
		flags  slotFlag
		tuple  []byte
	}
	var usedTuples []usedTuple

	// Собираем все живые кортежи
	for i := range sp.slotCount() {
		offset, length, flags := sp.unpackSlot(i)
		if flags != slotUnused {
			// TODO переделать на единственную аллокацию буфера
			tupleCopy := make([]byte, length)
			copy(tupleCopy, sp.data[offset:offset+length])
			usedTuples = append(usedTuples, usedTuple{
				slotID: uint16(i),
				flags:  flags,
				//tuple:  sp.data[offset : offset+length],
				tuple: tupleCopy,
			})
		}
	}

	// Перезаписываем страницу
	freeSpacePointer := uint16(len(sp.data))
	sp.setFreeSpacePointer(freeSpacePointer)
	for _, usedTuple := range usedTuples {
		copy(sp.data[freeSpacePointer-uint16(len(usedTuple.tuple)):freeSpacePointer], usedTuple.tuple)

		pointerToSlot := headerSize + slotSize*usedTuple.slotID
		writeSlot(freeSpacePointer-uint16(len(usedTuple.tuple)), len(usedTuple.tuple), usedTuple.flags, sp.data[pointerToSlot:pointerToSlot+slotSize])

		freeSpacePointer -= uint16(len(usedTuple.tuple))
	}
	sp.setFreeSpacePointer(freeSpacePointer)
}

// GetTuple возвращает данные кортежа по SlotID
func (sp *slottedPage) GetTuple(slotID uint16) ([]byte, error) {
	if slotID > sp.slotCount() {
		return nil, fmt.Errorf("slotID %d is out of bounds", slotID)
	}

	offset, length, _ := sp.unpackSlot(slotID)
	return sp.data[offset : offset+length], nil
}

// DeleteTuple помечает слот как пустой
func (sp *slottedPage) DeleteTuple(slotID uint16) error {
	return sp.setFlagToSlot(slotID, slotDead)
}

// SetTupleAsUnused помечает слот как неиспользуемый
func (sp *slottedPage) SetTupleAsUnused(slotID uint16) error {
	return sp.setFlagToSlot(slotID, slotUnused)
}

func (sp *slottedPage) setFlagToSlot(slotID uint16, flag slotFlag) error {
	if slotID > sp.slotCount() {
		return fmt.Errorf("slotID %d is out of bounds", slotID)
	}

	pointerToSlot := headerSize + slotSize*slotID
	slot := sp.data[pointerToSlot : pointerToSlot+slotSize]
	val := binary.LittleEndian.Uint32(slot)
	val = val &^ 3           // маска для очистки двух младших бит
	val = val | uint32(flag) // установка флага
	binary.LittleEndian.PutUint32(slot, val)
	return nil
}

func (sp *slottedPage) setSlotCount(c uint16) {
	binary.LittleEndian.PutUint16(sp.data[slotCountOffset:slotCountOffset+slotCountSize], c)
}

func (sp *slottedPage) slotCount() uint16 {
	return binary.LittleEndian.Uint16(sp.data[slotCountOffset : slotCountOffset+slotCountSize])
}

func (sp *slottedPage) setFreeSpacePointer(p uint16) {
	binary.LittleEndian.PutUint16(sp.data[freeSpacePointerOffset:freeSpacePointerOffset+freeSpacePointerSize], p)
}

func (sp *slottedPage) freeSpacePointer() uint16 {
	return binary.LittleEndian.Uint16(sp.data[freeSpacePointerOffset : freeSpacePointerOffset+freeSpacePointerSize])
}

// unpackSlot распаковывает слот
func (sp *slottedPage) unpackSlot(slotID uint16) (offset uint16, length uint16, flags slotFlag) {
	pointerToSlot := headerSize + slotSize*slotID
	slot := sp.data[pointerToSlot : pointerToSlot+slotSize]
	val := binary.LittleEndian.Uint32(slot)

	offset = uint16(val >> 17)
	length = uint16(val>>2) & 0x7FFF // маска для 15 бит
	flags = slotFlag(val) & 3        // маска для 2 бит
	return
}

// writeSlot формирует слот
func writeSlot(offset uint16, length int, flags slotFlag, data []byte) {
	// Схема упаковки:
	// [ Offset (15 бит) ] [ Length (15 бит) ] [ Flags (2 бита) ]
	// Биты: 31.........17 16................2 1................0
	packed := (uint32(offset) << 17) | (uint32(length) << 2) | uint32(flags)
	binary.LittleEndian.PutUint32(data, packed)
}
