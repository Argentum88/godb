package table

import (
	"fmt"
	"slices"
)

// TypeID — перечисление типов.
type TypeID uint8

const ReserveBytes = 3

const (
	INVALID TypeID = iota
	INTEGER
	TEXT
)

// Value — интерфейс для любого типизированного значения.
type Value interface {
	// String возвращает человекочитаемое представление значения (для REPL).
	String() string

	// Serialize дописывает байтовое представление значения в конец buf
	// и возвращает расширенный срез.
	Serialize(buf []byte) []byte

	// Deserialize создаёт Value из байтового представления.
	// Должен быть вызван на типе, который соответствует данным байтам.
	Deserialize(buf []byte) int

	// Compare сравнивает текущий объект с другим.
	// Возвращает -1 (<), 0 (==), 1 (>). NULL'ы должны быть отсеяны выше.
	Compare(other Value) (int, error)
}

type Column struct {
	Name   string
	TypeID TypeID
}

type Schema struct {
	columns []Column
}

type Tuple struct {
	values []Value
}

func createValue(t TypeID) (Value, error) {
	switch t {
	case INTEGER:
		return &IntegerValue{}, nil
	case TEXT:
		return &TextValue{}, nil
	default:
		return nil, fmt.Errorf("unsupported type ID: %d", t)
	}
}

func NewTuple(vs []Value) *Tuple {
	return &Tuple{values: vs}
}

func NewTupleFromBytes(sc *Schema, b []byte) (*Tuple, error) {
	nullBitMapSize := ((len(sc.columns) - 1) / 8) + 1
	nullBitMap := b[ReserveBytes : ReserveBytes+nullBitMapSize]
	data := b[ReserveBytes+nullBitMapSize:]
	var values []Value
	for i, column := range sc.columns {
		bitShift := i % 8
		mask := 1 << bitShift
		if (nullBitMap[i/8] & byte(mask)) != 0 {
			values = append(values, nil)
			continue
		}

		value, err := createValue(column.TypeID)
		if err != nil {
			return nil, err
		}
		offset := value.Deserialize(data)
		data = data[offset:]
		values = append(values, value)
	}

	return &Tuple{values: values}, nil
}

func (t *Tuple) Serialize() []byte {
	reserve := make([]byte, ReserveBytes)
	nullBitMap := make([]byte, ((len(t.values)-1)/8)+1)
	var data []byte
	for i, value := range t.values {
		if value == nil {
			bitShift := i % 8
			mask := 1 << bitShift
			nullBitMap[i/8] = nullBitMap[i/8] | byte(mask)
			continue
		}

		data = value.Serialize(data)
	}
	return slices.Concat(reserve, nullBitMap, data)
}

func (t *Tuple) Get(i int) Value {
	return t.values[i]
}

func (t *Tuple) IsNull(i int) bool {
	return t.values[i] == nil
}
