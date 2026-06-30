package table

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

type IntegerValue struct {
	value int64
}

func (iv *IntegerValue) String() string {
	return strconv.FormatInt(iv.value, 10)
}

func (iv *IntegerValue) Serialize(buf []byte) []byte {
	return binary.LittleEndian.AppendUint64(buf, uint64(iv.value))
}

func (iv *IntegerValue) Deserialize(b []byte) int {
	iv.value = int64(binary.LittleEndian.Uint64(b))
	return 8
}

func (iv *IntegerValue) Compare(other Value) (int, error) {
	otherInt, ok := other.(*IntegerValue)
	if !ok {
		return 0, fmt.Errorf("cannot compare INTEGER with %T", other)
	}
	switch {
	case iv.value < otherInt.value:
		return -1, nil
	case iv.value > otherInt.value:
		return 1, nil
	default:
		return 0, nil
	}
}

type TextValue struct {
	value string
}

func (tv *TextValue) String() string {
	return tv.value
}

func (tv *TextValue) Serialize(buf []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(tv.value)))
	return append(buf, []byte(tv.value)...)
}

func (tv *TextValue) Deserialize(b []byte) int {
	vLen := binary.LittleEndian.Uint32(b)
	tv.value = string(b[4 : 4+vLen])
	return 4 + int(vLen)
}

func (tv *TextValue) Compare(other Value) (int, error) {
	otherText, ok := other.(*TextValue)
	if !ok {
		return 0, fmt.Errorf("cannot compare TEXT with %T", other)
	}
	return strings.Compare(tv.value, otherText.value), nil
}
