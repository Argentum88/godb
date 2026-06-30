package table

import (
	"testing"
)

func Test_Tuple_SerializeDeserialize(t *testing.T) {
	schema := &Schema{columns: []Column{
		{Name: "txt0", TypeID: TEXT},
		{Name: "int1", TypeID: INTEGER},
		{Name: "txt2", TypeID: TEXT},
		{Name: "int3", TypeID: INTEGER},
		{Name: "txt4", TypeID: TEXT},
		{Name: "int5", TypeID: INTEGER},
		{Name: "txt6", TypeID: TEXT},
		{Name: "int7", TypeID: INTEGER},
		{Name: "txt8", TypeID: TEXT},
	}}
	tpl := NewTuple([]Value{
		&TextValue{value: "0"},
		&IntegerValue{value: 1},
		&TextValue{value: "2"},
		nil,
		&TextValue{value: "4"},
		&IntegerValue{value: 5},
		&TextValue{value: "6"},
		&IntegerValue{value: 7},
		nil,
	})

	deserializedTpl, err := NewTupleFromBytes(schema, tpl.Serialize())
	if err != nil {
		t.Fatalf("Failed to deserialize tuple: %v", err)
	}

	if deserializedTpl.Get(4).String() != "4" {
		t.Errorf("Expected value at index 4 to be '4', got '%s'", deserializedTpl.Get(4).String())
	}
	if deserializedTpl.Get(5).String() != "5" {
		t.Errorf("Expected value at index 5 to be '5', got '%s'", deserializedTpl.Get(5).String())
	}
	if !deserializedTpl.IsNull(3) {
		t.Errorf("Expected value at index 3 to be NULL, but it is not")
	}
	if !deserializedTpl.IsNull(8) {
		t.Errorf("Expected value at index 8 to be NULL, but it is not")
	}
}
