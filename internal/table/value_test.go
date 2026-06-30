package table

import "testing"

func Test_Value_Compare(t *testing.T) {
	tests := []struct {
		name    string
		value   Value
		other   Value
		want    int
		wantErr bool
	}{
		{
			name:  "IntegerValue bigger than other",
			value: &IntegerValue{value: 6},
			other: &IntegerValue{value: 5},
			want:  1,
		},
		{
			name:  "IntegerValue smaller than other",
			value: &IntegerValue{value: 6},
			other: &IntegerValue{value: 7},
			want:  -1,
		},
		{
			name:  "IntegerValue equal to other",
			value: &IntegerValue{value: 6},
			other: &IntegerValue{value: 6},
			want:  0,
		},
		{
			name:    "IntegerValue compare with TextValue",
			value:   &IntegerValue{value: 6},
			other:   &TextValue{value: "text"},
			wantErr: true,
		},
		{
			name:  "TextValue bigger than other",
			value: &TextValue{value: "zebra"},
			other: &TextValue{value: "apple"},
			want:  1,
		},
		{
			name:  "TextValue smaller than other",
			value: &TextValue{value: "apple"},
			other: &TextValue{value: "zebra"},
			want:  -1,
		},
		{
			name:  "TextValue equal to other",
			value: &TextValue{value: "hello"},
			other: &TextValue{value: "hello"},
			want:  0,
		},
		{
			name:    "TextValue compare with IntegerValue",
			value:   &TextValue{value: "text"},
			other:   &IntegerValue{value: 6},
			wantErr: true,
		},
		{
			name:  "TextValue case-sensitive comparison",
			value: &TextValue{value: "Apple"},
			other: &TextValue{value: "apple"},
			want:  -1,
		},
		{
			name:  "TextValue with special characters",
			value: &TextValue{value: "hello!"},
			other: &TextValue{value: "hello"},
			want:  1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.value.Compare(tt.other)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compare() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Compare() got = %v, want %v", got, tt.want)
			}
		})
	}
}
