package shell_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Argentum88/godb/internal/executor"
	"github.com/Argentum88/godb/internal/shell"
	"github.com/Argentum88/godb/internal/storage"
)

func TestShell_Commands(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		commands []string
		expected []string
	}{
		{
			name:     "set and get",
			commands: []string{"set foo bar", "get foo", "exit"},
			expected: []string{"OK", "bar"},
		},
		{
			name:     "get non-existent",
			commands: []string{"get missing", "exit"},
			expected: []string{"Error:", "key not found"},
		},
		{
			name:     "unknown command",
			commands: []string{"delete foo", "exit"},
			expected: []string{"Error:", "unknown command"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			engine := storage.NewInMemoryKVEngine()
			exec := executor.NewKVExecutor(engine)
			sh := shell.NewShell(exec)

			input := bytes.NewBufferString(strings.Join(tt.commands, "\n") + "\n")
			output := &bytes.Buffer{}

			err := sh.Run(context.Background(), input, output)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			outputStr := output.String()
			for _, expected := range tt.expected {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("expected substring %q not found in output:\n%s", expected, outputStr)
				}
			}
		})
	}
}
