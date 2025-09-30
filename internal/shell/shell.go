package shell

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/Argentum88/godb/internal/executor"
)

type Shell struct {
	executor executor.Executor
}

func NewShell(executor executor.Executor) *Shell {
	return &Shell{executor: executor}
}

func (s *Shell) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)

	for {
		fmt.Fprint(out, "godb> ")

		if !scanner.Scan() {
			break // EOF или ошибка
		}

		cmd := strings.TrimSpace(scanner.Text())
		if cmd == "" {
			continue
		}

		if cmd == "exit" || cmd == "quit" {
			break
		}

		result, err := s.executor.Execute(ctx, cmd)
		if err != nil {
			fmt.Fprintf(out, "Error: %v\n", err)
		} else {
			fmt.Fprintf(out, "%s\n", result.Text)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
