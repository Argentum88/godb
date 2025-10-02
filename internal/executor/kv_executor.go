package executor

import (
	"context"
	"strings"

	"github.com/Argentum88/godb/internal/storage"
)

type kvExecutor struct {
	engine storage.Engine
}

func NewKVExecutor(engine storage.Engine) *kvExecutor {
	return &kvExecutor{engine: engine}
}

func(e *kvExecutor) Execute(ctx context.Context, cmd string) (Result, error) {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return Result{}, ErrInvalidCommandSyntax
	}
	op := fields[0]
	switch op {
		case "set":
			if len(fields) != 3 {
				return Result{}, ErrInvalidCommandSyntax
			}
			key := []byte(fields[1])
			value := []byte(fields[2])
			if err := e.engine.Set(key, value); err != nil {
				return Result{}, err
			}
			return Result{Text: "OK"}, nil
		case "get":
			if len(fields) != 2 {
				return Result{}, ErrInvalidCommandSyntax
			}
			key := []byte(fields[1])
			value, err := e.engine.Get(key)
			if err != nil {
				return Result{}, err
			}
			return Result{Text: string(value)}, nil
		default:
			return Result{}, ErrUnknownCommand
	}
}
