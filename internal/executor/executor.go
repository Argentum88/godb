package executor

import (
	"context"
	"errors"
)

type Result struct { 
	Text string 
}

var ErrInvalidCommandSyntax = errors.New("invalid command syntax")
var ErrUnknownCommand = errors.New("unknown command")

type Executor interface {
	Execute(ctx context.Context, cmd string) (Result, error)
}