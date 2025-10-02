package main

import (
	"context"
	"os"

	"github.com/Argentum88/godb/internal/executor"
	"github.com/Argentum88/godb/internal/shell"
	"github.com/Argentum88/godb/internal/storage"
)

func main() {
	inMemoryKVEngine := storage.NewInMemoryKVEngine()
	kvExecutor := executor.NewKVExecutor(inMemoryKVEngine)
	sh := shell.NewShell(kvExecutor)
	sh.Run(context.Background(), os.Stdin, os.Stdout)
}
