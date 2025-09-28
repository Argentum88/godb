package storage

import (
	"errors"
)

type Engine interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
}

var ErrKeyNotFound = errors.New("key not found")