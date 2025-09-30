package storage

import (
	"sync"
)

type inMemoryKVEngine struct {
	data map[string][]byte
	mtx  sync.RWMutex
}

func NewInMemoryKVEngine() *inMemoryKVEngine {
	return &inMemoryKVEngine{
		data: make(map[string][]byte),
		mtx:  sync.RWMutex{},
	}
}

func (kv *inMemoryKVEngine) Set(key []byte, value []byte) error {
	kv.mtx.Lock()
	defer kv.mtx.Unlock()
	kv.data[string(key)] = value
	return nil
}

func (kv *inMemoryKVEngine) Get(key []byte) ([]byte, error) {
	kv.mtx.RLock()
	defer kv.mtx.RUnlock()
	v, ok := kv.data[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return v, nil
}
