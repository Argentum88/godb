package storage_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Argentum88/godb/internal/storage"
)

func TestInMemoryKV_SetAndGet(t *testing.T) {
	t.Parallel()
	kv := storage.NewInMemoryKVEngine()
	err := kv.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	value, err := kv.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("Expected value 'value', got '%s'", value)
	}
}

func TestInMemoryKV_Update(t *testing.T) {
	t.Parallel()
	kv := storage.NewInMemoryKVEngine()
	err := kv.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// Check that the initial value is set correctly
	value, err := kv.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after initial Set failed: %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("Expected initial value 'value', got '%s'", value)
	}

	// Update the value
	err = kv.Set([]byte("key"), []byte("newvalue"))
	if err != nil {
		t.Fatalf("Update (Set) failed: %v", err)
	}

	value, err = kv.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if string(value) != "newvalue" {
		t.Fatalf("Expected value 'newvalue', got '%s'", value)
	}
}

func TestInMemoryKV_Get_NonExistentKey(t *testing.T) {
	t.Parallel()
	kv := storage.NewInMemoryKVEngine()
	_, err := kv.Get([]byte("nonexistent"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Expected error '%v', got '%v'", storage.ErrKeyNotFound, err)
	}
}

func TestInMemoryKV_Concurrency(t *testing.T) {
	t.Parallel()
	kv := storage.NewInMemoryKVEngine()
	wg := new(sync.WaitGroup)
	n := 100
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", i)
			value := fmt.Sprintf("value_%d", i)
			kv.Set([]byte(key), []byte(value))

			j := (i + 1) % n
			readKey := fmt.Sprintf("key_%d", j)
			kv.Get([]byte(readKey))
		}(i)
	}
	wg.Wait()

	for i := range n {
		key := fmt.Sprintf("key_%d", i)
		expectedValue := fmt.Sprintf("value_%d", i)

		actualValue, err := kv.Get([]byte(key))
		if err != nil {
			t.Fatalf("Key %s should exist, but Get failed: %v", key, err)
		}

		if string(actualValue) != expectedValue {
			t.Fatalf("For key %s, expected %s, but got %s", key, expectedValue, actualValue)
		}
	}
}
