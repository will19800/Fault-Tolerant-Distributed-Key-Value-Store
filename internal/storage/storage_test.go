package storage

import (
	"errors"
	"strconv"
	"sync"
	"testing"
)

func TestStoreLifecycle(t *testing.T) {
	store := New()

	if _, err := store.Get("user:123:preferences"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get missing key error = %v, want %v", err, ErrKeyNotFound)
	}

	if err := store.Put("user:123:preferences", []byte(`{"theme":"light"}`)); err != nil {
		t.Fatalf("Put new key: %v", err)
	}
	if err := store.Put("user:123:preferences", []byte(`{"theme":"dark"}`)); err != nil {
		t.Fatalf("Put existing key: %v", err)
	}

	got, err := store.Get("user:123:preferences")
	if err != nil {
		t.Fatalf("Get existing key: %v", err)
	}
	if want := `{"theme":"dark"}`; string(got) != want {
		t.Fatalf("Get existing key = %q, want %q", got, want)
	}

	if err := store.Delete("user:123:preferences"); err != nil {
		t.Fatalf("Delete existing key: %v", err)
	}
	if err := store.Delete("user:123:preferences"); err != nil {
		t.Fatalf("Delete missing key: %v", err)
	}
	if _, err := store.Get("user:123:preferences"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get deleted key error = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestStoreOwnsValues(t *testing.T) {
	store := New()
	original := []byte("value")
	if err := store.Put("key", original); err != nil {
		t.Fatal(err)
	}
	original[0] = 'X'

	first, err := store.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	first[0] = 'Y'

	second, err := store.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(second), "value"; got != want {
		t.Fatalf("stored value = %q, want %q", got, want)
	}
}

func TestStoreRejectsEmptyKey(t *testing.T) {
	store := New()

	if err := store.Put("", []byte("value")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Put empty key error = %v, want %v", err, ErrEmptyKey)
	}
	if _, err := store.Get(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Get empty key error = %v, want %v", err, ErrEmptyKey)
	}
	if err := store.Delete(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Delete empty key error = %v, want %v", err, ErrEmptyKey)
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	store := New()
	const workers = 32

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "key-" + strconv.Itoa(i)
			if err := store.Put(key, []byte(key)); err != nil {
				t.Errorf("Put(%q): %v", key, err)
				return
			}
			if _, err := store.Get(key); err != nil {
				t.Errorf("Get(%q): %v", key, err)
			}
		}()
	}
	wg.Wait()
}
