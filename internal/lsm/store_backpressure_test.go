package lsm

import (
	"errors"
	"testing"
)

func TestFlushAllDrainsQueuedImmutables(t *testing.T) {
	cfg := tinyMemtableConfig(t)
	cfg.MaxImmutableTables = 2

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	// Directly create queued immutable tables. This is safe because this test
	// is in package lsm, so it can test Store's internal queue behavior
	// without adding a test-only field to production code.
	first := newMemtable()
	first.Put([]byte("a"), []byte("1"), 1)

	second := newMemtable()
	second.Put([]byte("b"), []byte("2"), 2)

	store.immutables = []*Memtable{first, second}

	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if len(store.immutables) != 0 {
		t.Fatalf("expected no queued immutables, got %d", len(store.immutables))
	}

	if len(store.manifest.Tables) != 2 {
		t.Fatalf("expected 2 manifest tables, got %d", len(store.manifest.Tables))
	}

	value, found, err := store.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if !found || string(value) != "1" {
		t.Fatalf("expected a=1 after FlushAll, got value=%q found=%v", value, found)
	}

	value, found, err = store.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get b: %v", err)
	}
	if !found || string(value) != "2" {
		t.Fatalf("expected b=2 after FlushAll, got value=%q found=%v", value, found)
	}
}

func TestFlushAllAfterCloseReturnsStoreClosed(t *testing.T) {
	cfg := testConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = store.FlushAll()
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
}
