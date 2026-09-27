package lsm

import "testing"

func TestForceFlushWritesActiveMemtableToSSTable(t *testing.T) {
	cfg := testConfig(t)

	// Keep memtable huge so Put alone never auto-rotates.
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := store.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("put b: %v", err)
	}

	// Before force flush: data only in active memtable, no SSTables.
	stats := store.Stats()
	if stats.SSTCount != 0 {
		t.Fatalf("expected 0 sst before force flush, got %d", stats.SSTCount)
	}
	if stats.ActiveEntries != 2 {
		t.Fatalf("expected 2 active entries before force flush, got %d", stats.ActiveEntries)
	}

	if err := store.ForceFlush(); err != nil {
		t.Fatalf("force flush: %v", err)
	}

	stats = store.Stats()
	if stats.SSTCount != 1 {
		t.Fatalf("expected 1 sst after force flush, got %d", stats.SSTCount)
	}
	if stats.ActiveEntries != 0 {
		t.Fatalf("expected empty active memtable after force flush, got %d entries", stats.ActiveEntries)
	}
	if stats.ImmutablesCount != 0 {
		t.Fatalf("expected no immutables after force flush, got %d", stats.ImmutablesCount)
	}

	// Reads must still work via the published SSTable.
	got, found, err := store.Get([]byte("a"))
	if err != nil || !found || string(got) != "1" {
		t.Fatalf("get a after force flush: got=%q found=%v err=%v", got, found, err)
	}
	got, found, err = store.Get([]byte("b"))
	if err != nil || !found || string(got) != "2" {
		t.Fatalf("get b after force flush: got=%q found=%v err=%v", got, found, err)
	}
}

func TestForceFlushEmptyStoreIsNoOp(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	if err := store.ForceFlush(); err != nil {
		t.Fatalf("force flush empty: %v", err)
	}

	stats := store.Stats()
	if stats.SSTCount != 0 {
		t.Fatalf("expected 0 sst on empty force flush, got %d", stats.SSTCount)
	}
}

func TestForceFlushAfterCloseReturnsStoreClosed(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := store.ForceFlush(); err != ErrStoreClosed {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
}

func TestForceFlushSurvivesReopen(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Put([]byte("persist"), []byte("yes")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.ForceFlush(); err != nil {
		t.Fatalf("force flush: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: value must come from the SSTable loaded via the manifest,
	// not from WAL replay (WAL was reset after flush).
	store2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()

	got, found, err := store2.Get([]byte("persist"))
	if err != nil || !found || string(got) != "yes" {
		t.Fatalf("get after reopen: got=%q found=%v err=%v", got, found, err)
	}

	stats := store2.Stats()
	if stats.SSTCount != 1 {
		t.Fatalf("expected 1 sst after reopen, got %d", stats.SSTCount)
	}
}
