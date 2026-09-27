package lsm

import "testing"

func TestStoreCompactMergesFlushedTables(t *testing.T) {
	cfg := testConfig(t)
	// Small memtable so each ForceFlush path is easy to reason about.
	cfg.MemtableMaxBytes = 1
	cfg.MaxImmutableTables = 4

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Build two separate SSTables synchronously via ForceFlush.
	// ForceFlush is intentionally sync; background flush is not relied on here.
	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush 1: %v", err)
	}

	if err := s.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush 2: %v", err)
	}

	st := s.Stats()
	if st.SSTCount != 2 {
		t.Fatalf("expected 2 sstables before compaction, got %d", st.SSTCount)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	st = s.Stats()
	if st.SSTCount != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", st.SSTCount)
	}

	// Values must still be readable from the merged table.
	for key, want := range map[string]string{"a": "1", "b": "2"} {
		got, found, err := s.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if !found {
			t.Fatalf("Get %s: not found after compaction", key)
		}
		if string(got) != want {
			t.Fatalf("Get %s: got %q want %q", key, got, want)
		}
	}
}

func TestStoreCompactNoOpWithOneTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("only"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	value, found, err := store.Get([]byte("only"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "value" {
		t.Fatalf("expected only=value, got value=%q found=%v", value, found)
	}
}
