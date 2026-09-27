package lsm

import (
	"testing"
	"time"
)

func tinyMemtableConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1
	return cfg
}

// waitForFlush waits until background flush has drained all immutables
// and published at least wantSST SSTables, or fails the test on timeout.
func waitForFlush(t *testing.T, s *Store, wantSST int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := s.Stats()
		if st.ImmutablesCount == 0 && st.SSTCount >= wantSST {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	st := s.Stats()
	t.Fatalf(
		"timed out waiting for flush: immutables=%d sst=%d want_sst>=%d status=%s",
		st.ImmutablesCount,
		st.SSTCount,
		wantSST,
		st.EngineStatus,
	)
}

func TestPutTriggersFlushAndPublishesSSTable(t *testing.T) {
	cfg := testConfig(t)
	// Tiny memtable so one Put rotates immediately.
	cfg.MemtableMaxBytes = 1

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Flush is background now — wait instead of asserting instantly.
	waitForFlush(t, s, 1)

	st := s.Stats()
	if st.ImmutablesCount != 0 {
		t.Fatalf("expected no pending immutables after successful flush, got %d", st.ImmutablesCount)
	}
	if st.SSTCount != 1 {
		t.Fatalf("expected SSTCount=1, got %d", st.SSTCount)
	}

	got, found, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatalf("expected key present after flush")
	}
	if string(got) != "v" {
		t.Fatalf("got %q, want %q", got, "v")
	}
}

func TestGetFindsValueAfterFlush(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, found, err := store.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected flushed key to be found")
	}
	if string(val) != "v" {
		t.Fatalf("expected value 'v', got %q", val)
	}
}

func TestDeleteFlushPublishesTombstone(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("gone"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete([]byte("gone")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := store.Get([]byte("gone"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("expected tombstoned key to be hidden after flush")
	}
}

func TestReopenLoadsFlushedSSTable(t *testing.T) {
	cfg := tinyMemtableConfig(t)

	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}

	if err := s1.Put([]byte("persisted"), []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	val, found, err := s2.Get([]byte("persisted"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found {
		t.Fatal("expected reopened store to find flushed key")
	}
	if string(val) != "yes" {
		t.Fatalf("expected 'yes', got %q", val)
	}
}

func TestStatsReflectFlushedTable(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("stats-key"), []byte("stats-val")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	waitForFlush(t, s, 1)

	st := s.Stats()
	if st.SSTCount != 1 {
		t.Fatalf("expected SSTCount=1, got %d", st.SSTCount)
	}
	if st.SSTTotalBytes <= 0 {
		t.Fatalf("expected SSTTotalBytes > 0, got %d", st.SSTTotalBytes)
	}
	if st.ImmutablesCount != 0 {
		t.Fatalf("expected ImmutablesCount=0, got %d", st.ImmutablesCount)
	}
}
