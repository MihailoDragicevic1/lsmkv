package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

// testConfig returns a Config pointing at a fresh temp directory.
// t.TempDir() is automatically cleaned up by the test runner after each test.
func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		DataDir:             t.TempDir(),
		MemtableMaxBytes:    67108864,
		BlockSize:           8192,
		BloomFalsePositive:  0.01,
		WALFsyncEveryN:      1,
		WALSegmentRollBytes: 1024,  //dodato: Ranije: test helper je pravio nepotpun config; WALSegmentRollBytes je ostajao 0.
		Compression:         "off", //dodato: Sada: svaki test koji koristi testConfig(t) dobija validan segment limit od 1024 B.
		LogLevel:            "info",
	}
}

// --- Basic operations ---

func TestPutAndGet(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, found, err := store.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestGetMissingKey(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_, found, err := store.Get([]byte("ghost"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("expected key to be absent")
	}
}

func TestDeleteHidesKey(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete([]byte("key")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := store.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Fatal("expected deleted key to be hidden")
	}
}

func TestOverwriteReturnsLatestValue(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_ = store.Put([]byte("k"), []byte("first"))
	_ = store.Put([]byte("k"), []byte("second"))

	val, found, err := store.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if string(val) != "second" {
		t.Fatalf("expected 'second', got %q", val)
	}
}

// --- Crash recovery ---

func TestReopenReplaysPuts(t *testing.T) {
	cfg := testConfig(t)

	// First open: write, close.
	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Put([]byte("persistent"), []byte("yes"))
	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Second open: WAL should replay the put.
	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	val, found, err := s2.Get([]byte("persistent"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found {
		t.Fatal("expected key to survive reopen")
	}
	if string(val) != "yes" {
		t.Fatalf("expected 'yes', got %q", val)
	}
}

func TestReopenReplaysDelete(t *testing.T) {
	cfg := testConfig(t)

	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Put([]byte("gone"), []byte("here"))
	_ = s1.Delete([]byte("gone"))
	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	_, found, err := s2.Get([]byte("gone"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if found {
		t.Fatal("expected deleted key to stay hidden after reopen")
	}
}

func TestReopenRestoresSeqNo(t *testing.T) {
	cfg := testConfig(t)

	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	_ = s1.Put([]byte("a"), []byte("1"))
	_ = s1.Put([]byte("b"), []byte("2"))
	seqAfterFirst := s1.Stats().LastSeqNo
	if err := s1.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()

	if s2.Stats().LastSeqNo != seqAfterFirst {
		t.Fatalf("expected seqNo %d after reopen, got %d",
			seqAfterFirst, s2.Stats().LastSeqNo)
	}

	// Write one more — seqNo must advance beyond the replayed value.
	_ = s2.Put([]byte("c"), []byte("3"))
	if s2.Stats().LastSeqNo <= seqAfterFirst {
		t.Fatal("expected seqNo to advance after new write")
	}
}

// --- Guard rails ---

func TestPutEmptyKeyReturnsError(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte{}, []byte("v")); err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

func TestGetEmptyKeyReturnsError(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if _, _, err := store.Get([]byte{}); err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

func TestDeleteEmptyKeyReturnsError(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Delete([]byte{}); err == nil {
		t.Fatal("expected error for empty key, got nil")
	}
}

func TestOperationsAfterCloseReturnError(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = store.Close()

	if err := store.Put([]byte("k"), []byte("v")); err == nil {
		t.Fatal("expected error after close")
	}
	if _, _, err := store.Get([]byte("k")); err == nil {
		t.Fatal("expected error after close")
	}
	if err := store.Delete([]byte("k")); err == nil {
		t.Fatal("expected error after close")
	}
}

// --- Stats ---

func TestStatsAfterWrites(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_ = store.Put([]byte("x"), []byte("1"))
	_ = store.Put([]byte("y"), []byte("2"))

	stats := store.Stats()
	if stats.LastSeqNo != 2 {
		t.Fatalf("expected LastSeqNo=2, got %d", stats.LastSeqNo)
	}
	if stats.ActiveEntries != 2 {
		t.Fatalf("expected ActiveEntries=2, got %d", stats.ActiveEntries)
	}
	if stats.BytesWritten == 0 {
		t.Fatal("expected BytesWritten > 0")
	}
}

// --- WAL file existence ---

func TestWALFileExistsAfterWrite(t *testing.T) {
	cfg := testConfig(t)
	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	walPath := filepath.Join(cfg.DataDir, "wal", "000001.wal")
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected %s to exist after write: %v", walPath, err)
	}
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info
}

func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", path)
	}
}
