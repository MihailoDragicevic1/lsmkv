package lsm

import (
	"path/filepath"
	"testing"
)

func TestSSTableWriterBasic(t *testing.T) {
	cfg := testConfig(t)

	w := NewSSTableWriter(cfg)
	w.Add([]byte("apple"), []byte("1"), 1, false)
	w.Add([]byte("cherry"), []byte("3"), 3, false)
	w.Add([]byte("banana"), []byte("2"), 2, false) // added out of order on purpose
	w.Add([]byte("deleted"), nil, 4, true)         // tombstone

	path := filepath.Join(cfg.DataDir, "000001.sst")
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Verify the file exists and is non-empty.
	info := mustStat(t, path)
	if info.Size() == 0 {
		t.Fatal("expected non-empty SSTable file")
	}

	// Verify no .tmp file was left behind.
	mustNotExist(t, path+".tmp")
}

func TestSSTableWriterEmptyFlushFails(t *testing.T) {
	// Writing a completely empty SSTable is allowed (edge case for empty memtable).
	cfg := testConfig(t)
	w := NewSSTableWriter(cfg)
	path := filepath.Join(cfg.DataDir, "empty.sst")
	// Should not panic or error — even an empty SSTable needs a valid footer.
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush of empty writer: %v", err)
	}
}
