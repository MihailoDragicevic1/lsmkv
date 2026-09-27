package lsm

import (
	"path/filepath"
	"testing"
)

// writeAndOpenSSTable is a small helper: build a one-table SSTable on disk
// containing the given live entries, and register it (plus a matching
// manifest) so that Open(cfg) will load it into the store's Version.
func writeAndOpenSSTable(t *testing.T, cfg Config, filename string, entries []sstEntry) {
	t.Helper()

	w := NewSSTableWriter(cfg)
	for _, e := range entries {
		w.Add(e.key, e.value, e.seqNo, e.tombstone)
	}

	path := filepath.Join(cfg.DataDir, filename)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush sstable: %v", err)
	}

	m := &Manifest{
		Version: 1,
		Epoch:   1,
		Tables: []ManifestTable{
			{ID: 1, File: filename},
		},
	}
	if err := saveManifest(cfg, m); err != nil {
		t.Fatalf("saveManifest: %v", err)
	}
}

func TestGetFallsThroughToSSTableOnMemtableMiss(t *testing.T) {
	cfg := testConfig(t)

	writeAndOpenSSTable(t, cfg, "000001.sst", []sstEntry{
		{key: []byte("disk-only"), value: []byte("from-disk"), seqNo: 1},
	})

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	val, found, err := store.Get([]byte("disk-only"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key that only exists on disk to be found")
	}
	if string(val) != "from-disk" {
		t.Fatalf("expected 'from-disk', got %q", val)
	}
}

func TestGetMemtableHitWinsOverSSTable(t *testing.T) {
	cfg := testConfig(t)

	writeAndOpenSSTable(t, cfg, "000001.sst", []sstEntry{
		{key: []byte("k"), value: []byte("old-from-disk"), seqNo: 1},
	})

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	if err := store.Put([]byte("k"), []byte("new-from-memtable")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, found, err := store.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if string(val) != "new-from-memtable" {
		t.Fatalf("expected memtable value to win, got %q", val)
	}
}

func TestGetTombstoneInSSTableHidesValue(t *testing.T) {
	cfg := testConfig(t)

	writeAndOpenSSTable(t, cfg, "000001.sst", []sstEntry{
		{key: []byte("deleted"), seqNo: 2, tombstone: true},
	})

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_, found, err := store.Get([]byte("deleted"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("expected tombstoned key on disk to be reported as not found")
	}
}

func TestGetMissingKeyAcrossMemtableAndSSTable(t *testing.T) {
	cfg := testConfig(t)

	writeAndOpenSSTable(t, cfg, "000001.sst", []sstEntry{
		{key: []byte("present"), value: []byte("v"), seqNo: 1},
	})

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()

	_, found, err := store.Get([]byte("totally-absent"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if found {
		t.Fatal("expected key absent from both memtable and sstable to be not found")
	}
}
