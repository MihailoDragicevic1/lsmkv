package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "000001.sst")

	// Write
	w := NewSSTableWriter(cfg)
	w.Add([]byte("apple"), []byte("fruit"), 1, false)
	w.Add([]byte("cherry"), []byte("red"), 3, false)
	w.Add([]byte("banana"), []byte("yellow"), 2, false) // out of order
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Read
	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	tests := []struct {
		key   string
		value string
	}{
		{"apple", "fruit"},
		{"banana", "yellow"},
		{"cherry", "red"},
	}
	for _, tt := range tests {
		entry, err := r.Get([]byte(tt.key))
		if err != nil {
			t.Errorf("Get(%q): %v", tt.key, err)
			continue
		}
		if string(entry.value) != tt.value {
			t.Errorf("Get(%q) = %q, want %q", tt.key, entry.value, tt.value)
		}
		if entry.tombstone {
			t.Errorf("Get(%q): unexpected tombstone", tt.key)
		}
	}
}

func TestSSTableGetMissingKey(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "000002.sst")

	w := NewSSTableWriter(cfg)
	w.Add([]byte("apple"), []byte("fruit"), 1, false)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	_, err = r.Get([]byte("mango"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for missing key, got %v", err)
	}
}

func TestSSTableTombstoneRoundTrip(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "000003.sst")

	w := NewSSTableWriter(cfg)
	w.Add([]byte("deleted"), nil, 5, true)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	entry, err := r.Get([]byte("deleted"))
	if err != nil {
		t.Fatalf("Get(deleted): %v", err)
	}
	if !entry.tombstone {
		t.Error("expected tombstone=true for deleted key")
	}
	if entry.seqNo != 5 {
		t.Errorf("seqNo = %d, want 5", entry.seqNo)
	}
}

func TestSSTableBloomFiltersAbsentKey(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "000004.sst")

	w := NewSSTableWriter(cfg)
	// Write many keys so the bloom filter is well-populated.
	for i := 0; i < 100; i++ {
		w.Add([]byte(fmt.Sprintf("key%04d", i)), []byte("v"), uint64(i), false)
	}
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	// A key that was definitely not written.
	_, err = r.Get([]byte("zzz_not_in_file"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestOpenSSTableReaderTooSmallFile(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "toosmall.sst")

	if err := os.WriteFile(path, []byte{1, 2, 3}, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := OpenSSTableReader(path)
	if err == nil {
		t.Fatal("expected error for too-small SSTable file")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestOpenSSTableReaderCorruptedFooterOffsets(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "badfooter.sst")

	w := NewSSTableWriter(cfg)
	w.Add([]byte("apple"), []byte("fruit"), 1, false)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if len(data) < 16 {
		t.Fatal("test file unexpectedly too small")
	}

	// Put impossible offsets in footer.
	binary.LittleEndian.PutUint64(data[len(data)-16:len(data)-8], uint64(len(data)+100))
	binary.LittleEndian.PutUint64(data[len(data)-8:], uint64(len(data)+200))

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = OpenSSTableReader(path)
	if err == nil {
		t.Fatal("expected error for corrupted footer offsets")
	}
}

func TestSSTableGetCorruptedCRC(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "badcrc.sst")

	w := NewSSTableWriter(cfg)
	w.Add([]byte("apple"), []byte("fruit"), 1, false)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	// Corrupt one byte in the key or value area, not the footer.
	// The first entry starts at byte 0, header is 21 bytes.
	if len(data) < 30 {
		t.Fatal("test SSTable too small to corrupt safely")
	}
	data[25] ^= 0xFF

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	_, err = r.Get([]byte("apple"))
	if err == nil {
		t.Fatal("expected corruption error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestSSTableEmptyFileGetReturnsNotFound(t *testing.T) {
	cfg := testConfig(t)
	path := filepath.Join(cfg.DataDir, "empty.sst")

	w := NewSSTableWriter(cfg)
	if err := w.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}

	_, err = r.Get([]byte("missing"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
