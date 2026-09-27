package lsm

import "testing"

func TestSSTableAllEntriesReturnsSortedRecords(t *testing.T) {
	cfg := testConfig(t)
	path := cfg.DataDir + "/all-entries.sst"

	writer := NewSSTableWriter(cfg)
	writer.Add([]byte("charlie"), []byte("3"), 3, false)
	writer.Add([]byte("alpha"), []byte("1"), 1, false)
	writer.Add([]byte("bravo"), nil, 2, true)

	if err := writer.Flush(path); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	reader, err := OpenSSTableReader(path)
	if err != nil {
		t.Fatalf("OpenSSTableReader: %v", err)
	}
	defer reader.Close()

	entries, err := reader.AllEntries()
	if err != nil {
		t.Fatalf("AllEntries: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	tests := []struct {
		key       string
		value     string
		seqNo     uint64
		tombstone bool
	}{
		{key: "alpha", value: "1", seqNo: 1, tombstone: false},
		{key: "bravo", value: "", seqNo: 2, tombstone: true},
		{key: "charlie", value: "3", seqNo: 3, tombstone: false},
	}

	for i, want := range tests {
		got := entries[i]

		if string(got.key) != want.key {
			t.Fatalf("entry %d: expected key %q, got %q", i, want.key, got.key)
		}

		if string(got.value) != want.value {
			t.Fatalf("entry %d: expected value %q, got %q", i, want.value, got.value)
		}

		if got.seqNo != want.seqNo {
			t.Fatalf("entry %d: expected seqNo %d, got %d", i, want.seqNo, got.seqNo)
		}

		if got.tombstone != want.tombstone {
			t.Fatalf("entry %d: expected tombstone=%v, got %v", i, want.tombstone, got.tombstone)
		}
	}
}
