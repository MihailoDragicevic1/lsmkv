package lsm

import "testing"

func TestMergeEntriesKeepsNewestValuePerKey(t *testing.T) {
	older := []sstEntry{
		{
			key:   []byte("alpha"),
			value: []byte("old-alpha"),
			seqNo: 1,
		},
		{
			key:   []byte("bravo"),
			value: []byte("only-in-older"),
			seqNo: 2,
		},
		{
			key:   []byte("charlie"),
			value: []byte("old-charlie"),
			seqNo: 3,
		},
	}

	newer := []sstEntry{
		{
			key:   []byte("alpha"),
			value: []byte("new-alpha"),
			seqNo: 4,
		},
		{
			key:       []byte("charlie"),
			seqNo:     5,
			tombstone: true,
		},
		{
			key:   []byte("delta"),
			value: []byte("only-in-newer"),
			seqNo: 6,
		},
	}

	merged := mergeEntries(older, newer)

	if len(merged) != 4 {
		t.Fatalf("expected 4 merged keys, got %d", len(merged))
	}

	want := []struct {
		key       string
		value     string
		seqNo     uint64
		tombstone bool
	}{
		{key: "alpha", value: "new-alpha", seqNo: 4, tombstone: false},
		{key: "bravo", value: "only-in-older", seqNo: 2, tombstone: false},
		{key: "charlie", value: "", seqNo: 5, tombstone: true},
		{key: "delta", value: "only-in-newer", seqNo: 6, tombstone: false},
	}

	for i, expected := range want {
		got := merged[i]

		if string(got.key) != expected.key {
			t.Fatalf("entry %d: expected key %q, got %q", i, expected.key, got.key)
		}

		if string(got.value) != expected.value {
			t.Fatalf("entry %d: expected value %q, got %q", i, expected.value, got.value)
		}

		if got.seqNo != expected.seqNo {
			t.Fatalf("entry %d: expected seqNo %d, got %d", i, expected.seqNo, got.seqNo)
		}

		if got.tombstone != expected.tombstone {
			t.Fatalf(
				"entry %d: expected tombstone=%v, got tombstone=%v",
				i,
				expected.tombstone,
				got.tombstone,
			)
		}
	}
}

func TestMergeEntriesPreservesNewerTombstone(t *testing.T) {
	older := []sstEntry{
		{
			key:   []byte("deleted-key"),
			value: []byte("old-value"),
			seqNo: 10,
		},
	}

	newer := []sstEntry{
		{
			key:       []byte("deleted-key"),
			seqNo:     11,
			tombstone: true,
		},
	}

	merged := mergeEntries(older, newer)

	if len(merged) != 1 {
		t.Fatalf("expected 1 merged entry, got %d", len(merged))
	}

	if !merged[0].tombstone {
		t.Fatal("expected the newer tombstone to win")
	}

	if merged[0].seqNo != 11 {
		t.Fatalf("expected tombstone seqNo 11, got %d", merged[0].seqNo)
	}
}

func TestMergeEntriesReturnsKeysSorted(t *testing.T) {
	entries := mergeEntries(
		[]sstEntry{
			{key: []byte("zulu"), value: []byte("z"), seqNo: 1},
			{key: []byte("mango"), value: []byte("m"), seqNo: 2},
		},
		[]sstEntry{
			{key: []byte("alpha"), value: []byte("a"), seqNo: 3},
		},
	)

	if len(entries) != 3 {
		t.Fatalf("expected 3 merged entries, got %d", len(entries))
	}

	wantKeys := []string{"alpha", "mango", "zulu"}
	for i, wantKey := range wantKeys {
		if gotKey := string(entries[i].key); gotKey != wantKey {
			t.Fatalf("entry %d: expected key %q, got %q", i, wantKey, gotKey)
		}
	}
}
