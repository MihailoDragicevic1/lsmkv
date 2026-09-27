package lsm

import "testing"

func TestPickSizeTieredSimilarSizes(t *testing.T) {
	tables := []ManifestTable{
		{ID: 1, FileSize: 1000},
		{ID: 2, FileSize: 1100},
		{ID: 3, FileSize: 1050},
		{ID: 4, FileSize: 5000}, // outlier
	}
	picked, ok := pickSizeTiered(tables, 3, 2.0)
	if !ok {
		t.Fatal("expected a pick")
	}
	if len(picked) != 3 {
		t.Fatalf("len=%d want 3", len(picked))
	}

	ids := map[uint64]bool{}
	for _, p := range picked {
		ids[p.ID] = true
	}
	if ids[4] {
		t.Fatalf("should not pick outlier id=4: %+v", picked)
	}
	if !ids[1] || !ids[2] || !ids[3] {
		t.Fatalf("expected ids 1,2,3 got %+v", picked)
	}
	// Newest-first among the pick.
	if picked[0].ID != 3 {
		t.Fatalf("newest-first: first id=%d want 3; picked=%+v", picked[0].ID, picked)
	}
}

func TestPickSizeTieredFallbackNewest(t *testing.T) {
	// Sizes all far apart → no size window of K=4; only 3 tables → all 3, newest-first.
	tables := []ManifestTable{
		{ID: 10, FileSize: 100},
		{ID: 20, FileSize: 10_000},
		{ID: 30, FileSize: 1_000_000},
	}
	picked, ok := pickSizeTiered(tables, 4, 2.0)
	if !ok || len(picked) != 3 {
		t.Fatalf("ok=%v len=%d picked=%+v", ok, len(picked), picked)
	}
	if picked[0].ID != 30 {
		t.Fatalf("newest-first: first id=%d want 30", picked[0].ID)
	}
}

func TestPickSizeTieredNeedTwo(t *testing.T) {
	_, ok := pickSizeTiered([]ManifestTable{{ID: 1, FileSize: 10}}, 4, 2.0)
	if ok {
		t.Fatal("single table must not pick")
	}
}

func TestMergeEntriesNewestWinsAndKeepsTombstone(t *testing.T) {
	// mergeEntries(sets...): later logic keeps highest seqNo; tombstones kept.
	// Call with newer set first so equal-seq ties prefer newer table if your
	// merge uses first-seen order — highest seq still dominates here.
	old := []sstEntry{
		{key: []byte("a"), value: []byte("old"), seqNo: 1},
		{key: []byte("b"), value: []byte("b1"), seqNo: 2},
	}
	neu := []sstEntry{
		{key: []byte("a"), value: nil, seqNo: 5, tombstone: true},
		{key: []byte("b"), value: []byte("b2"), seqNo: 4},
	}

	out := mergeEntries(neu, old)
	if len(out) != 2 {
		t.Fatalf("len=%d want 2; out=%+v", len(out), out)
	}

	by := map[string]sstEntry{}
	for _, e := range out {
		by[string(e.key)] = e
	}

	a, ok := by["a"]
	if !ok || !a.tombstone || a.seqNo != 5 {
		t.Fatalf("a: %+v", a)
	}
	b, ok := by["b"]
	if !ok || string(b.value) != "b2" || b.seqNo != 4 {
		t.Fatalf("b: %+v", b)
	}
}
