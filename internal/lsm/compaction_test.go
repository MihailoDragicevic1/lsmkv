package lsm

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func planCfg(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	// Deterministic picker defaults for unit tests.
	cfg.SizeTieredFanIn = 4
	cfg.SizeTieredSizeRatio = 2.0
	return cfg
}

// --- newCompactionPlan ---

func TestNewCompactionPlanReturnsNilForFewerThanTwoTables(t *testing.T) {
	cfg := planCfg(t)

	tests := []struct {
		name     string
		manifest *Manifest
	}{
		{
			name: "empty manifest",
			manifest: &Manifest{
				Version: 1,
				Epoch:   1,
			},
		},
		{
			name: "one table",
			manifest: &Manifest{
				Version: 1,
				Epoch:   1,
				Tables: []ManifestTable{
					{ID: 1, File: "000001.sst", FileSize: 100},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := newCompactionPlan(test.manifest, cfg)
			if err != nil {
				t.Fatalf("newCompactionPlan: %v", err)
			}
			if plan != nil {
				t.Fatalf("expected no plan, got %+v", plan)
			}
		})
	}
}

func TestNewCompactionPlanPicksSizeTieredSet(t *testing.T) {
	cfg := planCfg(t)
	cfg.SizeTieredFanIn = 2
	cfg.SizeTieredSizeRatio = 2.0

	// Four similar-sized tables → fan-in 2 picks a size window of 2.
	// With equal sizes, sort is size asc then ID desc; first window is the
	// two smallest — all equal size, so first window is IDs sorted by ID desc
	// among full sort: IDs 4,3,2,1 by ID within same size → sorted order
	// starts with highest IDs first when sizes equal...
	// Actually equal FileSize: sort by ID descending → [4,3,2,1], window of 2 = [4,3].
	manifest := &Manifest{
		Version: 1,
		Epoch:   7,
		Tables: []ManifestTable{
			{ID: 4, File: "000004.sst", FileSize: 1000},
			{ID: 3, File: "000003.sst", FileSize: 1000},
			{ID: 2, File: "000002.sst", FileSize: 1000},
			{ID: 1, File: "000001.sst", FileSize: 1000},
		},
	}

	plan, err := newCompactionPlan(manifest, cfg)
	if err != nil {
		t.Fatalf("newCompactionPlan: %v", err)
	}
	if plan == nil {
		t.Fatal("expected a compaction plan, got nil")
	}
	if len(plan.Inputs) != 2 {
		t.Fatalf("expected 2 inputs (fan_in=2), got %d: %+v", len(plan.Inputs), plan.Inputs)
	}
	// Newest-first among pick.
	if plan.Inputs[0].ID < plan.Inputs[1].ID {
		t.Fatalf("expected newest-first inputs, got [%d %d]", plan.Inputs[0].ID, plan.Inputs[1].ID)
	}
	if plan.OutputID != 5 {
		t.Fatalf("expected output ID 5, got %d", plan.OutputID)
	}
	if plan.OutputFile != "000005.sst" {
		t.Fatalf("expected output file 000005.sst, got %q", plan.OutputFile)
	}
}

func TestNewCompactionPlanRejectsNilManifest(t *testing.T) {
	cfg := planCfg(t)
	_, err := newCompactionPlan(nil, cfg)
	if err == nil {
		t.Fatal("expected an error for a nil manifest")
	}
}

// --- compactReaders ---

func TestCompactReadersWritesMergedSSTable(t *testing.T) {
	cfg := testConfig(t)

	olderPath := filepath.Join(cfg.DataDir, "older.sst")
	newerPath := filepath.Join(cfg.DataDir, "newer.sst")
	outputPath := filepath.Join(cfg.DataDir, "compacted.sst")

	olderWriter := NewSSTableWriter(cfg)
	olderWriter.Add([]byte("alpha"), []byte("old-alpha"), 1, false)
	olderWriter.Add([]byte("bravo"), []byte("only-in-older"), 2, false)
	olderWriter.Add([]byte("charlie"), []byte("old-charlie"), 3, false)
	if err := olderWriter.Flush(olderPath); err != nil {
		t.Fatalf("flush older table: %v", err)
	}

	newerWriter := NewSSTableWriter(cfg)
	newerWriter.Add([]byte("alpha"), []byte("new-alpha"), 4, false)
	newerWriter.Add([]byte("charlie"), nil, 5, true)
	newerWriter.Add([]byte("delta"), []byte("only-in-newer"), 6, false)
	if err := newerWriter.Flush(newerPath); err != nil {
		t.Fatalf("flush newer table: %v", err)
	}

	olderReader, err := OpenSSTableReader(olderPath)
	if err != nil {
		t.Fatalf("open older table: %v", err)
	}
	defer olderReader.Close()

	newerReader, err := OpenSSTableReader(newerPath)
	if err != nil {
		t.Fatalf("open newer table: %v", err)
	}
	defer newerReader.Close()

	// Newest reader first (matches runCompactionOnce / plan.Inputs order).
	compacted, err := compactReaders(outputPath, cfg, newerReader, olderReader)
	if err != nil {
		t.Fatalf("compactReaders: %v", err)
	}
	defer compacted.Close()

	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("expected compacted output file: %v", err)
	}

	tests := []struct {
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

	for _, test := range tests {
		entry, err := compacted.Get([]byte(test.key))
		if err != nil {
			t.Fatalf("Get %q: %v", test.key, err)
		}
		if string(entry.value) != test.value {
			t.Fatalf("Get %q: expected value %q, got %q", test.key, test.value, entry.value)
		}
		if entry.seqNo != test.seqNo {
			t.Fatalf("Get %q: expected seqNo %d, got %d", test.key, test.seqNo, entry.seqNo)
		}
		if entry.tombstone != test.tombstone {
			t.Fatalf("Get %q: expected tombstone=%v, got %v", test.key, test.tombstone, entry.tombstone)
		}
	}
}

func TestCompactReadersRejectsNoReaders(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "unused.sst")

	_, err := compactReaders(outputPath, cfg)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestCompactReadersRejectsNilReader(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "unused.sst")

	_, err := compactReaders(outputPath, cfg, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

// --- manifestAfterCompaction ---

func TestManifestAfterCompactionReplacesInputTables(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "000005.sst")

	outputBytes := []byte("compacted output")
	if err := os.WriteFile(outputPath, outputBytes, 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	current := &Manifest{
		Version: 1,
		Epoch:   7,
		Tables: []ManifestTable{
			{ID: 4, File: "000004.sst"},
			{ID: 3, File: "000003.sst"},
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"},
		},
	}

	plan := &compactionPlan{
		Inputs: []ManifestTable{
			{ID: 2, File: "000002.sst"},
			{ID: 1, File: "000001.sst"},
		},
		OutputID:   5,
		OutputFile: "000005.sst",
	}

	outputEntries := []sstEntry{
		{key: []byte("alpha"), value: []byte("1"), seqNo: 1},
		{key: []byte("bravo"), value: nil, seqNo: 2, tombstone: true},
		{key: []byte("charlie"), value: []byte("3"), seqNo: 3},
	}

	next, err := manifestAfterCompaction(current, plan, outputPath, outputEntries)
	if err != nil {
		t.Fatalf("manifestAfterCompaction: %v", err)
	}

	if next.Version != current.Version {
		t.Fatalf("expected version %d, got %d", current.Version, next.Version)
	}
	if next.Epoch != 8 {
		t.Fatalf("expected epoch 8, got %d", next.Epoch)
	}
	if len(next.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(next.Tables))
	}

	wantIDs := []uint64{5, 4, 3}
	for i, wantID := range wantIDs {
		if next.Tables[i].ID != wantID {
			t.Fatalf("table %d: expected ID %d, got %d", i, wantID, next.Tables[i].ID)
		}
	}

	output := next.Tables[0]
	if output.File != "000005.sst" {
		t.Fatalf("expected output file 000005.sst, got %q", output.File)
	}
	if output.MinKey != "alpha" || output.MaxKey != "charlie" {
		t.Fatalf("keys: min=%q max=%q", output.MinKey, output.MaxKey)
	}
	if output.MinSeqNo != 1 || output.MaxSeqNo != 3 {
		t.Fatalf("seq: min=%d max=%d", output.MinSeqNo, output.MaxSeqNo)
	}
	if output.FileSize != int64(len(outputBytes)) {
		t.Fatalf("expected size %d, got %d", len(outputBytes), output.FileSize)
	}

	if len(current.Tables) != 4 || current.Epoch != 7 {
		t.Fatal("current manifest was mutated")
	}
}

func TestManifestAfterCompactionRejectsInvalidInputs(t *testing.T) {
	cfg := testConfig(t)
	outputPath := filepath.Join(cfg.DataDir, "000005.sst")

	if err := os.WriteFile(outputPath, []byte("output"), 0o644); err != nil {
		t.Fatalf("write output file: %v", err)
	}

	current := &Manifest{Version: 1}
	plan := &compactionPlan{
		Inputs: []ManifestTable{
			{ID: 1, File: "000001.sst"},
		},
		OutputID:   2,
		OutputFile: "000002.sst",
	}
	entries := []sstEntry{
		{key: []byte("key"), value: []byte("value"), seqNo: 1},
	}

	tests := []struct {
		name    string
		current *Manifest
		plan    *compactionPlan
		path    string
		entries []sstEntry
	}{
		{name: "nil manifest", current: nil, plan: plan, path: outputPath, entries: entries},
		{name: "nil plan", current: current, plan: nil, path: outputPath, entries: entries},
		{
			name:    "empty plan inputs",
			current: current,
			plan:    &compactionPlan{OutputID: 2, OutputFile: "000002.sst"},
			path:    outputPath,
			entries: entries,
		},
		{name: "empty output entries", current: current, plan: plan, path: outputPath, entries: nil},
		{
			name:    "missing output file",
			current: current,
			plan:    plan,
			path:    filepath.Join(cfg.DataDir, "missing.sst"),
			entries: entries,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := manifestAfterCompaction(test.current, test.plan, test.path, test.entries); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

// --- runCompactionOnce ---

func writeTestSSTable(t *testing.T, cfg Config, path string, entries []sstEntry) {
	t.Helper()
	writer := NewSSTableWriter(cfg)
	for _, e := range entries {
		writer.Add(e.key, e.value, e.seqNo, e.tombstone)
	}
	if err := writer.Flush(path); err != nil {
		t.Fatalf("flush %s: %v", path, err)
	}
}

func TestRunCompactionOnceMergesTwoTables(t *testing.T) {
	cfg := planCfg(t)
	cfg.SizeTieredFanIn = 2

	p1 := filepath.Join(cfg.DataDir, "000001.sst")
	p2 := filepath.Join(cfg.DataDir, "000002.sst")

	writeTestSSTable(t, cfg, p1, []sstEntry{
		{key: []byte("alpha"), value: []byte("old-alpha"), seqNo: 1},
		{key: []byte("bravo"), value: []byte("only-in-1"), seqNo: 2},
	})
	writeTestSSTable(t, cfg, p2, []sstEntry{
		{key: []byte("alpha"), value: []byte("new-alpha"), seqNo: 3},
	})

	// Real sizes from disk so the picker sees similar sizes.
	fi1, err := os.Stat(p1)
	if err != nil {
		t.Fatal(err)
	}
	fi2, err := os.Stat(p2)
	if err != nil {
		t.Fatal(err)
	}

	current := &Manifest{
		Version: 1,
		Epoch:   1,
		Tables: []ManifestTable{
			{ID: 2, File: "000002.sst", FileSize: fi2.Size()},
			{ID: 1, File: "000001.sst", FileSize: fi1.Size()},
		},
	}

	next, err := runCompactionOnce(cfg, current, &Metrics{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("runCompactionOnce: %v", err)
	}

	if len(next.Tables) != 1 {
		t.Fatalf("expected 1 table after compaction, got %d", len(next.Tables))
	}
	if next.Tables[0].ID != 3 {
		t.Fatalf("expected output table ID 3, got %d", next.Tables[0].ID)
	}
	if next.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", next.Epoch)
	}

	if _, err := os.Stat(p1); !os.IsNotExist(err) {
		t.Fatalf("expected 000001.sst removed, stat err: %v", err)
	}
	if _, err := os.Stat(p2); !os.IsNotExist(err) {
		t.Fatalf("expected 000002.sst removed, stat err: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, next.Tables[0].File)); err != nil {
		t.Fatalf("expected output file: %v", err)
	}

	savedManifest, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if savedManifest.Epoch != 2 || len(savedManifest.Tables) != 1 {
		t.Fatalf("manifest on disk not updated: %+v", savedManifest)
	}

	reader, err := OpenSSTableReader(filepath.Join(cfg.DataDir, next.Tables[0].File))
	if err != nil {
		t.Fatalf("open compacted table: %v", err)
	}
	defer reader.Close()

	entry, err := reader.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get alpha: %v", err)
	}
	if string(entry.value) != "new-alpha" {
		t.Fatalf("expected new-alpha, got %q", entry.value)
	}

	entry, err = reader.Get([]byte("bravo"))
	if err != nil {
		t.Fatalf("Get bravo: %v", err)
	}
	if string(entry.value) != "only-in-1" {
		t.Fatalf("expected only-in-1, got %q", entry.value)
	}
}

func TestRunCompactionOnceNoOpWithFewerThanTwoTables(t *testing.T) {
	cfg := planCfg(t)

	current := &Manifest{
		Version: 1,
		Epoch:   5,
		Tables: []ManifestTable{
			{ID: 1, File: "000001.sst", FileSize: 100},
		},
	}

	next, err := runCompactionOnce(cfg, current, &Metrics{},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("runCompactionOnce: %v", err)
	}
	if next != current {
		t.Fatal("expected the same manifest pointer as no-op")
	}
}

func TestCompactFourTablesSizeTiered(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 64 << 20
	cfg.SizeTieredFanIn = 4
	cfg.SizeTieredSizeRatio = 2.0
	cfg.L0CompactionTrigger = 0
	cfg.L0StopWrites = 0

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i, kv := range []struct{ k, v string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"},
	} {
		if err := s.Put([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if err := s.ForceFlush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}
	if got := s.Stats().SSTCount; got != 4 {
		t.Fatalf("sst=%d want 4", got)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if got := s.Stats().SSTCount; got != 1 {
		t.Fatalf("after compact sst=%d want 1", got)
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		_, ok, err := s.Get([]byte(k))
		if err != nil || !ok {
			t.Fatalf("get %s: ok=%v err=%v", k, ok, err)
		}
	}
}

func TestWriteStallWhenTooManySST(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 64 << 20
	cfg.L0CompactionTrigger = 0
	cfg.L0StopWrites = 2
	cfg.SizeTieredFanIn = 4

	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if s.Stats().SSTCount < 2 {
		t.Fatalf("need >=2 sst, got %d", s.Stats().SSTCount)
	}

	err = s.Put([]byte("c"), []byte("3"))
	if err == nil || !errors.Is(err, ErrWriteStall) {
		t.Fatalf("want ErrWriteStall, got %v", err)
	}

	if err := s.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if err := s.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("put after compact: %v", err)
	}
}
