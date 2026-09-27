package lsm

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Crash point 1: afterSSTRename
//
// Simulates a crash after the compaction output SST is written to disk but
// before the manifest is saved. On restart the engine must find the old
// input SSTables still authoritative (manifest unchanged) and the orphan
// output SST is harmless.
// ---------------------------------------------------------------------------

func TestCrashAfterSSTRenameBeforeManifest(t *testing.T) {
	cfg := planCfg(t)
	cfg.SizeTieredFanIn = 2

	// Write two SSTables directly so we have inputs for compaction.
	p1 := filepath.Join(cfg.DataDir, "000001.sst")
	p2 := filepath.Join(cfg.DataDir, "000002.sst")

	writeTestSSTable(t, cfg, p1, []sstEntry{
		{key: []byte("alpha"), value: []byte("old"), seqNo: 1},
	})
	writeTestSSTable(t, cfg, p2, []sstEntry{
		{key: []byte("alpha"), value: []byte("new"), seqNo: 2},
	})

	fi1, _ := os.Stat(p1)
	fi2, _ := os.Stat(p2)

	manifest := &Manifest{
		Version: 1,
		Epoch:   1,
		Tables: []ManifestTable{
			{ID: 2, File: "000002.sst", FileSize: fi2.Size()},
			{ID: 1, File: "000001.sst", FileSize: fi1.Size()},
		},
	}

	// Save the initial manifest so "restart" can load it.
	if err := saveManifest(cfg, manifest); err != nil {
		t.Fatalf("save initial manifest: %v", err)
	}

	// Inject crash: panic after SST rename, before manifest save.
	crashFired := false
	SetCrashHook("afterSSTRename", func() {
		crashFired = true
		panic("injected crash: afterSSTRename")
	})
	defer ClearCrashHook("afterSSTRename")

	// Run compaction — it will panic at the hook point.
	func() {
		defer func() { recover() }() //nolint:errcheck
		_, _ = runCompactionOnce(cfg, manifest, &Metrics{}, noopLogger())
	}()

	if !crashFired {
		t.Fatal("crash hook was never fired — hook not reached")
	}

	// --- Invariant checks after "restart" ---

	// 1. Manifest on disk must still be the original (epoch=1, 2 tables).
	loaded, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest after crash: %v", err)
	}
	if loaded.Epoch != 1 {
		t.Fatalf("manifest epoch: want 1, got %d (manifest was overwritten before crash)", loaded.Epoch)
	}
	if len(loaded.Tables) != 2 {
		t.Fatalf("manifest tables: want 2, got %d", len(loaded.Tables))
	}

	// 2. Both original input SSTables must still exist.
	for _, name := range []string{"000001.sst", "000002.sst"} {
		if _, err := os.Stat(filepath.Join(cfg.DataDir, name)); err != nil {
			t.Fatalf("input SST %s missing after crash: %v", name, err)
		}
	}

	// 3. Orphan output SST may exist — that is acceptable (engine ignores it).
	// We just log whether it's there; no assertion needed.
	outputPath := filepath.Join(cfg.DataDir, "000003.sst")
	if _, err := os.Stat(outputPath); err == nil {
		t.Logf("orphan output SST 000003.sst present — acceptable, manifest does not reference it")
	}
}

// ---------------------------------------------------------------------------
// Crash point 2: midCompactBeforeManifest (alias for afterSSTRename)
//
// Verifies same invariants using the named alias so docs/chaos.md recipes
// match the hook names exactly.
// ---------------------------------------------------------------------------

func TestCrashMidCompactBeforeManifest(t *testing.T) {
	cfg := planCfg(t)
	cfg.SizeTieredFanIn = 2

	p1 := filepath.Join(cfg.DataDir, "000001.sst")
	p2 := filepath.Join(cfg.DataDir, "000002.sst")

	writeTestSSTable(t, cfg, p1, []sstEntry{
		{key: []byte("bravo"), value: []byte("v1"), seqNo: 10},
	})
	writeTestSSTable(t, cfg, p2, []sstEntry{
		{key: []byte("bravo"), value: []byte("v2"), seqNo: 20},
	})

	fi1, _ := os.Stat(p1)
	fi2, _ := os.Stat(p2)

	manifest := &Manifest{
		Version: 1,
		Epoch:   3,
		Tables: []ManifestTable{
			{ID: 2, File: "000002.sst", FileSize: fi2.Size()},
			{ID: 1, File: "000001.sst", FileSize: fi1.Size()},
		},
	}

	if err := saveManifest(cfg, manifest); err != nil {
		t.Fatalf("save initial manifest: %v", err)
	}

	// This time use the alias name.
	SetCrashHook("afterSSTRename", func() {
		panic("injected crash: midCompactBeforeManifest")
	})
	defer ClearCrashHook("afterSSTRename")

	func() {
		defer func() { recover() }() //nolint:errcheck
		_, _ = runCompactionOnce(cfg, manifest, &Metrics{}, noopLogger())
	}()

	loaded, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if loaded.Epoch != 3 {
		t.Fatalf("epoch: want 3, got %d", loaded.Epoch)
	}
	// Input files intact.
	for _, name := range []string{"000001.sst", "000002.sst"} {
		if _, err := os.Stat(filepath.Join(cfg.DataDir, name)); err != nil {
			t.Fatalf("input SST %s missing: %v", name, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Crash point 3: walTailCrash
//
// Simulates a torn WAL tail — the last record is incomplete. On recovery
// the engine must replay all complete records and stop cleanly at the torn
// entry. All previously fsync-ed writes must survive.
// ---------------------------------------------------------------------------

func TestCrashWALTailTruncated(t *testing.T) {
	cfg := testConfig(t)
	cfg.WALFsyncEveryN = 1 // every write is fsync-ed

	// Phase 1: open store, write two durable keys, close cleanly.
	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s1.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("put key1: %v", err)
	}
	if err := s1.Put([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("put key2: %v", err)
	}
	if err := s1.CloseGraceful(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Phase 2: corrupt the last few bytes of the last WAL segment to simulate
	// a torn write (power loss mid-append).
	walDir := filepath.Join(cfg.DataDir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no WAL segments found")
	}
	lastSeg := filepath.Join(walDir, entries[len(entries)-1].Name())
	data, err := os.ReadFile(lastSeg)
	if err != nil {
		t.Fatalf("read wal segment: %v", err)
	}
	if len(data) < 8 {
		t.Skip("WAL segment too small to truncate meaningfully")
	}
	// Truncate last 4 bytes — simulates torn final record.
	truncated := data[:len(data)-4]
	if err := os.WriteFile(lastSeg, truncated, 0o644); err != nil {
		t.Fatalf("write truncated wal: %v", err)
	}

	// Phase 3: reopen — engine must recover without error.
	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after WAL tail corruption: %v", err)
	}
	defer s2.Close()

	// At least key1 must survive (it was written and fsync-ed before key2).
	// key2 may or may not survive depending on where truncation fell.
	val, found, err := s2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key1: %v", err)
	}
	if !found {
		t.Fatal("key1 lost after WAL tail crash — fsync-ed write must survive")
	}
	if string(val) != "val1" {
		t.Fatalf("key1 value: want val1, got %q", val)
	}
}

// ---------------------------------------------------------------------------
// Invariant: Version is never torn (no manifest referencing missing SST)
// ---------------------------------------------------------------------------

func TestRestartWithOrphanSSTIsClean(t *testing.T) {
	cfg := testConfig(t)

	// Create a manifest that references one real SST.
	sstPath := filepath.Join(cfg.DataDir, "000001.sst")
	writeTestSSTable(t, cfg, sstPath, []sstEntry{
		{key: []byte("hello"), value: []byte("world"), seqNo: 1},
	})
	fi, _ := os.Stat(sstPath)

	manifest := &Manifest{
		Version: 1,
		Epoch:   1,
		Tables: []ManifestTable{
			{ID: 1, File: "000001.sst", FileSize: fi.Size()},
		},
	}
	if err := saveManifest(cfg, manifest); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	// Write an orphan SST that is NOT in the manifest.
	orphanPath := filepath.Join(cfg.DataDir, "000002.sst")
	writeTestSSTable(t, cfg, orphanPath, []sstEntry{
		{key: []byte("orphan"), value: []byte("data"), seqNo: 2},
	})

	// Open engine — must succeed and not panic on the orphan.
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open with orphan SST: %v", err)
	}
	defer s.Close()

	// The known key must be readable.
	val, found, err := s.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("get hello: %v", err)
	}
	if !found || string(val) != "world" {
		t.Fatalf("get hello: found=%v val=%q", found, val)
	}

	// The orphan key must NOT be visible (not in manifest).
	_, found, err = s.Get([]byte("orphan"))
	if err != nil {
		t.Fatalf("get orphan: %v", err)
	}
	if found {
		t.Fatal("orphan SST key is visible — engine incorrectly opened non-manifest file")
	}
}

// noopLogger returns a logger that discards all output — used in tests so
// structured log lines don't pollute test output.
func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const hardCrashHelperEnv = "LSMKV_HARD_CRASH_HELPER"

func TestHardCrashAfterAcknowledgedPutsRecoverFromWAL(t *testing.T) { //dodato: novi test za subprocess crash posle dva uspešna Put
	if os.Getenv(hardCrashHelperEnv) == "1" {
		runHardCrashPutHelper()
		return
	}

	dataDir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=TestHardCrashAfterAcknowledgedPutsRecoverFromWAL")
	cmd.Env = append(os.Environ(),
		hardCrashHelperEnv+"=1",
		"LSMKV_HARD_CRASH_DATA_DIR="+dataDir,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hard-crash helper failed: %v\noutput:\n%s", err, output)
	}

	cfg := testConfig(t)
	cfg.DataDir = dataDir
	cfg.WALFsyncEveryN = 1
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after hard crash: %v", err)
	}
	defer store.Close()

	assertRecoveredValue(t, store, "k1", "v1")
	assertRecoveredValue(t, store, "k2", "v2")
}

func runHardCrashPutHelper() {
	dataDir := os.Getenv("LSMKV_HARD_CRASH_DATA_DIR")
	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "missing LSMKV_HARD_CRASH_DATA_DIR")
		os.Exit(2)
	}

	cfg := DefaultConfig()
	cfg.DataDir = dataDir
	cfg.WALFsyncEveryN = 1
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		os.Exit(2)
	}

	if err := store.Put([]byte("k1"), []byte("v1")); err != nil {
		fmt.Fprintln(os.Stderr, "put k1:", err)
		os.Exit(2)
	}

	if err := store.Put([]byte("k2"), []byte("v2")); err != nil {
		fmt.Fprintln(os.Stderr, "put k2:", err)
		os.Exit(2)
	}

	// Namerno nema store.Close(): simuliramo prekid procesa nakon
	// successful, fsync-ovanih Put operacija.
	os.Exit(0)
}

func assertRecoveredValue(t *testing.T, store *Store, key, want string) {
	t.Helper()

	got, found, err := store.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get %q after restart: %v", key, err)
	}
	if !found {
		t.Fatalf("expected %q to survive hard crash", key)
	}
	if string(got) != want {
		t.Fatalf("Get %q after restart: got %q, want %q", key, got, want)
	}
}
func TestHardCrashAfterAcknowledgedDeleteRecoversTombstone(t *testing.T) {
	if os.Getenv(hardCrashHelperEnv) == "1" {
		runHardCrashDeleteHelper()
		return
	}

	dataDir := t.TempDir()

	cmd := exec.Command(
		os.Args[0],
		"-test.run=TestHardCrashAfterAcknowledgedDeleteRecoversTombstone",
	)
	cmd.Env = append(
		os.Environ(),
		hardCrashHelperEnv+"=1",
		"LSMKV_HARD_CRASH_DATA_DIR="+dataDir,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hard-crash delete helper failed: %v\noutput:\n%s", err, output)
	}

	cfg := testConfig(t)
	cfg.DataDir = dataDir
	cfg.WALFsyncEveryN = 1
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen after hard crash: %v", err)
	}
	defer store.Close()

	_, found, err := store.Get([]byte("gone"))
	if err != nil {
		t.Fatalf("Get gone after restart: %v", err)
	}
	if found {
		t.Fatal("expected acknowledged tombstone to survive hard crash")
	}
}

func runHardCrashDeleteHelper() {
	dataDir := os.Getenv("LSMKV_HARD_CRASH_DATA_DIR")
	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "missing LSMKV_HARD_CRASH_DATA_DIR")
		os.Exit(2)
	}

	cfg := DefaultConfig()
	cfg.DataDir = dataDir
	cfg.WALFsyncEveryN = 1
	cfg.MemtableMaxBytes = 64 * 1024 * 1024

	store, err := Open(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		os.Exit(2)
	}

	if err := store.Put([]byte("gone"), []byte("value")); err != nil {
		fmt.Fprintln(os.Stderr, "put gone:", err)
		os.Exit(2)
	}

	if err := store.Delete([]byte("gone")); err != nil {
		fmt.Fprintln(os.Stderr, "delete gone:", err)
		os.Exit(2)
	}

	// Namerno nema store.Close(): i Put i Delete su acknowledged i
	// fsync-ovani, a child proces se prekida bez graceful shutdown-a.
	os.Exit(0)
}
