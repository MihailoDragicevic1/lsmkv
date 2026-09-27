package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"lsmkv/internal/lsm"
)

const defaultConfigPath = "config/default.json"

type commandFlags struct {
	Config string
	Key    string
	Value  string

	KeySet   bool
	ValueSet bool
	Fast     bool
	CrashAt  string
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	args := os.Args[2:]

	switch command {
	case "help", "--help", "-h":
		printUsage()

	case "init":
		runInit(args)

	case "put":
		runPut(args)

	case "crash-put":
		runCrashPut(args)

	case "hard-crash-put":
		runHardCrashPut(args)

	case "hard-crash-flush":
		runHardCrashFlush(args)

	case "hard-crash-batch-put":
		runHardCrashBatchPut(args)

	case "auto-flush-demo":
		runAutoFlushDemo(args)

	case "immutable-read-demo":
		runImmutableReadDemo(args)
	case "bloom-demo":
		runBloomDemo(args)

	case "bloom-correctness-demo":
		runBloomCorrectnessDemo(args)

	case "seed-wal-tail":
		runSeedWALTail(args)

	case "truncate-wal-tail":
		runTruncateWALTail(args)

	case "get":
		runGet(args)

	case "get-source":
		runGetSource(args)

	case "del", "delete":
		runDelete(args)

	case "stats":
		runStats(args)

	case "bg-status":
		runBGStatus(args)

	case "flush":
		runFlush(args)

	case "compact":
		runCompact(args)

	case "close":
		runClose(args)

	case "manifest-info":
		runManifestInfo(args)

	case "list-sst":
		runListSST(args)

	case "demo":
		runDemo(args)

	case "run":
		run(args)
	default:
		fatalf("unknown command %q", command)
	}
}

func runInit(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatalf("create data dir error: %v", err)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fatalf("open store error: %v", err)
	}
	defer closeStore(store)

	fmt.Println("config loaded ✓")
	fmt.Printf("data dir ready ✓ (%s)\n", cfg.DataDir)
	fmt.Printf("wal dir ready ✓ (%s)\n", filepath.Join(cfg.DataDir, "wal"))
	fmt.Println("store opened ✓")
	fmt.Printf(
		"memtable_max_bytes=%d block_size=%d wal_fsync_every_n=%d wal_segment_roll_bytes=%d l0_compaction_trigger=%d\n",
		cfg.MemtableMaxBytes,
		cfg.BlockSize,
		cfg.WALFsyncEveryN,
		cfg.WALSegmentRollBytes,
		cfg.L0CompactionTrigger,
	)
}

func runPut(args []string) {
	flags := mustParseFlags(args, true, true)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.Put([]byte(flags.Key), []byte(flags.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok put key=%q seqno=%d memtable_bytes=%d\n",
		flags.Key,
		stats.LastSeqNo,
		stats.ActiveBytes,
	)
}

func runCrashPut(args []string) {
	flags := mustParseFlags(args, true, true)

	if flags.CrashAt != "afterWALSyncBeforeMemtable" {
		fatalf(
			"crash-put requires --crash-at afterWALSyncBeforeMemtable",
		)
	}

	if err := os.Setenv("LSMKV_DEMO_CRASH_AT", flags.CrashAt); err != nil {
		fatalf("set crash point: %v", err)
	}

	fmt.Printf(
		"Starting controlled crash demo at point=%s\n",
		flags.CrashAt,
	)
	fmt.Println("Expected result: WAL is durable; process stops before memtable update.")

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.Put([]byte(flags.Key), []byte(flags.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	fatalf("ERROR: crash point did not fire")
}

func runHardCrashPut(args []string) {
	flags := mustParseFlags(args, true, true)

	store := mustOpenStore(flags.Config)

	if err := store.Put([]byte(flags.Key), []byte(flags.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	stats := store.Stats()

	fmt.Printf(
		"PUT ACKNOWLEDGED: key=%q value=%q seqno=%d\n",
		flags.Key,
		flags.Value,
		stats.LastSeqNo,
	)
	fmt.Println("WAL record is fsync-protected according to wal_fsync_every_n.")
	fmt.Println("SIMULATING HARD CRASH NOW: Store.Close will NOT run.")

	os.Exit(87)
}

func runHardCrashBatchPut(args []string) {
	flags := mustParseFlags(args, false, false)

	if flags.KeySet || flags.ValueSet {
		fatalf("hard-crash-batch-put does not use --key or --value")
	}

	store := mustOpenStore(flags.Config)

	records := []struct {
		key   string
		value string
	}{
		{key: "k1", value: "v1"},
		{key: "k2", value: "v2"},
		{key: "k3", value: "v3"},
	}

	for _, record := range records {
		if err := store.Put([]byte(record.key), []byte(record.value)); err != nil {
			fatalf("put key=%q error: %v", record.key, err)
		}

		stats := store.Stats()
		fmt.Printf(
			"PUT ACKNOWLEDGED: key=%q value=%q seqno=%d\n",
			record.key,
			record.value,
			stats.LastSeqNo,
		)
	}

	fmt.Println("All PUT operations were acknowledged.")
	fmt.Println("SIMULATING HARD CRASH NOW: Store.Close will NOT run.")

	os.Exit(89)
}

func runSeedWALTail(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)

	records := []struct {
		key   string
		value string
	}{
		{key: "key1", value: "val1"},
		{key: "key2", value: "val2"},
	}

	for _, record := range records {
		if err := store.Put([]byte(record.key), []byte(record.value)); err != nil {
			fatalf("put key=%q error: %v", record.key, err)
		}

		stats := store.Stats()
		fmt.Printf(
			"PUT ACKNOWLEDGED: key=%q value=%q seqno=%d\n",
			record.key,
			record.value,
			stats.LastSeqNo,
		)
	}

	fmt.Println("Seed WAL contains two complete fsync-protected records.")
	fmt.Println("SIMULATING HARD CRASH NOW: Store.Close will NOT run.")

	os.Exit(90)
}

func runTruncateWALTail(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	walDir := filepath.Join(cfg.DataDir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		fatalf("read WAL directory: %v", err)
	}

	var latestName string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}

		if latestName == "" || entry.Name() > latestName {
			latestName = entry.Name()
		}
	}

	if latestName == "" {
		fatalf("no WAL segment found in %s", walDir)
	}

	walPath := filepath.Join(walDir, latestName)
	info, err := os.Stat(walPath)
	if err != nil {
		fatalf("stat WAL segment: %v", err)
	}

	const bytesToRemove int64 = 4
	if info.Size() <= 8+bytesToRemove {
		fatalf(
			"WAL segment %s is too small to truncate safely: %d bytes",
			walPath,
			info.Size(),
		)
	}

	newSize := info.Size() - bytesToRemove
	if err := os.Truncate(walPath, newSize); err != nil {
		fatalf("truncate WAL tail: %v", err)
	}

	fmt.Printf(
		"WAL TAIL TRUNCATED: file=%s old_size=%d new_size=%d removed=%d\n",
		walPath,
		info.Size(),
		newSize,
		bytesToRemove,
	)
}

func runHardCrashFlush(args []string) {
	flags := mustParseFlags(args, true, true)

	if err := os.Setenv(
		"LSMKV_DEMO_CRASH_AT",
		"afterFlushSSTBeforeManifest",
	); err != nil {
		fatalf("set crash point: %v", err)
	}

	store := mustOpenStore(flags.Config)

	if err := store.Put([]byte(flags.Key), []byte(flags.Value)); err != nil {
		fatalf("put error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"PUT READY FOR FLUSH: key=%q value=%q seqno=%d\n",
		flags.Key,
		flags.Value,
		stats.LastSeqNo,
	)
	fmt.Println("Starting flush. The process will crash after SSTable creation and before manifest save.")

	if err := store.ForceFlush(); err != nil {
		fatalf("flush error: %v", err)
	}

	fatalf("ERROR: flush crash point did not fire")
}

func runGet(args []string) {
	flags := mustParseFlags(args, true, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	value, found, err := store.Get([]byte(flags.Key))
	if err != nil {
		fatalf("get error: %v", err)
	}

	if !found {
		fmt.Println("NOT FOUND")
		return
	}

	fmt.Println(string(value))
}

func runAutoFlushDemo(args []string) {
	flags := mustParseFlags(args, false, false)

	_, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	records := []struct {
		key   string
		value string
	}{
		{key: "a", value: "11111111"},
		{key: "b", value: "22222222"},
		{key: "c", value: "33333333"},
	}

	fmt.Println("=== automatic flush demo ===")
	fmt.Println("The third write exceeds memtable_max_bytes and triggers rotation.")
	fmt.Println()

	for _, record := range records {
		if err := store.Put([]byte(record.key), []byte(record.value)); err != nil {
			fatalf("put key=%q error: %v", record.key, err)
		}

		stats := store.Stats()
		bg := store.BGStatus()

		fmt.Printf(
			"PUT OK: key=%q value=%q seqno=%d active_entries=%d active_bytes=%d immutables=%d sstables=%d flush_queue=%d flush_running=%v\n",
			record.key,
			record.value,
			stats.LastSeqNo,
			stats.ActiveEntries,
			stats.ActiveBytes,
			stats.ImmutablesCount,
			stats.SSTCount,
			bg.FlushQueueLen,
			bg.FlushRunning,
		)
	}

	fmt.Println()
	fmt.Println("Waiting until all background flush jobs finish...")

	for {
		stats := store.Stats()
		bg := store.BGStatus()

		if stats.ImmutablesCount == 0 && !bg.FlushRunning && bg.FlushQueueLen == 0 {
			fmt.Printf(
				"BACKGROUND FLUSH COMPLETE: active_entries=%d active_bytes=%d immutables=%d sstables=%d\n",
				stats.ActiveEntries,
				stats.ActiveBytes,
				stats.ImmutablesCount,
				stats.SSTCount,
			)
			return
		}

		fmt.Printf(
			"WAITING: active_entries=%d immutables=%d sstables=%d flush_queue=%d flush_running=%v\n",
			stats.ActiveEntries,
			stats.ImmutablesCount,
			stats.SSTCount,
			bg.FlushQueueLen,
			bg.FlushRunning,
		)

		time.Sleep(20 * time.Millisecond)
	}
}

func runImmutableReadDemo(args []string) {
	flags := mustParseFlags(args, false, false)

	if err := os.Setenv(
		"LSMKV_DEMO_PAUSE_AT",
		"beforeFlushImmutable",
	); err != nil {
		fatalf("set demo pause: %v", err)
	}

	_, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	records := []struct {
		key   string
		value string
	}{
		{key: "a", value: "11111111"},
		{key: "b", value: "22222222"},
		{key: "c", value: "33333333"},
	}

	fmt.Println("=== immutable memtable read-path demo ===")
	fmt.Println("The third PUT exceeds the threshold and rotates the active memtable.")
	fmt.Println("The background worker pauses before writing the immutable memtable to SSTable.")

	for _, record := range records {
		if err := store.Put([]byte(record.key), []byte(record.value)); err != nil {
			fatalf("put key=%q error: %v", record.key, err)
		}
	}

	stats := store.Stats()
	bg := store.BGStatus()

	fmt.Printf(
		"PAUSED STATE: active_entries=%d immutables=%d sstables=%d flush_running=%v\n",
		stats.ActiveEntries,
		stats.ImmutablesCount,
		stats.SSTCount,
		bg.FlushRunning,
	)

	value, found, source, err := store.GetWithSource([]byte("a"))
	if err != nil {
		fatalf("get source from immutable memtable: %v", err)
	}
	if !found {
		fatalf("key %q was not found while flush is paused", "a")
	}

	fmt.Printf(
		"GET WHILE PAUSED: key=%q value=%q source=%s\n",
		"a",
		string(value),
		source,
	)

	fmt.Println("From a second PowerShell terminal, create resume-flush.signal to continue the background flush.")

	for {
		stats = store.Stats()
		bg = store.BGStatus()

		if stats.ImmutablesCount == 0 && !bg.FlushRunning && bg.FlushQueueLen == 0 {
			fmt.Printf(
				"BACKGROUND FLUSH COMPLETE: active_entries=%d immutables=%d sstables=%d\n",
				stats.ActiveEntries,
				stats.ImmutablesCount,
				stats.SSTCount,
			)

			value, found, source, err := store.GetWithSource([]byte("a"))
			if err != nil {
				fatalf("get source from sstable: %v", err)
			}
			if !found {
				fatalf("key %q was not found after background flush", "a")
			}

			fmt.Printf(
				"GET AFTER FLUSH: key=%q value=%q source=%s\n",
				"a",
				string(value),
				source,
			)
			return
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func runBloomDemo(args []string) {
	flags := mustParseFlags(args, false, false)

	_, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	records := []struct {
		key   string
		value string
	}{
		{key: "a", value: "value-a"},
		{key: "z", value: "value-z"},
	}

	fmt.Println("=== bloom filter demo ===")
	fmt.Println("Creating one SSTable with key range [a, z].")

	for _, record := range records {
		if err := store.Put([]byte(record.key), []byte(record.value)); err != nil {
			fatalf("put key=%q error: %v", record.key, err)
		}
	}

	if err := store.ForceFlush(); err != nil {
		fatalf("force flush error: %v", err)
	}

	afterFlush := store.MetricsSnapshot()
	fmt.Printf(
		"AFTER FLUSH: bloom_checks=%d bloom_skips=%d block_reads=%d\n",
		afterFlush.BloomChecksTotal,
		afterFlush.BloomSkipsTotal,
		afterFlush.BlockReadsTotal,
	)

	fmt.Println()
	fmt.Println(`LOOKUP 1: key="m" is inside [a, z] but was never inserted.`)

	beforeMissing := store.MetricsSnapshot()
	_, found, source, err := store.GetWithSource([]byte("m"))
	if err != nil {
		fatalf("get missing key error: %v", err)
	}
	afterMissing := store.MetricsSnapshot()

	fmt.Printf(
		"MISSING RESULT: found=%v source=%s\n",
		found,
		source,
	)
	fmt.Printf(
		"MISSING DELTA: bloom_checks=%+d bloom_skips=%+d block_reads=%+d\n",
		afterMissing.BloomChecksTotal-beforeMissing.BloomChecksTotal,
		afterMissing.BloomSkipsTotal-beforeMissing.BloomSkipsTotal,
		afterMissing.BlockReadsTotal-beforeMissing.BlockReadsTotal,
	)

	fmt.Println()
	fmt.Println(`LOOKUP 2: key="a" exists in the SSTable.`)

	beforeExisting := store.MetricsSnapshot()
	value, found, source, err := store.GetWithSource([]byte("a"))
	if err != nil {
		fatalf("get existing key error: %v", err)
	}
	afterExisting := store.MetricsSnapshot()

	fmt.Printf(
		"EXISTING RESULT: found=%v value=%q source=%s\n",
		found,
		string(value),
		source,
	)
	fmt.Printf(
		"EXISTING DELTA: bloom_checks=%+d bloom_skips=%+d block_reads=%+d\n",
		afterExisting.BloomChecksTotal-beforeExisting.BloomChecksTotal,
		afterExisting.BloomSkipsTotal-beforeExisting.BloomSkipsTotal,
		afterExisting.BlockReadsTotal-beforeExisting.BlockReadsTotal,
	)
}

func runBloomCorrectnessDemo(args []string) {
	flags := mustParseFlags(args, false, false)

	_, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	const keyCount = 100

	fmt.Println("=== bloom filter correctness demo ===")
	fmt.Printf("Writing and verifying %d deterministic keys.\n", keyCount)

	for i := 0; i < keyCount; i++ {
		key := "present-" + strconv.Itoa(i)
		value := "value-" + strconv.Itoa(i)

		if err := store.Put([]byte(key), []byte(value)); err != nil {
			fatalf("put key=%q error: %v", key, err)
		}
	}

	if err := store.ForceFlush(); err != nil {
		fatalf("force flush error: %v", err)
	}

	before := store.MetricsSnapshot()

	for i := 0; i < keyCount; i++ {
		key := "present-" + strconv.Itoa(i)
		want := "value-" + strconv.Itoa(i)

		value, found, source, err := store.GetWithSource([]byte(key))
		if err != nil {
			fatalf("get key=%q error: %v", key, err)
		}
		if !found {
			fatalf("FALSE NEGATIVE: inserted key=%q was not found", key)
		}
		if string(value) != want {
			fatalf(
				"WRONG VALUE: key=%q got=%q want=%q",
				key,
				string(value),
				want,
			)
		}
		if source != "sstable" {
			fatalf(
				"WRONG SOURCE: key=%q source=%s; expected sstable",
				key,
				source,
			)
		}
	}

	after := store.MetricsSnapshot()

	checks := after.BloomChecksTotal - before.BloomChecksTotal
	skips := after.BloomSkipsTotal - before.BloomSkipsTotal
	blockReads := after.BlockReadsTotal - before.BlockReadsTotal

	fmt.Println("ALL INSERTED KEYS FOUND")
	fmt.Printf("verified_keys=%d\n", keyCount)
	fmt.Printf("bloom_checks=%d\n", checks)
	fmt.Printf("bloom_skips=%d\n", skips)
	fmt.Printf("block_reads=%d\n", blockReads)

	if checks != keyCount {
		fatalf("unexpected bloom check count: got=%d want=%d", checks, keyCount)
	}
	if skips != 0 {
		fatalf(
			"FALSE NEGATIVE DETECTED: bloom skipped %d inserted key lookups",
			skips,
		)
	}
	if blockReads < keyCount {
		fatalf(
			"unexpected block reads: got=%d, want at least %d",
			blockReads,
			keyCount,
		)
	}

	fmt.Println("RESULT: no false negatives were observed for inserted keys.")
}

func runGetSource(args []string) {
	flags := mustParseFlags(args, true, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	value, found, source, err := store.GetWithSource([]byte(flags.Key))
	if err != nil {
		fatalf("get-source error: %v", err)
	}

	if !found {
		fmt.Printf("NOT FOUND: key=%q source=%s\n", flags.Key, source)
		return
	}

	fmt.Printf(
		"GET OK: key=%q value=%q source=%s\n",
		flags.Key,
		string(value),
		source,
	)
}

func runDelete(args []string) {
	flags := mustParseFlags(args, true, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.Delete([]byte(flags.Key)); err != nil {
		fatalf("delete error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok del key=%q seqno=%d memtable_bytes=%d\n",
		flags.Key,
		stats.LastSeqNo,
		stats.ActiveBytes,
	)
}

func runStats(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	stats := store.Stats()
	m := stats.Metrics

	fmt.Println("=== store stats ===")
	fmt.Printf("engine_status:      %s\n", stats.EngineStatus)
	fmt.Printf("last_seqno:         %d\n", stats.LastSeqNo)
	fmt.Println("")
	fmt.Println("--- wal ---")
	fmt.Printf("active_segment_id:  %d\n", stats.ActiveSegmentID)
	fmt.Printf("total_wal_segments: %d\n", stats.TotalWALSegments)
	fmt.Printf("bytes_written:      %d\n", stats.BytesWritten)
	fmt.Println("")
	fmt.Println("--- memtable ---")
	fmt.Printf("active_entries:     %d\n", stats.ActiveEntries)
	fmt.Printf("active_bytes:       %d\n", stats.ActiveBytes)
	fmt.Printf("immutables_count:   %d\n", stats.ImmutablesCount)
	fmt.Printf("immutables_bytes:   %d\n", stats.ImmutablesBytes)
	fmt.Println("")
	fmt.Println("--- sstables ---")
	fmt.Printf("sst_count:          %d\n", stats.SSTCount)
	fmt.Printf("sst_total_bytes:    %d\n", stats.SSTTotalBytes)
	fmt.Println("")
	fmt.Println("--- metrics (unit 8) ---")
	fmt.Printf("puts_total:               %d\n", m.PutsTotal)
	fmt.Printf("deletes_total:            %d\n", m.DeletesTotal)
	fmt.Printf("bloom_checks_total:       %d\n", m.BloomChecksTotal)
	fmt.Printf("bloom_skips_total:        %d\n", m.BloomSkipsTotal)
	fmt.Printf("block_reads_total:        %d\n", m.BlockReadsTotal)
	fmt.Printf("flushes_total:            %d\n", m.FlushesTotal)
	fmt.Printf("compactions_total:        %d\n", m.CompactionsTotal)
	fmt.Printf("last_flush_duration_ms:   %d\n", m.LastFlushDurationMs)
	fmt.Printf("last_compact_duration_ms: %d\n", m.LastCompactDurationMs)
}

func runBGStatus(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	bg := store.BGStatus()
	fmt.Println("background status")
	fmt.Printf("flush_running: %v\n", bg.FlushRunning)
	fmt.Printf("compact_running: %v\n", bg.CompactRunning)
	fmt.Printf("flush_queue_len: %d\n", bg.FlushQueueLen)
	fmt.Printf("compact_queue_len: %d\n", bg.CompactQueueLen)
	fmt.Printf("flush_jobs_total: %d\n", bg.FlushJobsTotal)
	fmt.Printf("compact_jobs_total: %d\n", bg.CompactJobsTotal)
	fmt.Printf("last_flush_ms: %d\n", bg.LastFlushMs)
	fmt.Printf("last_compact_ms: %d\n", bg.LastCompactMs)
	fmt.Printf("compaction_trigger: %d\n", bg.CompactionTrigger)
	if bg.LastError != "" {
		fmt.Printf("last_error: %s\n", bg.LastError)
	} else {
		fmt.Println("last_error: none")
	}
}

func runFlush(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	if err := store.ForceFlush(); err != nil {
		fatalf("flush error: %v", err)
	}

	stats := store.Stats()
	fmt.Printf(
		"ok flush immutables_remaining=%d sst_count=%d last_seqno=%d\n",
		stats.ImmutablesCount,
		stats.SSTCount,
		stats.LastSeqNo,
	)
}

func runCompact(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)
	defer closeStore(store)

	before := store.Stats()

	if err := store.Compact(); err != nil {
		fatalf("compact error: %v", err)
	}

	after := store.Stats()
	fmt.Printf(
		"ok compact sst_count_before=%d sst_count_after=%d sst_total_bytes=%d\n",
		before.SSTCount,
		after.SSTCount,
		after.SSTTotalBytes,
	)
}

func runClose(args []string) {
	flags := mustParseFlags(args, false, false)

	store := mustOpenStore(flags.Config)

	var err error
	if flags.Fast {
		err = store.CloseFast()
	} else {
		err = store.CloseGraceful()
	}
	if err != nil {
		fatalf("close error: %v", err)
	}

	if flags.Fast {
		fmt.Println("closed fast ✓")
	} else {
		fmt.Println("closed ✓")
	}
}

// runManifestInfo čita manifest JSON direktno — bez otvaranja Store-a.
// Ispisuje listu svih SST-ova sa metapodacima.
func runManifestInfo(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	manifestPath := filepath.Join(cfg.DataDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("=== manifest info ===")
			fmt.Println("manifest file: not created yet")
			fmt.Println("sst count: 0")
			fmt.Println()
			fmt.Println("(no published sstables)")
			return
		}

		fatalf("read manifest error: %v", err)
	}

	var raw struct {
		Epoch  uint64 `json:"epoch"`
		Tables []struct {
			ID       uint64 `json:"id"`
			File     string `json:"file"`
			MinKey   string `json:"min_key"`
			MaxKey   string `json:"max_key"`
			MinSeqNo uint64 `json:"min_seq_no"`
			MaxSeqNo uint64 `json:"max_seq_no"`
			FileSize int64  `json:"file_size"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		fatalf("parse manifest error: %v", err)
	}

	fmt.Printf("=== manifest info (epoch %d) ===\n", raw.Epoch)
	fmt.Printf("sst count: %d\n\n", len(raw.Tables))

	if len(raw.Tables) == 0 {
		fmt.Println("(no sstables)")
		return
	}

	fmt.Printf("%-8s %-16s %-20s %-20s %-10s %-10s %-12s\n",
		"id", "file", "min_key", "max_key", "min_seq", "max_seq", "size_bytes")
	fmt.Println(strings.Repeat("-", 100))

	for _, t := range raw.Tables {
		minKey := t.MinKey
		if len(minKey) > 18 {
			minKey = minKey[:15] + "..."
		}
		maxKey := t.MaxKey
		if len(maxKey) > 18 {
			maxKey = maxKey[:15] + "..."
		}
		fmt.Printf("%-8d %-16s %-20s %-20s %-10d %-10d %-12d\n",
			t.ID, t.File, minKey, maxKey, t.MinSeqNo, t.MaxSeqNo, t.FileSize)
	}
}

// runListSST otvara svaki SST fajl i ispisuje detalje:
// broj entries, seq range, bloom bits, veličinu.
func runListSST(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, err := lsm.LoadConfig(flags.Config)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	manifestPath := filepath.Join(cfg.DataDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		fatalf("read manifest error: %v", err)
	}

	var raw struct {
		Epoch  uint64 `json:"epoch"`
		Tables []struct {
			ID       uint64 `json:"id"`
			File     string `json:"file"`
			MinSeqNo uint64 `json:"min_seq_no"`
			MaxSeqNo uint64 `json:"max_seq_no"`
			FileSize int64  `json:"file_size"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		fatalf("parse manifest error: %v", err)
	}

	fmt.Printf("=== list-sst (epoch=%d, count=%d) ===\n\n", raw.Epoch, len(raw.Tables))

	if len(raw.Tables) == 0 {
		fmt.Println("(no sstables)")
		return
	}

	fmt.Printf("%-8s %-16s %-10s %-10s %-10s %-12s\n",
		"id", "file", "entries", "min_seq", "max_seq", "size_bytes")
	fmt.Println(strings.Repeat("-", 76))

	for _, t := range raw.Tables {
		sstPath := filepath.Join(cfg.DataDir, t.File)
		entryCount := countSSTEntries(sstPath)
		fmt.Printf("%-8d %-16s %-10d %-10d %-10d %-12d\n",
			t.ID, t.File, entryCount, t.MinSeqNo, t.MaxSeqNo, t.FileSize)
	}
}

// countSSTEntries otvara SST reader i broji sve entries.
// Vraća -1 ako fajl nije čitljiv.
func countSSTEntries(path string) int {
	r, err := lsm.OpenSSTableReader(path)
	if err != nil {
		return -1
	}
	defer r.Close()

	entries, err := r.AllEntries()
	if err != nil {
		return -1
	}
	return len(entries)
}

func runDemo(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	fmt.Println("=== LSMKV interactive lifecycle demo ===")
	fmt.Printf("data directory: %s\n", cfg.DataDir)
	fmt.Println("This process keeps one Store instance open.")
	fmt.Println("Commands: put <key> <value> | get <key> | state | flush | manifest | files | help | exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("lsmkv-demo> ")

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				fatalf("read demo command: %v", err)
			}
			fmt.Println()
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		command := parts[0]

		switch command {
		case "help":
			printDemoUsage()

		case "put":
			if len(parts) != 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}

			if err := store.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Printf("PUT ERROR: %v\n", err)
				continue
			}

			stats := store.Stats()
			fmt.Printf(
				"PUT OK: key=%q value=%q seqno=%d\n",
				parts[1],
				parts[2],
				stats.LastSeqNo,
			)
			fmt.Println("Location after PUT: WAL + active memtable")

		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}

			value, found, err := store.Get([]byte(parts[1]))
			if err != nil {
				fmt.Printf("GET ERROR: %v\n", err)
				continue
			}

			if !found {
				fmt.Println("NOT FOUND")
				continue
			}

			fmt.Printf("GET OK: key=%q value=%q\n", parts[1], string(value))

		case "state":
			printDemoState(store)

		case "flush":
			fmt.Println("FLUSH START: active memtable will rotate to immutable memtable.")
			fmt.Println("FLUSH STEP: immutable memtable is written to an SSTable.")
			fmt.Println("FLUSH STEP: manifest is updated before the SSTable becomes visible.")

			if err := store.ForceFlush(); err != nil {
				fmt.Printf("FLUSH ERROR: %v\n", err)
				continue
			}

			stats := store.Stats()
			fmt.Printf(
				"FLUSH OK: active_entries=%d immutables=%d sstables=%d\n",
				stats.ActiveEntries,
				stats.ImmutablesCount,
				stats.SSTCount,
			)
			fmt.Println("Location after successful flush: SSTable")

		case "manifest":
			printDemoManifest(cfg)

		case "files":
			printDemoFiles(cfg)

		case "exit", "quit":
			fmt.Println("Demo session finished. Closing store gracefully.")
			return

		default:
			fmt.Printf("unknown demo command %q; type help\n", command)
		}
	}
}

func printDemoUsage() {
	fmt.Println("Available demo commands:")
	fmt.Println("  put <key> <value>  - write one key/value pair")
	fmt.Println("  get <key>          - read one key")
	fmt.Println("  state              - show WAL, memtable, immutable, and SSTable counters")
	fmt.Println("  flush              - synchronously flush the active memtable to an SSTable")
	fmt.Println("  manifest           - show published SSTable metadata from manifest.json")
	fmt.Println("  files              - show physical files in the data and WAL directories")
	fmt.Println("  exit               - gracefully close the Store and end the demo")
}

func printDemoState(store *lsm.Store) {
	stats := store.Stats()

	fmt.Println("=== lifecycle state ===")
	fmt.Printf("last sequence number: %d\n", stats.LastSeqNo)
	fmt.Println("")
	fmt.Println("WAL")
	fmt.Printf("  active segment id:  %d\n", stats.ActiveSegmentID)
	fmt.Printf("  total segments:     %d\n", stats.TotalWALSegments)
	fmt.Printf("  data bytes written: %d\n", stats.BytesWritten)
	fmt.Println("")
	fmt.Println("MEMTABLE")
	fmt.Printf("  active entries:     %d\n", stats.ActiveEntries)
	fmt.Printf("  active bytes:       %d\n", stats.ActiveBytes)
	fmt.Printf("  immutable count:    %d\n", stats.ImmutablesCount)
	fmt.Printf("  immutable bytes:    %d\n", stats.ImmutablesBytes)
	fmt.Println("")
	fmt.Println("SSTABLES")
	fmt.Printf("  published count:    %d\n", stats.SSTCount)
	fmt.Printf("  total bytes:        %d\n", stats.SSTTotalBytes)
	fmt.Println("=======================")
}

func printDemoManifest(cfg lsm.Config) {
	manifestPath := filepath.Join(cfg.DataDir, "manifest.json")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		fmt.Printf("MANIFEST ERROR: %v\n", err)
		return
	}

	var raw struct {
		Epoch  uint64 `json:"epoch"`
		Tables []struct {
			ID       uint64 `json:"id"`
			File     string `json:"file"`
			MinKey   string `json:"min_key"`
			MaxKey   string `json:"max_key"`
			MinSeqNo uint64 `json:"min_seq_no"`
			MaxSeqNo uint64 `json:"max_seq_no"`
			FileSize int64  `json:"file_size"`
		} `json:"tables"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		fmt.Printf("MANIFEST ERROR: %v\n", err)
		return
	}

	fmt.Printf("=== manifest (epoch=%d) ===\n", raw.Epoch)

	if len(raw.Tables) == 0 {
		fmt.Println("No published SSTables.")
		return
	}

	for _, table := range raw.Tables {
		fmt.Printf(
			"SSTable: id=%d file=%s key_range=[%q, %q] seq_range=[%d, %d] size=%d bytes\n",
			table.ID,
			table.File,
			table.MinKey,
			table.MaxKey,
			table.MinSeqNo,
			table.MaxSeqNo,
			table.FileSize,
		)
	}
}

func printDemoFiles(cfg lsm.Config) {
	fmt.Println("=== physical data files ===")
	printDirectoryFiles(cfg.DataDir, false)

	fmt.Println("=== physical WAL files ===")
	printDirectoryFiles(filepath.Join(cfg.DataDir, "wal"), false)
}

func printDirectoryFiles(dir string, recursive bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("Cannot read %s: %v\n", dir, err)
		return
	}

	if len(entries) == 0 {
		fmt.Println("(empty)")
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			fmt.Printf("[dir]  %s\n", entry.Name())
			continue
		}

		info, err := entry.Info()
		if err != nil {
			fmt.Printf("[file] %s (metadata error: %v)\n", entry.Name(), err)
			continue
		}

		fmt.Printf("[file] %s (%d bytes)\n", entry.Name(), info.Size())
	}
}

func run(args []string) {
	flags := mustParseFlags(args, false, false)

	cfg, store := loadAndOpenStore(flags.Config)
	defer closeStore(store)

	fmt.Println("lsmkv skeleton is alive")
	fmt.Printf("data dir: %s\n", cfg.DataDir)
	fmt.Printf("memtable max bytes: %d\n", cfg.MemtableMaxBytes)
	fmt.Printf("block size: %d\n", cfg.BlockSize)
}

func mustParseFlags(args []string, requireKey, requireValue bool) commandFlags {
	flags := commandFlags{
		Config: defaultConfigPath,
	}

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch {
		case arg == "--config":
			flags.Config = nextFlagValue(args, &i, "--config")

		case strings.HasPrefix(arg, "--config="):
			flags.Config = strings.TrimPrefix(arg, "--config=")
			if flags.Config == "" {
				fatalf("--config cannot be empty")
			}

		case arg == "--key":
			flags.Key = nextFlagValue(args, &i, "--key")
			flags.KeySet = true

		case strings.HasPrefix(arg, "--key="):
			flags.Key = strings.TrimPrefix(arg, "--key=")
			flags.KeySet = true

		case arg == "--value":
			flags.Value = nextFlagValue(args, &i, "--value")
			flags.ValueSet = true

		case strings.HasPrefix(arg, "--value="):
			flags.Value = strings.TrimPrefix(arg, "--value=")
			flags.ValueSet = true

		case arg == "--crash-at":
			flags.CrashAt = nextFlagValue(args, &i, "--crash-at")

		case strings.HasPrefix(arg, "--crash-at="):
			flags.CrashAt = strings.TrimPrefix(arg, "--crash-at=")
			if flags.CrashAt == "" {
				fatalf("--crash-at cannot be empty")
			}

		case arg == "--fast" || arg == "--fast=true":
			flags.Fast = true
		case arg == "--fast=false":
			flags.Fast = false

		case arg == "--help" || arg == "-h":
			printUsage()
			os.Exit(0)

		case strings.HasPrefix(arg, "--"):
			fatalf("unknown flag %q", arg)

		default:
			fatalf("unexpected positional argument %q", arg)
		}
	}

	if requireKey && !flags.KeySet {
		fatalf("command requires --key")
	}

	if requireKey && flags.Key == "" {
		fatalf("--key cannot be empty")
	}

	if requireValue && !flags.ValueSet {
		fatalf("put requires --value")
	}

	return flags
}

func nextFlagValue(args []string, index *int, name string) string {
	nextIndex := *index + 1

	if nextIndex >= len(args) {
		fatalf("%s requires a value", name)
	}

	value := args[nextIndex]
	if strings.HasPrefix(value, "--") {
		fatalf("%s requires a value", name)
	}

	*index = nextIndex
	return value
}

func mustOpenStore(configPath string) *lsm.Store {
	_, store := loadAndOpenStore(configPath)
	return store
}

func loadAndOpenStore(configPath string) (lsm.Config, *lsm.Store) {
	cfg, err := lsm.LoadConfig(configPath)
	if err != nil {
		fatalf("load config error: %v", err)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fatalf("create data dir error: %v", err)
	}

	store, err := lsm.Open(cfg)
	if err != nil {
		fatalf("open store error: %v", err)
	}

	return cfg, store
}

func closeStore(store *lsm.Store) {
	if store == nil {
		return
	}

	if err := store.Close(); err != nil && err != lsm.ErrStoreClosed {
		fmt.Fprintf(os.Stderr, "close store error: %v\n", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func printUsage() {
	exe := filepath.Base(os.Args[0])

	fmt.Println("usage:")
	fmt.Printf("  %s init [--config path]\n", exe)
	fmt.Printf("  %s put --key K --value V [--config path]\n", exe)
	fmt.Printf("  %s crash-put --key K --value V --crash-at afterWALSyncBeforeMemtable [--config path]\n", exe)
	fmt.Printf("  %s hard-crash-put --key K --value V [--config path]\n", exe)
	fmt.Printf("  %s hard-crash-batch-put [--config path]\n", exe)
	fmt.Printf("  %s hard-crash-flush --key K --value V [--config path]\n", exe)
	fmt.Printf("  %s auto-flush-demo [--config path]\n", exe)
	fmt.Printf("  %s immutable-read-demo [--config path]\n", exe)
	fmt.Printf("  %s bloom-demo [--config path]\n", exe)
	fmt.Printf("  %s bloom-correctness-demo [--config path]\n", exe)
	fmt.Printf("  %s seed-wal-tail [--config path]\n", exe)
	fmt.Printf("  %s truncate-wal-tail [--config path]\n", exe)
	fmt.Printf("  %s get --key K [--config path]\n", exe)
	fmt.Printf("  %s get-source --key K [--config path]\n", exe)
	fmt.Printf("  %s del --key K [--config path]\n", exe)
	fmt.Printf("  %s stats [--config path]\n", exe)
	fmt.Printf("  %s bg-status [--config path]\n", exe)
	fmt.Printf("  %s flush [--config path]\n", exe)
	fmt.Printf("  %s compact [--config path]\n", exe)
	fmt.Printf("  %s close [--fast] [--config path]\n", exe)
	fmt.Printf("  %s manifest-info [--config path]\n", exe)
	fmt.Printf("  %s list-sst [--config path]\n", exe)
	fmt.Printf("  %s demo [--config path]\n", exe)
	fmt.Printf("  %s run [--config path] (legacy smoke test)\n", exe)
	fmt.Printf("  %s help\n", exe)
}
