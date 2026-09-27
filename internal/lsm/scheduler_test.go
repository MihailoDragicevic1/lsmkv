package lsm

import (
	"testing"
	"time"
)

func TestBGStatusSmoke(t *testing.T) {
	cfg := testConfig(t)
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	bg := s.BGStatus()
	if bg.CompactionTrigger != cfg.L0CompactionTrigger {
		t.Fatalf("trigger=%d want %d", bg.CompactionTrigger, cfg.L0CompactionTrigger)
	}
	if bg.FlushRunning {
		t.Fatalf("expected flush_running=false on idle store")
	}
	if bg.CompactRunning {
		t.Fatalf("expected compact_running=false on idle store")
	}
}

func TestCloseFastLeavesWALDurable(t *testing.T) {
	cfg := testConfig(t)
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Put([]byte("fast"), []byte("ok")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.CloseFast(); err != nil {
		t.Fatalf("CloseFast: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, found, err := s2.Get([]byte("fast"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(got) != "ok" {
		t.Fatalf("want fast=ok after CloseFast recovery, got %q found=%v", got, found)
	}
}

func TestCloseGracefulDrainsImmutables(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1
	cfg.MaxImmutableTables = 4
	cfg.L0CompactionTrigger = 0 // no auto-compact noise

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Put([]byte("g"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Force an immutable without waiting on the bg flush worker.
	s.mu.Lock()
	if err := s.rotateActiveIfNonEmptyLocked(); err != nil {
		s.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	s.mu.Unlock()

	if err := s.CloseGraceful(); err != nil {
		t.Fatalf("CloseGraceful: %v", err)
	}

	s2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	if s2.Stats().SSTCount < 1 {
		t.Fatalf("expected >=1 sst after graceful drain, got %d", s2.Stats().SSTCount)
	}
	got, found, err := s2.Get([]byte("g"))
	if err != nil || !found || string(got) != "1" {
		t.Fatalf("want g=1, got %q found=%v err=%v", got, found, err)
	}
}

func TestCloseAfterCloseReturnsStoreClosed(t *testing.T) {
	cfg := testConfig(t)
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != ErrStoreClosed {
		t.Fatalf("second Close: got %v want ErrStoreClosed", err)
	}
	if err := s.CloseFast(); err != ErrStoreClosed {
		t.Fatalf("CloseFast after close: got %v want ErrStoreClosed", err)
	}
}

func TestLiveSSTCountMatchesStats(t *testing.T) {
	cfg := testConfig(t)
	cfg.L0CompactionTrigger = 0

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	if s.liveSSTCount() != s.Stats().SSTCount {
		t.Fatalf("liveSSTCount=%d stats.SSTCount=%d", s.liveSSTCount(), s.Stats().SSTCount)
	}
	if s.liveSSTCount() != 1 {
		t.Fatalf("expected 1 sst, got %d", s.liveSSTCount())
	}
}

// Ensures ForceFlush path still does not race with auto-compact policy
// when trigger is high: two tables stay until explicit Compact.
// ForceFlush is synchronous and must NOT call maybeEnqueueCompact.
// Puts must not rotate into the bg flush path either, or this test would
// observe legitimate auto-compact from the flush worker.
func TestForceFlushDoesNotAutoCompact(t *testing.T) {
	cfg := testConfig(t)
	// Large memtable so Put does not rotate / enqueue background flush.
	cfg.MemtableMaxBytes = 64 << 20
	cfg.MaxImmutableTables = 8
	cfg.L0CompactionTrigger = 2

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatalf("flush1: %v", err)
	}
	if err := s.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := s.ForceFlush(); err != nil {
		t.Fatalf("flush2: %v", err)
	}

	// Brief wait: if ForceFlush wrongly enqueued compact, count would drop.
	time.Sleep(50 * time.Millisecond)

	if got := s.Stats().SSTCount; got != 2 {
		t.Fatalf("expected 2 sstables after two ForceFlush (no auto-compact), got %d", got)
	}
}

func TestBackgroundFlushMayAutoCompactAtTrigger(t *testing.T) {
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1
	cfg.MaxImmutableTables = 8
	cfg.L0CompactionTrigger = 2

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Two tiny Puts each rotate + enqueue bg flush; after 2 SSTs, compact may run.
	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Eventually: flushes done and ideally compacted to 1 (trigger=2).
		st := s.Stats()
		if st.ImmutablesCount == 0 && st.SSTCount <= 1 && st.SSTCount >= 1 {
			// Readable either way if compact already ran or still 1 table mid-path.
			break
		}
		if st.ImmutablesCount == 0 && st.SSTCount == 2 {
			// flushes done, wait a bit more for compact worker
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Keys must remain readable regardless of whether compact finished.
	for _, key := range []string{"a", "b"} {
		got, found, err := s.Get([]byte(key))
		if err != nil || !found {
			t.Fatalf("Get %s: found=%v err=%v", key, found, err)
		}
		_ = got
	}

	// Soft check: with trigger=2, prefer final count 1 after workers settle.
	st := s.Stats()
	if st.ImmutablesCount != 0 {
		t.Fatalf("expected immutables drained, got %d", st.ImmutablesCount)
	}
	if st.SSTCount != 1 && st.SSTCount != 2 {
		t.Fatalf("unexpected SSTCount=%d", st.SSTCount)
	}
}
func TestBackgroundFlushEventuallyDrainsImmutables(t *testing.T) { //dodato: Šta tačno proverava:Da više malih write-ova naprave rotacije. Da background flush worker zaista odradi posao. Da ImmutablesCount na kraju padne na 0. Da je barem jedan SST objavljen. Da su svi upisani ključevi i dalje čitljivi.
	cfg := testConfig(t)
	cfg.MemtableMaxBytes = 1
	cfg.MaxImmutableTables = 8
	cfg.L0CompactionTrigger = 0

	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	for i := 0; i < 3; i++ {
		key := []byte{byte('a' + i)}
		val := []byte{byte('1' + i)}
		if err := s.Put(key, val); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := s.Stats()
		if st.ImmutablesCount == 0 && st.SSTCount >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	st := s.Stats()
	if st.ImmutablesCount != 0 {
		t.Fatalf("expected background flush to drain immutables, got %d", st.ImmutablesCount)
	}
	if st.SSTCount < 1 {
		t.Fatalf("expected at least one sstable after background flush, got %d", st.SSTCount)
	}

	for i := 0; i < 3; i++ {
		key := []byte{byte('a' + i)}
		want := string([]byte{byte('1' + i)})

		got, found, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get %q: %v", key, err)
		}
		if !found {
			t.Fatalf("expected key %q to be found after background flush", key)
		}
		if string(got) != want {
			t.Fatalf("key %q: got %q, want %q", key, got, want)
		}
	}
}
