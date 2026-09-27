package lsm

import (
	"sync"
	"sync/atomic"
	"time"
)

// jobKind razlikuje poslove koje scheduler može da izvršava u pozadini.
type jobKind int

const (
	jobFlush jobKind = iota
	jobCompact
)

// job je signal worker-u da izvrši odgovarajući maintenance posao.
type job struct {
	kind jobKind
}

// BGStatus je snapshot stanja background worker-a za CLI i Stats prikaz.
type BGStatus struct {
	FlushRunning      bool
	CompactRunning    bool
	FlushQueueLen     int
	CompactQueueLen   int
	FlushJobsTotal    uint64
	CompactJobsTotal  uint64
	LastFlushMs       int64
	LastCompactMs     int64
	LastError         string
	CompactionTrigger int
}

// scheduler pokreće po jednog worker-a za flush i compaction.
// Odvojeni queue-ovi omogućavaju da flush ima prioritet nad compaction-om.
type scheduler struct {
	store *Store

	flushQueue   chan job
	compactQueue chan job

	wg     sync.WaitGroup
	stopCh chan struct{}
	once   sync.Once

	// Atomici omogućavaju čitanje statusa bez blokiranja worker-a.
	flushRunning   atomic.Bool
	compactRunning atomic.Bool
	flushJobs      atomic.Uint64
	compactJobs    atomic.Uint64
	lastFlushMs    atomic.Int64
	lastCompactMs  atomic.Int64
}

// newScheduler pravi queue-ove i odmah pokreće flush i compaction worker-e.
func newScheduler(store *Store, queueDepth int) *scheduler {
	if queueDepth <= 0 {
		queueDepth = 4
	}

	s := &scheduler{
		store:        store,
		flushQueue:   make(chan job, queueDepth),
		compactQueue: make(chan job, queueDepth),
		stopCh:       make(chan struct{}),
	}

	s.wg.Add(2)
	go s.runFlushWorker()
	go s.runCompactWorker()
	return s
}

// runFlushWorker obrađuje zakazane flush poslove dok scheduler ne dobije stop signal.
func (s *scheduler) runFlushWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.flushQueue:
			s.flushRunning.Store(true)
			start := time.Now()

			// Demo-only pause: the memtable is already immutable and the flush job
			// has been accepted, but disk flush has not started yet.
			waitDemoPauseBeforeFlush(s.store.cfg.DataDir)

			err := s.store.flushPendingImmutables()
			s.lastFlushMs.Store(time.Since(start).Milliseconds())
			s.flushJobs.Add(1)
			s.flushRunning.Store(false)

			if err != nil {
				s.store.recordBackgroundError("flush", err)
				continue
			}

			// Compaction proveravamo tek nakon uspešnog flush-a novih SSTable-ova.
			s.maybeEnqueueCompact()

		case <-s.stopCh:
			return
		}
	}
}

// runCompactWorker obrađuje compaction poslove, ali prvo prepušta prioritet flush-u.
func (s *scheduler) runCompactWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.compactQueue:
			// Immutable memtable sadrži novije podatke koji još nisu na disku.
			// Dok postoji, prvo ga flush-ujemo pa compaction pokušavamo kasnije.
			s.store.mu.RLock()
			hasImmutables := s.store.immutableCountLocked() > 0
			s.store.mu.RUnlock()

			if hasImmutables {
				s.enqueueFlush()
				s.enqueueCompact()
				continue
			}

			s.compactRunning.Store(true)
			start := time.Now()
			err := s.store.Compact()
			s.lastCompactMs.Store(time.Since(start).Milliseconds())
			s.compactJobs.Add(1)
			s.compactRunning.Store(false)

			if err != nil {
				s.store.recordBackgroundError("compact", err)
			}

		case <-s.stopCh:
			return
		}
	}
}

// enqueueFlush pokušava da zakaže flush bez blokiranja write path-a.
// Ako je queue pun, naredna rotacija ili eksplicitni ForceFlush će ponovo pokušati.
func (s *scheduler) enqueueFlush() {
	select {
	case s.flushQueue <- job{kind: jobFlush}:
	default:
	}
}

// enqueueCompact pokušava da zakaže compaction bez blokiranja worker-a ili read/write path-a.
func (s *scheduler) enqueueCompact() {
	select {
	case s.compactQueue <- job{kind: jobCompact}:
	default:
	}
}

// maybeEnqueueCompact zakazuje compaction kada broj live SSTable-a dostigne trigger.
// Auto-compaction je isključena ako je trigger <= 0 ili ako flush još nije završen.
func (s *scheduler) maybeEnqueueCompact() {
	if s.store == nil {
		return
	}

	trigger := s.store.cfg.L0CompactionTrigger
	if trigger <= 0 {
		return
	}

	s.store.mu.RLock()
	immutables := s.store.immutableCountLocked()
	liveSST := s.store.liveSSTCountLocked()
	s.store.mu.RUnlock()

	if immutables > 0 {
		return
	}
	if liveSST >= trigger {
		s.enqueueCompact()
	}
}

// status vraća trenutne atomic brojače i stanje queue-ova.
func (s *scheduler) status() BGStatus {
	lastErr := ""
	trigger := 0
	if s.store != nil {
		trigger = s.store.cfg.L0CompactionTrigger
		s.store.mu.RLock()
		if s.store.lastBGError != nil {
			lastErr = s.store.lastBGError.Error()
		}
		s.store.mu.RUnlock()
	}

	return BGStatus{
		FlushRunning:      s.flushRunning.Load(),
		CompactRunning:    s.compactRunning.Load(),
		FlushQueueLen:     len(s.flushQueue),
		CompactQueueLen:   len(s.compactQueue),
		FlushJobsTotal:    s.flushJobs.Load(),
		CompactJobsTotal:  s.compactJobs.Load(),
		LastFlushMs:       s.lastFlushMs.Load(),
		LastCompactMs:     s.lastCompactMs.Load(),
		LastError:         lastErr,
		CompactionTrigger: trigger,
	}
}

// stopGraceful šalje signal worker-ima i čeka da se obe goroutine završe.
func (s *scheduler) stopGraceful() {
	s.once.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
}

// stopFast šalje stop signal, ali čeka najviše 200ms da se worker-i završe.
func (s *scheduler) stopFast() {
	s.once.Do(func() {
		close(s.stopCh)
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
	}
}
