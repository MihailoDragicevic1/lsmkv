package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store objedinjuje aktivni memtable, WAL, SSTable fajlove i pozadinske poslove
// za jednu lokalnu LSM bazu.
type Store struct {
	cfg    Config
	closed bool
	stats  Stats

	// mu štiti stanje koje dele foreground operacije i background worker-i.
	mu sync.RWMutex

	// WAL čuva potvrđene upise dok ne postanu deo SSTable-a.
	wal *WAL
	// mem prima nove upise i prvi se proverava na čitanju.
	mem *Memtable
	// immutables sadrži rotirane memtable-ove koji čekaju flush na disk.
	immutables []*Memtable
	// seqNo određuje redosled verzija istog ključa.
	seqNo    uint64
	version  *Version
	manifest *Manifest

	// bg upravlja flush/compaction worker-ima. Kreira se u Open, gasi u Close.
	bg *scheduler

	// lastBGError čuva poslednju grešku iz background worker-a.
	lastBGError error

	// metrics i log služe za praćenje rada engine-a.
	metrics Metrics
	log     *slog.Logger
}

// Open vraća store u poslednje konzistentno stanje: učitava manifest i SSTable-ove,
// a zatim replay-uje WAL zapise koji još nisu završili na disku.
func Open(cfg Config) (*Store, error) {
	manifest, err := loadManifest(cfg)
	if err != nil {
		return nil, err
	}

	// Otvaramo sve tabele iz manifesta pre nego što objavimo početnu verziju.
	readers := make([]*SSTableReader, 0, len(manifest.Tables))
	for _, t := range manifest.Tables {
		r, err := OpenSSTableReader(filepath.Join(cfg.DataDir, t.File))
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return nil, fmt.Errorf("open manifest sstable %s: %w", t.File, err)
		}
		readers = append(readers, r)
	}

	version := newVersionFromManifest(manifest, readers)

	wal, err := OpenWAL(cfg)
	if err != nil {
		_ = version.Close()
		return nil, err
	}

	// WAL sadrži novije upise koji nisu nužno stigli do SSTable-a.
	records, err := ReplayWAL(cfg)
	if err != nil {
		_ = wal.Close()
		_ = version.Close()
		return nil, err
	}

	mem := newMemtable()
	var maxSeq uint64

	for _, rec := range records {
		switch rec.Op {
		case WALOpPut:
			mem.Put(rec.Key, rec.Value, rec.SeqNo)
		case WALOpDel:
			mem.Delete(rec.Key, rec.SeqNo)
		}
		if rec.SeqNo > maxSeq {
			maxSeq = rec.SeqNo
		}
	}

	// SeqNo ne sme da ide unazad ni kada je WAL već očišćen nakon flush-a.
	for _, t := range manifest.Tables {
		if t.MaxSeqNo > maxSeq {
			maxSeq = t.MaxSeqNo
		}
	}

	store := &Store{
		cfg:        cfg,
		wal:        wal,
		mem:        mem,
		immutables: nil,
		seqNo:      maxSeq,
		version:    version,
		manifest:   manifest,
		log:        slog.Default(),
		stats: Stats{
			EngineStatus: "open",
		},
	}

	store.bg = newScheduler(store, cfg.MaxImmutableTables)
	return store, nil
}

// Put prvo trajno upisuje zapis u WAL, pa tek onda ažurira aktivni memtable.
// Tako uspešan upis može da se oporavi i ako proces padne pre flush-a.
func (s *Store) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if err := s.checkWriteStallLocked(); err != nil {
		return err
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpPut, SeqNo: s.seqNo, Key: key, Value: value}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	// Demo-only crash point: WAL record has been appended and fsync-ed according
	// to the configured WALFsyncEveryN policy, but the volatile memtable has not
	// been updated yet.
	runDemoCrashPoint(demoCrashPointAfterWALSyncBeforeMemtable)

	s.mem.Put(key, value, s.seqNo)
	s.metrics.PutsTotal.Add(1)
	return s.rotateIfNeededLocked()
}

// Get traži noviju verziju pre starije: aktivni memtable, immutable memtable-ovi,
// pa SSTable-ovi. Tombstone prekida pretragu jer briše sve starije vrednosti.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, ErrStoreClosed
	}

	if value, tombstone, found := s.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return value, true, nil
	}

	// Noviji immutable memtable-ovi su na kraju niza, zato idemo unazad.
	for i := len(s.immutables) - 1; i >= 0; i-- {
		if value, tombstone, found := s.immutables[i].Get(key); found {
			if tombstone {
				return nil, false, nil
			}
			return value, true, nil
		}
	}

	entry, err := s.version.Get(key, &s.metrics)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}

	if entry.tombstone {
		return nil, false, nil
	}
	return entry.value, true, nil
}

// GetWithSource returns the same logical result as Get, plus the storage layer
// that supplied the result. It is intended for read-only diagnostics and demos.
func (s *Store) GetWithSource(key []byte) ([]byte, bool, string, error) {
	if len(key) == 0 {
		return nil, false, "invalid_argument", ErrInvalidArgument
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false, "closed", ErrStoreClosed
	}

	if value, tombstone, found := s.mem.Get(key); found {
		if tombstone {
			return nil, false, "active_memtable_tombstone", nil
		}
		return value, true, "active_memtable", nil
	}

	// Newer immutable memtables are searched first.
	for i := len(s.immutables) - 1; i >= 0; i-- {
		if value, tombstone, found := s.immutables[i].Get(key); found {
			if tombstone {
				return nil, false, "immutable_memtable_tombstone", nil
			}
			return value, true, "immutable_memtable", nil
		}
	}

	entry, err := s.version.Get(key, &s.metrics)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, false, "not_found", nil
		}
		return nil, false, "sstable_error", err
	}

	if entry.tombstone {
		return nil, false, "sstable_tombstone", nil
	}

	return entry.value, true, "sstable", nil
}

// Delete upisuje tombstone umesto da odmah ukloni stare vrednosti sa diska.
// Stare vrednosti i tombstone kasnije obrađuje compaction.
func (s *Store) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if err := s.checkWriteStallLocked(); err != nil {
		return err
	}

	s.seqNo++

	rec := WALRecord{Op: WALOpDel, SeqNo: s.seqNo, Key: key}
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mem.Delete(key, s.seqNo)
	s.metrics.DeletesTotal.Add(1)
	return s.rotateIfNeededLocked()
}

// checkWriteStallLocked zaustavlja nove upise kada broj SSTable-a pređe limit.
// To je zaštita kada compaction ne stiže da smanji broj fajlova na vreme.
func (s *Store) checkWriteStallLocked() error {
	if s.cfg.L0StopWrites <= 0 {
		return nil
	}

	n := 0
	if s.version != nil {
		n = len(s.version.SSTables)
	}
	if n >= s.cfg.L0StopWrites {
		s.log.Warn("write stall",
			"live_sst", n,
			"limit", s.cfg.L0StopWrites,
		)
		return fmt.Errorf("%w: live_sst=%d limit=%d", ErrWriteStall, n, s.cfg.L0StopWrites)
	}
	return nil
}

// rotateIfNeededLocked zamrzava pun aktivni memtable i zakazuje njegov flush.
// Caller već mora da drži s.mu za pisanje.
func (s *Store) rotateIfNeededLocked() error {
	if s.mem.Bytes() < int64(s.cfg.MemtableMaxBytes) {
		return nil
	}

	// Ne prihvatamo dalje upise ako flush zaostaje i troši previše memorije.
	if s.cfg.MaxImmutableTables > 0 && len(s.immutables) >= s.cfg.MaxImmutableTables {
		return ErrTooManyImmutables
	}

	s.log.Info("rotate",
		"seq_no", s.seqNo,
		"memtable_bytes", s.mem.Bytes(),
	)

	s.immutables = append(s.immutables, s.mem)
	s.mem = newMemtable()

	if s.bg != nil {
		s.bg.enqueueFlush()
	}
	return nil
}

// flushPendingImmutables prazni immutable red od najstarijeg ka najnovijem.
func (s *Store) flushPendingImmutables() error {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return ErrStoreClosed
		}
		if len(s.immutables) == 0 {
			s.mu.Unlock()
			return nil
		}
		err := s.flushOldestImmutableLocked()
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

// flushOldestImmutableLocked pretvara najstariji immutable memtable u SSTable.
// Nova tabela postaje vidljiva tek kada je njen zapis uspešno sačuvan u manifestu.
func (s *Store) flushOldestImmutableLocked() error {
	if len(s.immutables) == 0 {
		return nil
	}

	start := time.Now()

	imm := s.immutables[0]
	entries := imm.AllEntries()
	if len(entries) == 0 {
		s.immutables = s.immutables[1:]
		return nil
	}

	id := s.manifest.nextTableID()
	filename := fmt.Sprintf("%06d.sst", id)
	path := filepath.Join(s.cfg.DataDir, filename)

	// Memtable vraća sortirane zapise, što SSTable writer očekuje kao ulaz.
	w := NewSSTableWriter(s.cfg)
	for _, e := range entries {
		w.Add(e.key, e.value, e.seqNo, e.tombstone)
	}

	if err := w.Flush(path); err != nil {
		return fmt.Errorf("flush immutable to sstable: %w", err)
	}

	// Demo-only crash point: SSTable file has been fully written, but this table
	// is not yet present in manifest.json and is therefore not published.
	runDemoCrashPoint(demoCrashPointAfterFlushSSTBeforeManifest)

	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat flushed sstable: %w", err)
	}

	minKey, maxKey := entries[0].key, entries[len(entries)-1].key
	minSeq, maxSeq := entries[0].seqNo, entries[0].seqNo
	for _, e := range entries[1:] {
		if e.seqNo < minSeq {
			minSeq = e.seqNo
		}
		if e.seqNo > maxSeq {
			maxSeq = e.seqNo
		}
	}

	info := ManifestTable{
		ID:       id,
		File:     filename,
		MinKey:   string(minKey),
		MaxKey:   string(maxKey),
		MinSeqNo: minSeq,
		MaxSeqNo: maxSeq,
		FileSize: fi.Size(),
	}

	// Manifest je trajni izvor istine o SSTable fajlovima koji pripadaju store-u.
	newManifest := s.manifest.withNewTable(info)
	if err := saveManifest(s.cfg, newManifest); err != nil {
		return fmt.Errorf("save manifest after flush: %w", err)
	}

	reader, err := OpenSSTableReader(path)
	if err != nil {
		return fmt.Errorf("open freshly flushed sstable: %w", err)
	}

	// Tek sada je tabela i na disku i evidentirana u manifestu, pa je objavljujemo.
	s.version = s.version.withPublishedFlush(reader, newManifest.Epoch)
	s.manifest = newManifest
	s.immutables = s.immutables[1:]

	// WAL se resetuje samo kada više nema podataka koji čekaju flush.
	if len(s.immutables) == 0 && s.mem.Len() == 0 {
		if err := s.wal.Reset(); err != nil {
			return fmt.Errorf("reset wal after flush: %w", err)
		}
	}

	durMs := time.Since(start).Milliseconds()
	s.metrics.FlushesTotal.Add(1)
	s.metrics.LastFlushDurationMs.Store(durMs)
	s.log.Info("flush publish",
		"sst_id", id,
		"entries", len(entries),
		"duration_ms", durMs,
		"epoch", newManifest.Epoch,
	)

	return nil
}

// Compact pokreće jedno spajanje SSTable-a kada compaction policy pronađe posao.
// Compaction se preskače dok postoje immutable memtable-ovi koji čekaju flush.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if len(s.immutables) > 0 {
		return nil
	}

	nextManifest, err := runCompactionOnce(s.cfg, s.manifest, &s.metrics, s.log)
	if err != nil {
		return err
	}
	if nextManifest == s.manifest {
		return nil
	}

	// Nova verzija mora imati readere za ceo skup tabela iz novog manifesta.
	readers := make([]*SSTableReader, 0, len(nextManifest.Tables))
	for _, table := range nextManifest.Tables {
		reader, err := OpenSSTableReader(filepath.Join(s.cfg.DataDir, table.File))
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
			return fmt.Errorf("reopen sstables after compaction: %w", err)
		}
		readers = append(readers, reader)
	}

	oldVersion := s.version
	s.version = newVersionFromManifest(nextManifest, readers)
	s.manifest = nextManifest

	if oldVersion != nil {
		_ = oldVersion.Close()
	}
	return nil
}

// ForceFlush rotira aktivni memtable, čak i ako nije pun, i sinhrono flush-uje
// sve immutable memtable-ove. Korisno je za testove i kontrolisano gašenje.
func (s *Store) ForceFlush() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrStoreClosed
	}
	if err := s.rotateActiveIfNonEmptyLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	return s.flushPendingImmutables()
}

// rotateActiveIfNonEmptyLocked rotira aktivni memtable bez obzira na njegovu veličinu.
// Caller već mora da drži s.mu za pisanje.
func (s *Store) rotateActiveIfNonEmptyLocked() error {
	if s.mem.Len() == 0 {
		return nil
	}
	if s.cfg.MaxImmutableTables > 0 && len(s.immutables) >= s.cfg.MaxImmutableTables {
		return ErrTooManyImmutables
	}
	s.immutables = append(s.immutables, s.mem)
	s.mem = newMemtable()
	return nil
}

// FlushAll sinhrono flush-uje samo memtable-ove koji su već immutable.
func (s *Store) FlushAll() error {
	return s.flushPendingImmutables()
}

// Stats vraća konzistentan snapshot stanja store-a i trenutnih metrika.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.stats.BytesWritten = s.wal.BytesWritten()
	s.stats.LastSeqNo = s.seqNo
	s.stats.ActiveSegmentID = s.wal.ActiveSegmentID()
	s.stats.TotalWALSegments = s.wal.TotalSegments()
	s.stats.ActiveEntries = s.mem.Len()
	s.stats.ActiveBytes = s.mem.Bytes()
	s.stats.SSTCount = len(s.version.SSTables)

	var sstTotalBytes int64
	for _, t := range s.manifest.Tables {
		sstTotalBytes += t.FileSize
	}
	s.stats.SSTTotalBytes = sstTotalBytes

	s.stats.ImmutablesCount = len(s.immutables)
	s.stats.ImmutablesBytes = 0
	for _, imm := range s.immutables {
		s.stats.ImmutablesBytes += imm.Bytes()
	}

	if s.closed {
		s.stats.EngineStatus = "closed"
	} else if s.lastBGError != nil {
		s.stats.EngineStatus = "degraded: " + s.lastBGError.Error()
	} else {
		s.stats.EngineStatus = "open"
	}

	s.stats.Metrics = s.metrics.Snapshot()
	return s.stats
}

// BGStatus vraća trenutno stanje background flush/compaction scheduler-a.
func (s *Store) BGStatus() BGStatus {
	if s.bg == nil {
		return BGStatus{}
	}
	return s.bg.status()
}

// MetricsSnapshot vraća trenutni snapshot brojača bez uzimanja Store lock-a.
func (s *Store) MetricsSnapshot() MetricsSnapshot {
	return s.metrics.Snapshot()
}

// liveSSTCountLocked vraća broj objavljenih SSTable-a. Caller već drži s.mu.
func (s *Store) liveSSTCountLocked() int {
	if s.version == nil {
		return 0
	}
	return len(s.version.SSTables)
}

// immutableCountLocked vraća broj memtable-ova koji čekaju flush. Caller drži s.mu.
func (s *Store) immutableCountLocked() int {
	return len(s.immutables)
}

// liveSSTCount uzima read lock za callere koji već nemaju Store lock.
func (s *Store) liveSSTCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.liveSSTCountLocked()
}

// immutableCount uzima read lock za callere koji već nemaju Store lock.
func (s *Store) immutableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.immutableCountLocked()
}

// recordBackgroundError čuva poslednju grešku iz asinhronog posla za Stats().
func (s *Store) recordBackgroundError(op string, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBGError = fmt.Errorf("%s: %w", op, err)
}

// Close koristi graceful shutdown kao podrazumevano ponašanje.
func (s *Store) Close() error {
	return s.CloseGraceful()
}

// CloseGraceful gasi worker-e, flush-uje immutable memtable-ove, a zatim zatvara
// WAL i SSTable readere. Aktivni memtable ostaje bezbedan jer je pokriven WAL-om.
func (s *Store) CloseGraceful() error {
	if s.bg != nil {
		s.bg.stopGraceful()
	}
	return s.finishClose(true)
}

// CloseFast gasi worker-e i zatvara fajlove bez čekanja da se immutable tabele flush-uju.
// Neupisani podaci ostaju dostupni za recovery kroz WAL.
func (s *Store) CloseFast() error {
	if s.bg != nil {
		s.bg.stopFast()
	}
	return s.finishClose(false)
}

// finishClose zatvara resurse nakon što su background worker-i zaustavljeni.
func (s *Store) finishClose(drainImmutables bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	if drainImmutables {
		for len(s.immutables) > 0 {
			if err := s.flushOldestImmutableLocked(); err != nil {
				closeErr := s.wal.Close()
				_ = s.version.Close()
				s.closed = true
				s.stats.EngineStatus = "closed"
				if closeErr != nil {
					return fmt.Errorf("flush on close: %v; wal close: %w", err, closeErr)
				}
				return fmt.Errorf("flush on close: %w", err)
			}
		}
	}

	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := s.version.Close(); err != nil {
		return err
	}

	s.closed = true
	s.stats.EngineStatus = "closed"
	return nil
}
