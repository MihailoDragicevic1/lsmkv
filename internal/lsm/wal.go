package lsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	walDirectoryName = "wal"

	// Header segmenta: magic (4 bajta) + verzija (1 bajt) + reserved (3 bajta).
	walSegmentMagic      uint32 = 0x4C534D57
	walSegmentVersion    byte   = 1
	walSegmentHeaderSize        = 8
)

// WAL upravlja append-only segmentima u kojima se čuvaju upisi pre flush-a u SSTable.
type WAL struct {
	dir          string
	path         string
	file         *os.File
	fsyncEveryN  int
	rollBytes    int64
	appendCount  int
	bytesWritten int64
	activeID     int
	activeSize   int64
	lastSeqNo    uint64
}

// walDirectory vraća direktorijum u kom se čuvaju WAL segmenti za dati store.
func walDirectory(dataDir string) string {
	return filepath.Join(dataDir, walDirectoryName)
}

// walSegmentPath pravi determinističko ime segmenta, npr. 000001.wal.
func walSegmentPath(dir string, id int) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", id))
}

// OpenWAL otvara poslednji postojeći segment ili kreira prvi segment ako WAL ne postoji.
func OpenWAL(cfg Config) (*WAL, error) {
	dir := walDirectory(cfg.DataDir)
	if cfg.WALFsyncEveryN < 0 {
		return nil, fmt.Errorf("%w: wal fsync interval must be >= 0", ErrInvalidArgument)
	}

	// Segment mora imati mesta makar za header i najmanje jedan zapis.
	if cfg.WALSegmentRollBytes <= walSegmentHeaderSize {
		return nil, fmt.Errorf(
			"%w: wal segment roll bytes must be > %d",
			ErrInvalidArgument,
			walSegmentHeaderSize,
		)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}

	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		dir:         dir,
		fsyncEveryN: cfg.WALFsyncEveryN,
		rollBytes:   int64(cfg.WALSegmentRollBytes),
	}

	if len(ids) == 0 {
		if err := wal.openNewSegment(1); err != nil {
			return nil, err
		}
		return wal, nil
	}

	// Nastavljamo append samo u najnoviji segment; stariji su read-only istorija.
	if err := wal.openExistingSegment(ids[len(ids)-1]); err != nil {
		return nil, err
	}

	return wal, nil
}

// openNewSegment kreira segment, trajno upisuje header i postavlja ga kao aktivni.
func (w *WAL) openNewSegment(id int) error {
	path := walSegmentPath(w.dir, id)

	// O_EXCL sprečava slučajno pregazivanje već postojećeg WAL segmenta.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("create wal segment %s: %w", path, err)
	}

	header := make([]byte, walSegmentHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], walSegmentMagic)
	header[4] = walSegmentVersion

	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return fmt.Errorf("write wal segment header: %w", err)
	}

	// Novi segment nije validan za recovery dok njegov header ne bude na disku.
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync wal segment header: %w", err)
	}

	w.file = file
	w.path = path
	w.activeID = id
	w.activeSize = walSegmentHeaderSize
	w.bytesWritten = 0
	w.appendCount = 0

	return nil
}

// openExistingSegment proverava header poslednjeg segmenta i otvara ga za append.
func (w *WAL) openExistingSegment(id int) error {
	path := walSegmentPath(w.dir, id)

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open wal segment %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("stat wal segment: %w", err)
	}

	if info.Size() < walSegmentHeaderSize {
		_ = file.Close()
		return fmt.Errorf("%w: wal segment %s is smaller than header", ErrCorruptionDetected, path)
	}

	// Provera magic-a i verzije sprečava rad nad pogrešnim ili nepodržanim formatom.
	header := make([]byte, walSegmentHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		_ = file.Close()
		return fmt.Errorf("read wal segment header: %w", err)
	}

	if binary.LittleEndian.Uint32(header[0:4]) != walSegmentMagic {
		_ = file.Close()
		return fmt.Errorf("%w: bad wal segment magic in %s", ErrCorruptionDetected, path)
	}

	if header[4] != walSegmentVersion {
		_ = file.Close()
		return fmt.Errorf(
			"%w: unsupported wal segment version %d in %s",
			ErrCorruptionDetected,
			header[4],
			path,
		)
	}

	w.file = file
	w.path = path
	w.activeID = id
	w.activeSize = info.Size()
	w.bytesWritten = info.Size() - walSegmentHeaderSize
	w.appendCount = 0

	return nil
}

// Append serijalizuje zapis i dodaje ga na kraj aktivnog WAL segmenta.
func (w *WAL) Append(record WALRecord) error {
	if w.file == nil {
		return ErrStoreClosed
	}

	encoded, err := record.Encode()
	if err != nil {
		return err
	}

	// Ne delimo zapis između dva segmenta. Novi segment se otvara pre append-a.
	if w.bytesWritten > 0 && w.activeSize+int64(len(encoded)) > w.rollBytes {
		if err := w.roll(); err != nil {
			return err
		}
	}

	n, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("write wal record: %w", err)
	}

	if n != len(encoded) {
		return fmt.Errorf("%w: partial wal write", ErrIOFailure)
	}

	w.appendCount++
	w.bytesWritten += int64(n)
	w.activeSize += int64(n)
	w.lastSeqNo = record.SeqNo

	// fsync je skuplji, pa se po konfiguraciji radi nakon svakog N-tog append-a.
	if w.fsyncEveryN > 0 && w.appendCount%w.fsyncEveryN == 0 {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync wal: %w", err)
		}
	}

	return nil
}

// roll zatvara trenutno aktivni segment nakon sync-a i otvara sledeći segment.
func (w *WAL) roll() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	// Pre prelaska u novi segment obavezno trajno završavamo prethodni.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal before roll: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal before roll: %w", err)
	}

	w.file = nil

	if err := w.openNewSegment(w.activeID + 1); err != nil {
		return err
	}

	return nil
}

// Reset se poziva tek nakon uspešnog flush-a iz memtable-a u SSTable i manifest.
// Tada su svi upisi iz postojećih WAL segmenata trajno pokriveni SSTable-ovima.
func (w *WAL) Reset() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close wal before reset: %w", err)
	}
	w.file = nil

	ids, err := listWALSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		path := walSegmentPath(w.dir, id)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove wal segment %s: %w", path, err)
		}
	}

	if err := w.openNewSegment(1); err != nil {
		return err
	}

	w.lastSeqNo = 0
	return nil
}

// Path vraća putanju trenutno aktivnog WAL segmenta.
func (w *WAL) Path() string {
	return w.path
}

// Dir vraća direktorijum u kom se nalaze svi WAL segmenti.
func (w *WAL) Dir() string {
	return w.dir
}

// BytesWritten vraća broj bajtova upisanih u trenutno aktivni segment bez header-a.
func (w *WAL) BytesWritten() int64 {
	return w.bytesWritten
}

// LastSeqNo vraća sequence number poslednjeg append-ovanog zapisa.
func (w *WAL) LastSeqNo() uint64 {
	return w.lastSeqNo
}

// ActiveSegmentID vraća ID segmenta u koji se trenutno append-uju novi zapisi.
func (w *WAL) ActiveSegmentID() int {
	return w.activeID
}

// TotalSegments vraća broj prepoznatih .wal fajlova u WAL direktorijumu.
func (w *WAL) TotalSegments() int {
	ids, err := listWALSegmentIDs(w.dir)
	if err != nil {
		return 0
	}
	return len(ids)
}

// Close radi završni sync i zatvara aktivni WAL fajl.
func (w *WAL) Close() error {
	if w.file == nil {
		return ErrStoreClosed
	}

	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		w.file = nil
		return fmt.Errorf("sync wal on close: %w", err)
	}

	err := w.file.Close()
	w.file = nil

	if err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	return nil
}

// listWALSegmentIDs pronalazi validno numerisane .wal fajlove i sortira ih po ID-u.
func listWALSegmentIDs(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list wal directory: %w", err)
	}

	ids := make([]int, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}

		idText := strings.TrimSuffix(name, ".wal")
		id, err := strconv.Atoi(idText)
		if err != nil || id <= 0 {
			continue
		}

		ids = append(ids, id)
	}

	sort.Ints(ids)
	return ids, nil
}
