package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// sstEntry predstavlja jedan key/value zapis koji će biti upisan u SSTable.
// tombstone=true označava brisanje ključa, a ne regularnu vrednost.
type sstEntry struct {
	key       []byte
	value     []byte
	seqNo     uint64
	tombstone bool
}

// +----------------+----------------+----------------+----------------+
// | DATA BLOCKS    | INDEX BLOCK    | BLOOM FILTER   | FOOTER         |
// +----------------+----------------+----------------+----------------+
// ^                ^                ^                ^
// 0                indexOffset      bloomOffset      kraj fajla

// sstIndexEntry pamti prvi ključ bloka i njegov offset u SSTable fajlu.
// Reader koristi indeks da binary search-om pronađe blok koji može sadržati ključ.
type sstIndexEntry struct {
	firstKey   []byte
	byteOffset uint64
}

// SSTableWriter prikuplja zapise i od njih pravi jedan kompletan immutable SSTable.
type SSTableWriter struct {
	cfg     Config
	entries []sstEntry
}

// NewSSTableWriter pravi writer koji koristi podešavanja store-a za blokove i bloom filter.
func NewSSTableWriter(cfg Config) *SSTableWriter {
	return &SSTableWriter{cfg: cfg}
}

// Add dodaje zapis za budući flush. Kopije sprečavaju da caller naknadno promeni podatke.
func (w *SSTableWriter) Add(key, value []byte, seqNo uint64, tombstone bool) {
	w.entries = append(w.entries, sstEntry{
		key:       append([]byte(nil), key...),
		value:     append([]byte(nil), value...),
		seqNo:     seqNo,
		tombstone: tombstone,
	})
}

// Flush sortira zapise, pravi SSTable u privremenom fajlu i zatim ga atomically
// preimenuje na finalnu putanju. Finalni fajl se pojavljuje tek kada je kompletan.
func (w *SSTableWriter) Flush(path string) error {
	// SSTable mora biti sortiran po ključu da bi indeks i čitanje po blokovima radili.
	sort.Slice(w.entries, func(i, j int) bool {
		ki := string(w.entries[i].key)
		kj := string(w.entries[j].key)
		return ki < kj
	})

	// Crash tokom upisa ostavlja samo .tmp fajl, a ne delimično vidljiv SSTable.
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create sst tmp: %w", err)
	}

	// Bafer smanjuje broj sistemskih poziva dok se pišu data blokovi.
	bw := bufio.NewWriterSize(f, 65536)

	var (
		index        []sstIndexEntry
		offset       uint64
		blockStart   uint64
		blockEntries int
	)

	// Data deo se deli na blokove približno veličine cfg.BlockSize.
	for i, entry := range w.entries {
		if blockEntries == 0 {
			// Indeks za blok pokazuje na njegov prvi ključ i početak u fajlu.
			index = append(index, sstIndexEntry{
				firstKey:   append([]byte(nil), entry.key...),
				byteOffset: offset,
			})
			blockStart = offset
		}

		n, err := writeSSEntry(bw, entry)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("write sst entry %d: %w", i, err)
		}
		offset += uint64(n)
		blockEntries++

		// Sledeći zapis započinje novi blok kada je trenutni dovoljno velik.
		if int(offset-blockStart) >= w.cfg.BlockSize {
			blockEntries = 0
		}
	}

	// Pre prelaska na indeks moramo isprazniti bafer sa data blokovima u fajl.
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush sst data: %w", err)
	}

	// Offset je granica između data dela i index bloka.
	indexOffset := offset

	// Index sadrži dovoljno informacija da reader izabere odgovarajući data blok.
	ibw := bufio.NewWriter(f)
	for _, ie := range index {
		n, err := writeSSIndexEntry(ibw, ie)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("write sst index: %w", err)
		}
		offset += uint64(n)
	}
	if err := ibw.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("flush sst index: %w", err)
	}

	// Offset je granica između indeksa i bloom filter-a.
	bloomOffset := offset

	// Bloom filter omogućava reader-u da preskoči tabelu kada ključ sigurno nije u njoj.
	bloom := buildBloomFilter(w.entries, w.cfg.BloomFalsePositive)
	bn, err := f.Write(bloom)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write sst bloom: %w", err)
	}
	offset += uint64(bn)

	// Footer ima fiksnu veličinu i govori reader-u gde počinju indeks i bloom filter.
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomOffset)
	if _, err := f.Write(footer); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write sst footer: %w", err)
	}

	// Sync pre rename-a obezbeđuje da finalni fajl ne pokaže na prazne OS buffere.
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync sst: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close sst tmp: %w", err)
	}

	// Rename unutar istog fajl sistema objavljuje kompletan SSTable odjednom.
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename sst: %w", err)
	}

	return nil
}

// writeSSEntry zapisuje jedan entry u data blok.
// Format: flags | seqNo | keyLen | valueLen | crc | key | value.
func writeSSEntry(w *bufio.Writer, e sstEntry) (int, error) {
	keyLen := len(e.key)
	valueLen := len(e.value)
	const headerSize = 1 + 8 + 4 + 4 + 4
	total := headerSize + keyLen + valueLen

	buf := make([]byte, total)

	var flags byte
	if e.tombstone {
		// Bit 0 označava da zapis predstavlja brisanje.
		flags = 1
	}
	buf[0] = flags
	binary.LittleEndian.PutUint64(buf[1:9], e.seqNo)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(keyLen))
	binary.LittleEndian.PutUint32(buf[13:17], uint32(valueLen))
	copy(buf[headerSize:headerSize+keyLen], e.key)
	copy(buf[headerSize+keyLen:], e.value)

	// CRC pokriva ceo zapis osim samog CRC polja.
	checksum := crc32.Checksum(sstChecksumInput(buf), crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(buf[17:21], checksum)

	n, err := w.Write(buf)
	return n, err
}

// sstChecksumInput vraća header bez CRC polja zajedno sa key/value payload-om.
func sstChecksumInput(buf []byte) []byte {
	out := make([]byte, 0, len(buf)-4)
	out = append(out, buf[:17]...)
	out = append(out, buf[21:]...)
	return out
}

// writeSSIndexEntry zapisuje jedan entry indeksa.
// Format: keyLen | firstKey | byteOffset.
func writeSSIndexEntry(w *bufio.Writer, ie sstIndexEntry) (int, error) {
	keyLen := len(ie.firstKey)
	buf := make([]byte, 4+keyLen+8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keyLen))
	copy(buf[4:4+keyLen], ie.firstKey)
	binary.LittleEndian.PutUint64(buf[4+keyLen:], ie.byteOffset)
	n, err := w.Write(buf)
	return n, err
}
