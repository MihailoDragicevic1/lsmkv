package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTableReader učitava metadata SSTable-a u memoriju i omogućava point lookup.
// Data blokovi ostaju na disku i čitaju se tek kada su potrebni.
type SSTableReader struct {
	path        string
	index       []sstIndexEntry
	bloom       *bloomFilter
	size        int64
	indexOffset int64

	// metrics postavlja Version.Get; nil znači da se metrike ne prikupljaju.
	metrics *Metrics
}

// OpenSSTableReader učitava footer, bloom filter i indeks, ali ne učitava data blokove.
func OpenSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sst %s: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat sst: %w", err)
	}
	size := info.Size()
	if size < 16 {
		return nil, fmt.Errorf("sst file too small (%d bytes): %w", size, ErrCorruptionDetected)
	}

	// Footer je poslednjih 16 bajtova: indexOffset pa bloomOffset.
	footer := make([]byte, 16)
	if _, err := f.ReadAt(footer, size-16); err != nil {
		return nil, fmt.Errorf("read sst footer: %w", err)
	}
	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footer[8:16])

	// Offset-i moraju ostati unutar dela fajla pre footera i u očekivanom redosledu.
	if int64(indexOffset) < 0 || int64(bloomOffset) < 0 {
		return nil, fmt.Errorf("negative offsets in sst footer: %w", ErrCorruptionDetected)
	}
	if int64(indexOffset) > size-16 || int64(bloomOffset) > size-16 {
		return nil, fmt.Errorf("footer offsets out of bounds: %w", ErrCorruptionDetected)
	}
	if indexOffset > bloomOffset {
		return nil, fmt.Errorf("invalid footer offsets order: %w", ErrCorruptionDetected)
	}

	// Bloom zauzima prostor od bloomOffset-a do početka footera.
	bloomSize := (size - 16) - int64(bloomOffset)
	if bloomSize < 0 {
		return nil, fmt.Errorf("invalid bloom offset in sst: %w", ErrCorruptionDetected)
	}
	bloomBytes := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomBytes, int64(bloomOffset)); err != nil {
		return nil, fmt.Errorf("read sst bloom: %w", err)
	}
	bloom, err := unmarshalBloom(bloomBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal sst bloom: %w", err)
	}

	// Index zauzima prostor od indexOffset-a do bloomOffset-a.
	indexSize := int64(bloomOffset) - int64(indexOffset)
	if indexSize < 0 {
		return nil, fmt.Errorf("invalid index offset in sst: %w", ErrCorruptionDetected)
	}
	indexBytes := make([]byte, indexSize)
	if _, err := f.ReadAt(indexBytes, int64(indexOffset)); err != nil {
		return nil, fmt.Errorf("read sst index: %w", err)
	}
	index, err := decodeIndexBlock(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("decode sst index: %w", err)
	}

	// Indeks mora uredno da deli data deo fajla na rastuće blokove.
	if len(index) > 0 {
		if index[0].byteOffset != 0 {
			return nil, fmt.Errorf(
				"sst index first block must start at offset 0: %w",
				ErrCorruptionDetected,
			)
		}

		for i, entry := range index {
			if int64(entry.byteOffset) >= int64(indexOffset) {
				return nil, fmt.Errorf(
					"sst index block offset out of data range: %w",
					ErrCorruptionDetected,
				)
			}

			if i == 0 {
				continue
			}

			previous := index[i-1]
			if entry.byteOffset <= previous.byteOffset {
				return nil, fmt.Errorf(
					"sst index offsets must be strictly increasing: %w",
					ErrCorruptionDetected,
				)
			}

			if string(entry.firstKey) <= string(previous.firstKey) {
				return nil, fmt.Errorf(
					"sst index keys must be strictly increasing: %w",
					ErrCorruptionDetected,
				)
			}
		}
	}

	return &SSTableReader{
		path:        path,
		index:       index,
		bloom:       bloom,
		size:        size,
		indexOffset: int64(indexOffset),
	}, nil
}

// Get prolazi kroz bloom filter, indeks i samo jedan data blok da pronađe ključ.
func (r *SSTableReader) Get(key []byte) (sstEntry, error) {
	// Bloom miss je definitivan, zato tada ne otvaramo SSTable fajl.
	if r.metrics != nil {
		r.metrics.BloomChecksTotal.Add(1)
	}
	if !r.bloom.mayContain(key) {
		if r.metrics != nil {
			r.metrics.BloomSkipsTotal.Add(1)
		}
		return sstEntry{}, ErrNotFound
	}

	// Indeks bira poslednji blok čiji je prvi ključ manji ili jednak traženom ključu.
	blockOffset, blockEnd := r.locateBlock(key)
	if blockOffset < 0 {
		return sstEntry{}, ErrNotFound
	}

	f, err := os.Open(r.path)
	if err != nil {
		return sstEntry{}, fmt.Errorf("open sst for read: %w", err)
	}
	defer f.Close()

	blockSize := blockEnd - blockOffset
	blockBytes := make([]byte, blockSize)
	if _, err := f.ReadAt(blockBytes, blockOffset); err != nil {
		return sstEntry{}, fmt.Errorf("read sst block: %w", err)
	}
	if r.metrics != nil {
		r.metrics.BlockReadsTotal.Add(1)
	}

	// Skeniramo samo odabrani blok, ne ceo SSTable fajl.
	return scanBlock(blockBytes, key)
}

// AllEntries čita sve data blokove po redosledu ključeva.
// Compaction koristi i tombstone zapise jer učestvuju u newest-write-wins spajanju.
func (r *SSTableReader) AllEntries() ([]sstEntry, error) {
	if len(r.index) == 0 {
		return nil, nil
	}

	f, err := os.Open(r.path)
	if err != nil {
		return nil, fmt.Errorf("open sstable for full scan: %w", err)
	}
	defer f.Close()

	var out []sstEntry

	for i := 0; i < len(r.index); i++ {
		blockStart := int64(r.index[i].byteOffset)

		var blockEnd int64
		if i+1 < len(r.index) {
			blockEnd = int64(r.index[i+1].byteOffset)
		} else {
			blockEnd = r.indexOffset
		}

		if blockStart < 0 || blockEnd < blockStart {
			return nil, fmt.Errorf("invalid sstable block range: %w", ErrCorruptionDetected)
		}

		blockSize := blockEnd - blockStart
		if blockSize == 0 {
			continue
		}

		block := make([]byte, blockSize)
		if _, err := f.ReadAt(block, blockStart); err != nil {
			return nil, fmt.Errorf("read sstable block: %w", err)
		}

		br := bufio.NewReader(newByteReader(block))
		for {
			entry, err := readSSEntry(br)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			out = append(out, entry)
		}
	}

	return out, nil
}

// Close trenutno ne radi ništa jer reader ne zadržava otvoren file descriptor.
// Metoda postoji da Version može jedinstveno da zatvara readere i da API ostane
// stabilan ako se kasnije uvede persistentni handle ili memory-mapped fajl.
func (r *SSTableReader) Close() error {
	return nil
}

// locateBlock binary search-om bira poslednji indeks entry čiji firstKey nije veći od key-a.
// Vraća opseg data bloka ili -1, -1 ako nijedan blok ne može sadržati ključ.
func (r *SSTableReader) locateBlock(key []byte) (start int64, end int64) {
	if len(r.index) == 0 {
		return -1, -1
	}

	n := len(r.index)
	pos := sort.Search(n, func(i int) bool {
		return string(r.index[i].firstKey) > string(key)
	})

	if pos == 0 {
		return -1, -1
	}
	blockIdx := pos - 1

	blockStart := int64(r.index[blockIdx].byteOffset)
	var blockEnd int64
	if blockIdx+1 < n {
		blockEnd = int64(r.index[blockIdx+1].byteOffset)
	} else {
		blockEnd = r.indexOffset
	}

	return blockStart, blockEnd
}

// scanBlock sekvencijalno čita zapise iz jednog data bloka dok ne pronađe ključ.
func scanBlock(block []byte, key []byte) (sstEntry, error) {
	r := bufio.NewReader(newByteReader(block))
	for {
		entry, err := readSSEntry(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return sstEntry{}, err
		}
		if string(entry.key) == string(key) {
			return entry, nil
		}
	}
	return sstEntry{}, ErrNotFound
}

// readSSEntry dekodira jedan entry iz data bloka i proverava njegov CRC.
// Format mora ostati usklađen sa writeSSEntry iz sstable_writer.go.
func readSSEntry(r *bufio.Reader) (sstEntry, error) {
	header := make([]byte, 21)
	if _, err := io.ReadFull(r, header); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return sstEntry{}, io.EOF
		}
		return sstEntry{}, fmt.Errorf("read sst entry header: %w", err)
	}

	flags := header[0]
	seqNo := binary.LittleEndian.Uint64(header[1:9])
	keyLen := binary.LittleEndian.Uint32(header[9:13])
	valueLen := binary.LittleEndian.Uint32(header[13:17])
	storedCRC := binary.LittleEndian.Uint32(header[17:21])

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return sstEntry{}, fmt.Errorf("read sst entry key: %w", err)
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return sstEntry{}, fmt.Errorf("read sst entry value: %w", err)
	}

	// CRC pokriva header bez CRC polja, zatim key i value bajtove.
	checksumInput := make([]byte, 0, 17+len(key)+len(value))
	checksumInput = append(checksumInput, header[:17]...)
	checksumInput = append(checksumInput, key...)
	checksumInput = append(checksumInput, value...)

	computed := crc32.Checksum(checksumInput, crc32.MakeTable(crc32.Castagnoli))
	if computed != storedCRC {
		return sstEntry{}, fmt.Errorf("crc mismatch in sst entry: %w", ErrCorruptionDetected)
	}

	return sstEntry{
		key:       key,
		value:     value,
		seqNo:     seqNo,
		tombstone: flags&1 == 1,
	}, nil
}

// decodeIndexBlock dekodira index blok u entry-je koje locateBlock koristi za binary search.
// Format mora ostati usklađen sa writeSSIndexEntry iz sstable_writer.go.
func decodeIndexBlock(data []byte) ([]sstIndexEntry, error) {
	var entries []sstIndexEntry
	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("index block truncated at keyLen: %w", ErrCorruptionDetected)
		}
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if pos+keyLen > len(data) {
			return nil, fmt.Errorf("index block truncated at key: %w", ErrCorruptionDetected)
		}
		key := append([]byte(nil), data[pos:pos+keyLen]...)
		pos += keyLen

		if pos+8 > len(data) {
			return nil, fmt.Errorf("index block truncated at offset: %w", ErrCorruptionDetected)
		}
		offset := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8

		entries = append(entries, sstIndexEntry{firstKey: key, byteOffset: offset})
	}
	return entries, nil
}

// byteReader prilagođava []byte interfejsu io.Reader za parsiranje data bloka u memoriji.
type byteReader struct {
	data []byte
	pos  int
}

// newByteReader pravi reader koji sekvencijalno čita dati bajt niz.
func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

// Read kopira sledeći deo data niza i vraća EOF kada nema više bajtova.
func (b *byteReader) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
