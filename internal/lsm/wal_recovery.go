package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// ReplayWAL čita sve WAL segmente redom i vraća zapise potrebne za oporavak memtable-a.
func ReplayWAL(cfg Config) ([]WALRecord, error) {
	dir := walDirectory(cfg.DataDir)

	ids, err := listWALSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	var records []WALRecord

	// Segmenti se replay-uju po rastućem ID-u da bi se očuvao redosled upisa.
	for _, id := range ids {
		path := walSegmentPath(dir, id)

		segmentRecords, err := replayWALSegment(path)
		if err != nil {
			return nil, fmt.Errorf("replay wal segment %06d: %w", id, err)
		}

		records = append(records, segmentRecords...)
	}

	return records, nil
}

// replayWALSegment vraća sve kompletne i validne zapise iz jednog WAL segmenta.
// Ako je crash ostavio nepotpun poslednji zapis, taj rep se odbacuje i fajl skraćuje.
func replayWALSegment(path string) ([]WALRecord, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open wal segment: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat wal segment: %w", err)
	}

	if info.Size() < walSegmentHeaderSize {
		return nil, fmt.Errorf("%w: segment is smaller than header", ErrCorruptionDetected)
	}

	// Pre podataka proveravamo da fajl zaista koristi očekivani WAL format.
	header := make([]byte, walSegmentHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("read wal segment header: %w", err)
	}

	if binary.LittleEndian.Uint32(header[0:4]) != walSegmentMagic {
		return nil, fmt.Errorf("%w: bad wal segment magic", ErrCorruptionDetected)
	}

	if header[4] != walSegmentVersion {
		return nil, fmt.Errorf(
			"%w: unsupported wal segment version %d",
			ErrCorruptionDetected,
			header[4],
		)
	}

	var records []WALRecord
	// offset uvek pokazuje kraj poslednjeg potpuno validnog WAL zapisa.
	offset := int64(walSegmentHeaderSize)

	for {
		record, recordLen, err := readWALRecord(file)

		if err == io.EOF {
			break
		}

		if err != nil {
			// Nagli prekid može ostaviti samo nepotpun poslednji zapis. Prethodni
			// validni zapisi su bezbedni, pa odbacujemo isključivo nekompletan rep.
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if err := file.Truncate(offset); err != nil {
					return nil, fmt.Errorf("truncate incomplete wal tail: %w", err)
				}

				// Truncate mora da se sinhronizuje da se isti oštećeni rep ne čita opet.
				if err := file.Sync(); err != nil {
					return nil, fmt.Errorf("sync truncated wal segment: %w", err)
				}

				break
			}

			// Neispravan CRC ili nevalidan record nije normalan kraj fajla.
			// Ne nastavljamo recovery sa potencijalno pogrešnim podacima.
			return nil, fmt.Errorf(
				"decode wal record at byte offset %d: %w",
				offset,
				err,
			)
		}

		records = append(records, record)
		offset += int64(recordLen)
	}

	return records, nil
}

// readWALRecord čita tačno jedan zapis u formatu koji pravi WALRecord.Encode.
// Vraća io.EOF samo kada je fajl čist i nema više bajtova za sledeći zapis.
func readWALRecord(file *os.File) (WALRecord, int, error) {
	header := make([]byte, walHeaderSize)

	n, err := io.ReadFull(file, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return WALRecord{}, 0, io.EOF
		}

		// Delimično zaglavlje znači da se crash desio tokom upisa poslednjeg zapisa.
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return WALRecord{}, 0, io.ErrUnexpectedEOF
		}

		return WALRecord{}, 0, err
	}

	keyLen := binary.LittleEndian.Uint32(
		header[walKeyLenOffset:walValueLenOffset],
	)
	valueLen := binary.LittleEndian.Uint32(
		header[walValueLenOffset:walCRCOffset],
	)

	// Ograničenje sprečava oštećen length field da izazove ogromnu alokaciju.
	const maxWALRecordBytes = 64 * 1024 * 1024

	payloadLen := uint64(keyLen) + uint64(valueLen)
	if payloadLen > maxWALRecordBytes {
		return WALRecord{}, 0, fmt.Errorf(
			"%w: wal record payload too large: %d bytes",
			ErrCorruptionDetected,
			payloadLen,
		)
	}

	recordLen := walHeaderSize + int(payloadLen)
	recordBytes := make([]byte, recordLen)
	copy(recordBytes, header)

	if payloadLen > 0 {
		if _, err := io.ReadFull(file, recordBytes[walHeaderSize:]); err != nil {
			// Delimičan key ili value se tretira kao nepotpun poslednji zapis.
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return WALRecord{}, 0, io.ErrUnexpectedEOF
			}
			return WALRecord{}, 0, err
		}
	}

	// Decode proverava CRC, granice zapisa i semantičku validnost operacije.
	record, err := DecodeWALRecord(recordBytes)
	if err != nil {
		return WALRecord{}, 0, err
	}

	return record, recordLen, nil
}
