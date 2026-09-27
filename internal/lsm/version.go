package lsm

import "errors"

// Version je immutable snapshot trenutno objavljenih SSTable-ova.
// Reader-i su poređani od najnovije ka najstarijoj tabeli, pa prvi pogodak
// odgovara pravilu da noviji upis ima prednost nad starijim.
type Version struct {
	Epoch uint64

	// SSTables sadrži jedan reader za svaku live tabelu, newest-first.
	SSTables []*SSTableReader
}

// newVersionFromManifest pravi read view iz učitanog manifesta i već otvorenih reader-a.
// readers mora biti u istom redosledu kao m.Tables.
func newVersionFromManifest(m *Manifest, readers []*SSTableReader) *Version {
	return &Version{
		Epoch:    m.Epoch,
		SSTables: readers,
	}
}

// Get pretražuje SSTable-ove redom od najnovijeg ka najstarijem.
// Tombstone se vraća caller-u jer i on predstavlja najnovije stanje ključa.
func (v *Version) Get(key []byte, metrics *Metrics) (sstEntry, error) {
	for _, r := range v.SSTables {
		// Reader koristi isti Metrics objekat da evidentira bloom probe i block read-ove.
		r.metrics = metrics

		entry, err := r.Get(key)
		if err == nil {
			return entry, nil
		}
		if errors.Is(err, ErrNotFound) {
			// Tabela sigurno nema ključ; nastavljamo na stariju tabelu.
			continue
		}

		// I/O i corruption greške ne smeju biti maskirane starijom vrednošću.
		return sstEntry{}, err
	}
	return sstEntry{}, ErrNotFound
}

// Close zatvara sve readere koje ova verzija poseduje.
// Bezbedno je pozvati ga i kada nema nijedne SSTable tabele.
func (v *Version) Close() error {
	for _, r := range v.SSTables {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

// withPublishedFlush pravi novu verziju sa sveže flush-ovanom tabelom na početku.
// Originalna verzija ostaje neizmenjena dok store ne objavi novu.
func (v *Version) withPublishedFlush(newReader *SSTableReader, newEpoch uint64) *Version {
	tables := make([]*SSTableReader, 0, len(v.SSTables)+1)
	tables = append(tables, newReader)
	tables = append(tables, v.SSTables...)

	return &Version{
		Epoch:    newEpoch,
		SSTables: tables,
	}
}
