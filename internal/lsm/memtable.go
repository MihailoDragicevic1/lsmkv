package lsm

import "sort"

// memtableEntry predstavlja poslednje stanje jednog ključa u memtable-u.
// Tombstone označava delete bez fizičkog brisanja starijih vrednosti sa diska.
type memtableEntry struct {
	value     []byte
	seqNo     uint64
	tombstone bool
}

// Memtable čuva najnovije upise u memoriji pre flush-a u SSTable.
// entries sadrži samo poslednju verziju svakog ključa.
type Memtable struct {
	entries map[string]memtableEntry
	bytes   int64
}

// newMemtable pravi prazan aktivni memtable za nove upise.
func newMemtable() *Memtable {
	return &Memtable{
		entries: make(map[string]memtableEntry),
	}
}

// Put upisuje novu vrednost za ključ. Kopiramo value da caller kasnije ne može
// izmenom svog []byte niza da promeni podatke koji su već u memtable-u.
func (m *Memtable) Put(key, value []byte, seqNo uint64) {
	m.applyEntry(key, memtableEntry{
		value: append([]byte(nil), value...),
		seqNo: seqNo,
	})
}

// Delete upisuje tombstone za ključ. Stare vrednosti ostaju u SSTable-ovima dok
// ih compaction bezbedno ne ukloni.
func (m *Memtable) Delete(key []byte, seqNo uint64) {
	m.applyEntry(key, memtableEntry{
		seqNo:     seqNo,
		tombstone: true,
	})
}

// applyEntry zamenjuje trenutno stanje ključa i ažurira približan broj bajtova.
// Memtable čuva samo najnoviju verziju ključa, pa prethodnu veličinu prvo skidamo.
func (m *Memtable) applyEntry(key []byte, entry memtableEntry) {
	k := string(key)

	if old, ok := m.entries[k]; ok {
		m.bytes -= int64(len(k) + len(old.value))
	}

	m.entries[k] = entry
	m.bytes += int64(len(k) + len(entry.value))
}

// Get vraća vrednost, informaciju da li je to tombstone i da li ključ postoji.
// Treći povratni parametar razlikuje nepostojeći ključ od upisanog tombstone-a.
func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	entry, ok := m.entries[string(key)]
	if !ok {
		return nil, false, false
	}

	return entry.value, entry.tombstone, true
}

// Len vraća broj različitih ključeva koje memtable trenutno sadrži.
func (m *Memtable) Len() int {
	return len(m.entries)
}

// Bytes vraća veličinu podataka koju store koristi za odluku o rotaciji memtable-a.
func (m *Memtable) Bytes() int64 {
	return m.bytes
}

// AllEntries pretvara sadržaj memtable-a u sortirane SSTable zapise.
// Mapa nema stabilan redosled iteracije, zato sortiramo po ključu pre flush-a.
func (m *Memtable) AllEntries() []sstEntry {
	entries := make([]sstEntry, 0, len(m.entries))
	for k, e := range m.entries {
		entries = append(entries, sstEntry{
			key:       []byte(k),
			value:     e.value,
			seqNo:     e.seqNo,
			tombstone: e.tombstone,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].key) < string(entries[j].key)
	})
	return entries
}
