package lsm

import (
	"encoding/binary"
	"math"
)

// bloomFilter je kompaktna bit mapa za brzu proveru da li ključ možda postoji.
// Može dati false positive, ali nikada ne sme dati false negative.
type bloomFilter struct {
	bits []byte // Bitovi su spakovani, osam bitova po bajtu.
	m    uint64 // Ukupan broj bitova u filteru.
	k    uint64 // Broj hash pozicija koje proveravamo za jedan ključ.
}

// newBloomFilter pravi filter za očekivani broj ključeva i zadatu false-positive stopu.
func newBloomFilter(n int, fpRate float64) *bloomFilter {
	// Optimalan broj bitova: m = -n * ln(p) / ln(2)^2.
	m := uint64(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))))
	if m < 64 {
		m = 64
	}
	// Bit mapa mora imati ceo broj bajtova.
	m = (m + 7) &^ 7

	// Optimalan broj hash funkcija: k = (m / n) * ln(2).
	k := uint64(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}

	return &bloomFilter{
		bits: make([]byte, m/8),
		m:    m,
		k:    k,
	}
}

// add postavlja k bitova izračunatih iz ključa.
func (bf *bloomFilter) add(key []byte) {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < bf.k; i++ {
		// Double hashing pravi više pozicija bez računanja k nezavisnih hash-eva.
		pos := (h1 + i*h2) % bf.m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// mayContain vraća false samo kada ključ sigurno nije dodat u filter.
// true znači da ključ možda postoji i reader tada mora proveriti SSTable.
func (bf *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := bloomHash(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// marshal pretvara metadata i bit mapu u format koji se čuva u SSTable fajlu.
// Format: m | k | bits.
func (bf *bloomFilter) marshal() []byte {
	header := make([]byte, 16)
	binary.LittleEndian.PutUint64(header[0:8], bf.m)
	binary.LittleEndian.PutUint64(header[8:16], bf.k)
	out := append(header, bf.bits...)
	return out
}

// unmarshalBloom učitava bloom filter iz SSTable-a i proverava osnovnu konzistentnost formata.
func unmarshalBloom(data []byte) (*bloomFilter, error) {
	if len(data) < 16 {
		return nil, ErrCorruptionDetected
	}
	m := binary.LittleEndian.Uint64(data[0:8])
	k := binary.LittleEndian.Uint64(data[8:16])
	bits := append([]byte(nil), data[16:]...)

	// m mora opisivati ceo broj bajtova, a payload mora tačno odgovarati toj veličini.
	if m == 0 || m%8 != 0 || k == 0 || uint64(len(bits)) != m/8 {
		return nil, ErrCorruptionDetected
	}

	return &bloomFilter{
		bits: bits,
		m:    m,
		k:    k,
	}, nil
}

// bloomHash vraća dve 64-bitne vrednosti za double hashing.
// Mešanje je jednostavno i lokalno, bez dodatne zavisnosti.
func bloomHash(key []byte) (uint64, uint64) {
	var h1, h2 uint64
	h1 = 14695981039346656037
	h2 = 1099511628211

	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= 1099511628211
		h2 ^= uint64(b)
		h2 *= 1000000007
	}
	return h1, h2
}

// buildBloomFilter dodaje ključeve svih SSTable entry-ja i vraća spreman binarni payload.
func buildBloomFilter(entries []sstEntry, fpRate float64) []byte {
	if len(entries) == 0 {
		return newBloomFilter(1, fpRate).marshal()
	}

	bf := newBloomFilter(len(entries), fpRate)
	for _, e := range entries {
		bf.add(e.key)
	}
	return bf.marshal()
}
