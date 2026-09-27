package lsm

import "sync/atomic"

// Metrics čuva kumulativne brojače rada engine-a od trenutka kada je store otvoren.
// Polja su atomic jer ih read path i background worker-i ažuriraju paralelno.
type Metrics struct {
	// Write path
	PutsTotal    atomic.Int64
	DeletesTotal atomic.Int64

	// Read path na SSTable sloju
	BloomChecksTotal atomic.Int64 // Broj provera bloom filter-a.
	BloomSkipsTotal  atomic.Int64 // Bloom miss: čitanje sa diska je preskočeno.
	BlockReadsTotal  atomic.Int64 // Broj data blokova pročitanih sa diska.

	// Background poslovi
	FlushesTotal     atomic.Int64
	CompactionsTotal atomic.Int64

	// Poslednje izmereno trajanje posla u milisekundama.
	LastFlushDurationMs   atomic.Int64
	LastCompactDurationMs atomic.Int64
}

// MetricsSnapshot je obična kopija trenutnih vrednosti, pogodna za vraćanje iz API-ja.
type MetricsSnapshot struct {
	PutsTotal             int64
	DeletesTotal          int64
	BloomChecksTotal      int64
	BloomSkipsTotal       int64
	BlockReadsTotal       int64
	FlushesTotal          int64
	CompactionsTotal      int64
	LastFlushDurationMs   int64
	LastCompactDurationMs int64
}

// Snapshot čita sve brojače bez Store lock-a.
// Polja se učitavaju pojedinačno, pa snapshot služi za observability, ne kao transakcioni presek.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		PutsTotal:             m.PutsTotal.Load(),
		DeletesTotal:          m.DeletesTotal.Load(),
		BloomChecksTotal:      m.BloomChecksTotal.Load(),
		BloomSkipsTotal:       m.BloomSkipsTotal.Load(),
		BlockReadsTotal:       m.BlockReadsTotal.Load(),
		FlushesTotal:          m.FlushesTotal.Load(),
		CompactionsTotal:      m.CompactionsTotal.Load(),
		LastFlushDurationMs:   m.LastFlushDurationMs.Load(),
		LastCompactDurationMs: m.LastCompactDurationMs.Load(),
	}
}
