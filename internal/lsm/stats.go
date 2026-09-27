package lsm

// Stats je snapshot stanja store-a koji vraća Store.Stats().
// Kombinuje trenutno strukturno stanje engine-a sa kumulativnim Metrics brojačima.
type Stats struct {
	// Lifecycle engine-a
	EngineStatus string

	// WAL
	ActiveSegmentID  int
	BytesWritten     int64
	TotalWALSegments int

	// Aktivni memtable
	LastSeqNo     uint64
	ActiveEntries int
	ActiveBytes   int64

	// Rotirani memtable-ovi koji čekaju flush
	ImmutablesCount int
	ImmutablesBytes int64

	// Live SSTable fajlovi iz trenutnog manifesta/version-a
	SSTCount      int
	SSTTotalBytes int64

	// Kumulativni brojači od otvaranja store-a.
	Metrics MetricsSnapshot
}
