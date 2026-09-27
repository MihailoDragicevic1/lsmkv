package lsm

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// compactionPlan opisuje jednu buduću compaction operaciju.
// Plan bira input tabele i ime output tabele, ali sam ne menja disk ni manifest.
type compactionPlan struct {
	Inputs     []ManifestTable
	OutputID   uint64
	OutputFile string
}

// newCompactionPlan bira SSTable-ove za size-tiered compaction i priprema output metadata.
// Vraća nil kada nema makar dve tabele pogodne za spajanje.
func newCompactionPlan(manifest *Manifest, cfg Config) (*compactionPlan, error) {
	if manifest == nil {
		return nil, fmt.Errorf("create compaction plan: nil manifest: %w", ErrInvalidArgument)
	}
	if len(manifest.Tables) < 2 {
		return nil, nil
	}

	fanIn := cfg.SizeTieredFanIn
	if fanIn < 2 {
		fanIn = 2
	}
	ratio := cfg.SizeTieredSizeRatio
	if ratio < 1.0 {
		ratio = 2.0
	}

	picked, ok := pickSizeTiered(manifest.Tables, fanIn, ratio)
	if !ok || len(picked) < 2 {
		return nil, nil
	}

	outputID := manifest.nextTableID()
	return &compactionPlan{
		Inputs:     picked,
		OutputID:   outputID,
		OutputFile: fmt.Sprintf("%06d.sst", outputID),
	}, nil
}

// pickSizeTiered bira do fanIn tabela slične veličine.
// Ako takav skup ne postoji, bira do fanIn najnovijih tabela kao fallback.
func pickSizeTiered(tables []ManifestTable, fanIn int, sizeRatio float64) ([]ManifestTable, bool) {
	n := len(tables)
	if n < 2 {
		return nil, false
	}
	if fanIn < 2 {
		fanIn = 2
	}
	if sizeRatio < 1.0 {
		sizeRatio = 1.0
	}
	k := fanIn
	if k > n {
		k = n
	}

	// Prvo gledamo tabele slične veličine, što je osnova size-tiered compaction-a.
	sorted := make([]ManifestTable, n)
	copy(sorted, tables)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].FileSize != sorted[j].FileSize {
			return sorted[i].FileSize < sorted[j].FileSize
		}
		// Kod iste veličine prednost ima novija tabela radi determinističkog izbora.
		return sorted[i].ID > sorted[j].ID
	})

	for i := 0; i+k <= len(sorted); i++ {
		window := sorted[i : i+k]
		base := window[0].FileSize
		if base <= 0 {
			base = 1
		}

		fit := true
		for _, t := range window {
			if float64(t.FileSize) > float64(base)*sizeRatio {
				fit = false
				break
			}
		}
		if fit {
			return orderTablesNewestFirst(window), true
		}
	}

	// Fallback sprečava da se broj tabela zauvek povećava kada veličine nisu slične.
	byNew := make([]ManifestTable, n)
	copy(byNew, tables)
	sort.SliceStable(byNew, func(i, j int) bool {
		return byNew[i].ID > byNew[j].ID
	})
	fallback := byNew
	if len(fallback) > k {
		fallback = fallback[:k]
	}
	if len(fallback) < 2 {
		return nil, false
	}
	return orderTablesNewestFirst(fallback), true
}

// orderTablesNewestFirst pravi kopiju i poređa tabele tako da noviji input ide prvi u merge.
func orderTablesNewestFirst(in []ManifestTable) []ManifestTable {
	out := make([]ManifestTable, len(in))
	copy(out, in)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ID > out[j].ID
	})
	return out
}

// compactReaders učitava sadržaj input tabela, spaja najnovija stanja ključeva
// i pravi jednu novu SSTable tabelu. Ne menja manifest i ne briše input fajlove.
func compactReaders(outputPath string, cfg Config, readers ...*SSTableReader) (*SSTableReader, error) {
	if len(readers) == 0 {
		return nil, fmt.Errorf("compact readers: %w", ErrInvalidArgument)
	}

	entrySets := make([][]sstEntry, 0, len(readers))
	for _, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("compact readers: nil reader: %w", ErrInvalidArgument)
		}
		entries, err := reader.AllEntries()
		if err != nil {
			return nil, fmt.Errorf("read entries for compaction: %w", err)
		}
		entrySets = append(entrySets, entries)
	}

	// mergeEntries bira entry sa najvećim seqNo za svaki ključ i čuva tombstone-ove.
	merged := mergeEntries(entrySets...)
	if len(merged) == 0 {
		return nil, fmt.Errorf("compact readers produced no entries: %w", ErrInvalidArgument)
	}

	writer := NewSSTableWriter(cfg)
	for _, entry := range merged {
		writer.Add(entry.key, entry.value, entry.seqNo, entry.tombstone)
	}

	if err := writer.Flush(outputPath); err != nil {
		return nil, fmt.Errorf("write compacted sstable: %w", err)
	}

	reader, err := OpenSSTableReader(outputPath)
	if err != nil {
		return nil, fmt.Errorf("open compacted sstable: %w", err)
	}
	return reader, nil
}

// manifestAfterCompaction pravi sledeći manifest nakon uspešnog upisa output tabele.
// Uklanja input tabele iz metadata, dodaje output kao najnoviju i povećava epoch.
func manifestAfterCompaction(
	current *Manifest,
	plan *compactionPlan,
	outputPath string,
	outputEntries []sstEntry,
) (*Manifest, error) {
	if current == nil {
		return nil, fmt.Errorf("build compacted manifest: nil manifest: %w", ErrInvalidArgument)
	}
	if plan == nil {
		return nil, fmt.Errorf("build compacted manifest: nil plan: %w", ErrInvalidArgument)
	}
	if len(plan.Inputs) == 0 {
		return nil, fmt.Errorf("build compacted manifest: empty inputs: %w", ErrInvalidArgument)
	}
	if len(outputEntries) == 0 {
		return nil, fmt.Errorf("build compacted manifest: empty output: %w", ErrInvalidArgument)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		return nil, fmt.Errorf("stat compacted output: %w", err)
	}

	inputIDs := make(map[uint64]struct{}, len(plan.Inputs))
	for _, input := range plan.Inputs {
		inputIDs[input.ID] = struct{}{}
	}

	minSeqNo := outputEntries[0].seqNo
	maxSeqNo := outputEntries[0].seqNo
	for _, entry := range outputEntries[1:] {
		if entry.seqNo < minSeqNo {
			minSeqNo = entry.seqNo
		}
		if entry.seqNo > maxSeqNo {
			maxSeqNo = entry.seqNo
		}
	}

	output := ManifestTable{
		ID:       plan.OutputID,
		File:     plan.OutputFile,
		MinKey:   string(outputEntries[0].key),
		MaxKey:   string(outputEntries[len(outputEntries)-1].key),
		MinSeqNo: minSeqNo,
		MaxSeqNo: maxSeqNo,
		FileSize: info.Size(),
	}

	// Novi output ide prvi, a sve tabele koje nisu input ostaju u postojećem redosledu.
	tables := make([]ManifestTable, 0, len(current.Tables)-len(plan.Inputs)+1)
	tables = append(tables, output)
	for _, table := range current.Tables {
		if _, isInput := inputIDs[table.ID]; isInput {
			continue
		}
		tables = append(tables, table)
	}

	return &Manifest{
		Version: current.Version,
		Epoch:   current.Epoch + 1,
		Tables:  tables,
	}, nil
}

// runCompactionOnce izvršava najviše jedan size-tiered compaction ciklus.
// Input fajlovi se brišu tek nakon što je novi manifest uspešno sačuvan.
func runCompactionOnce(cfg Config, current *Manifest, metrics *Metrics, logger *slog.Logger) (*Manifest, error) {
	start := time.Now()

	plan, err := newCompactionPlan(current, cfg)
	if err != nil {
		return nil, fmt.Errorf("plan compaction: %w", err)
	}
	if plan == nil {
		return current, nil
	}

	readers := make([]*SSTableReader, 0, len(plan.Inputs))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	// Input-i su newest-first kako bi merge zadržao ispravan tie-break za isti seqNo.
	for _, input := range plan.Inputs {
		reader, err := OpenSSTableReader(filepath.Join(cfg.DataDir, input.File))
		if err != nil {
			return nil, fmt.Errorf("open compaction input %s: %w", input.File, err)
		}
		readers = append(readers, reader)
	}

	outputPath := filepath.Join(cfg.DataDir, plan.OutputFile)

	compactedReader, err := compactReaders(outputPath, cfg, readers...)
	if err != nil {
		return nil, fmt.Errorf("write compacted sstable: %w", err)
	}
	defer func() { _ = compactedReader.Close() }()

	// Test hook: output SSTable postoji, ali novi manifest još nije objavljen.
	runCrashHook("afterSSTRename")

	// Čitamo output nazad da iz stvarnog fajla izračunamo metadata za novi manifest.
	outputEntries, err := compactedReader.AllEntries()
	if err != nil {
		return nil, fmt.Errorf("read back compacted sstable: %w", err)
	}

	nextManifest, err := manifestAfterCompaction(current, plan, outputPath, outputEntries)
	if err != nil {
		return nil, fmt.Errorf("build next manifest: %w", err)
	}

	if err := saveManifest(cfg, nextManifest); err != nil {
		return nil, fmt.Errorf("save next manifest: %w", err)
	}

	// Stari fajlovi su bezbedni za brisanje tek kada recovery vidi novi manifest.
	for _, input := range plan.Inputs {
		oldPath := filepath.Join(cfg.DataDir, input.File)
		if err := os.Remove(oldPath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove old sstable %s: %w", input.File, err)
		}
	}

	durMs := time.Since(start).Milliseconds()
	if metrics != nil {
		metrics.CompactionsTotal.Add(1)
		metrics.LastCompactDurationMs.Store(durMs)
	}
	if logger != nil {
		logger.Info("compact job",
			"inputs_count", len(plan.Inputs),
			"output_id", plan.OutputID,
			"duration_ms", durMs,
		)
	}

	return nextManifest, nil
}
