package lsm

import "sort"

// mergeEntries spaja zapise iz više SSTable-ova u jedan sortiran skup.
// Za isti ključ zadržava entry sa najvećim seqNo, uključujući tombstone zapise.
func mergeEntries(tables ...[]sstEntry) []sstEntry {
	newestByKey := make(map[string]sstEntry)

	for _, entries := range tables {
		for _, entry := range entries {
			key := string(entry.key)

			current, exists := newestByKey[key]
			if !exists || entry.seqNo > current.seqNo {
				// Kopije odvajaju rezultat compaction-a od bafera iz input tabela.
				newestByKey[key] = sstEntry{
					key:       append([]byte(nil), entry.key...),
					value:     append([]byte(nil), entry.value...),
					seqNo:     entry.seqNo,
					tombstone: entry.tombstone,
				}
			}
		}
	}

	merged := make([]sstEntry, 0, len(newestByKey))
	for _, entry := range newestByKey {
		merged = append(merged, entry)
	}

	// SSTableWriter očekuje ključeve u rastućem leksikografskom redosledu.
	sort.Slice(merged, func(i, j int) bool {
		return string(merged[i].key) < string(merged[j].key)
	})

	return merged
}
