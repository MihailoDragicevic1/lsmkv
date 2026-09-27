package lsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ManifestTable opisuje jedan SSTable koji trenutno pripada store-u.
// Metadata omogućava oporavak i planiranje compaction-a bez skeniranja svakog fajla.
type ManifestTable struct {
	ID       uint64 `json:"id"`
	File     string `json:"file"`
	MinKey   string `json:"min_key"`
	MaxKey   string `json:"max_key"`
	MinSeqNo uint64 `json:"min_seq_no"`
	MaxSeqNo uint64 `json:"max_seq_no"`
	FileSize int64  `json:"file_size"`
}

// Manifest je trajni spisak SSTable fajlova koje store smatra aktivnim.
// Tables je newest-first da read path prvo vidi novije verzije podataka.
type Manifest struct {
	Version int             `json:"version"`
	Epoch   uint64          `json:"epoch"`
	Tables  []ManifestTable `json:"tables"`
}

// manifestPath vraća lokaciju manifesta unutar data direktorijuma.
func manifestPath(cfg Config) string {
	return filepath.Join(cfg.DataDir, "manifest.json")
}

// loadManifest učitava i validira poslednji objavljeni skup SSTable metadata.
// Ako manifest ne postoji, store se tretira kao nova prazna baza.
func loadManifest(cfg Config) (*Manifest, error) {
	path := manifestPath(cfg)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{
				Version: 1,
				Epoch:   0,
				Tables:  nil,
			}, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", ErrCorruptionDetected)
	}

	// Version definiše format manifesta i sprečava pogrešno čitanje budućih formata.
	if m.Version == 0 {
		return nil, fmt.Errorf("manifest missing version: %w", ErrCorruptionDetected)
	}
	if m.Version != 1 {
		return nil, fmt.Errorf("manifest unsupported version %d: %w", m.Version, ErrCorruptionDetected)
	}

	seenIDs := make(map[uint64]struct{}, len(m.Tables))
	seenFiles := make(map[string]struct{}, len(m.Tables))

	for _, table := range m.Tables {
		// Svaka tabela mora imati jedinstven identitet i bezbedno lokalno ime fajla.
		if table.ID == 0 {
			return nil, fmt.Errorf("manifest table has invalid id 0: %w", ErrCorruptionDetected)
		}
		if table.File == "" {
			return nil, fmt.Errorf("manifest table has empty file: %w", ErrCorruptionDetected)
		}
		if table.MinKey > table.MaxKey {
			return nil, fmt.Errorf("manifest table has invalid key range: %w", ErrCorruptionDetected)
		}
		// Manifest čuva samo ime fajla, ne putanju van DataDir-a.
		if filepath.Base(table.File) != table.File {
			return nil, fmt.Errorf("manifest table file must be base name only: %w", ErrCorruptionDetected)
		}
		if table.MinSeqNo > table.MaxSeqNo {
			return nil, fmt.Errorf("manifest table has invalid seq range: %w", ErrCorruptionDetected)
		}
		if table.FileSize < 0 {
			return nil, fmt.Errorf("manifest table has negative file size: %w", ErrCorruptionDetected)
		}
		if _, ok := seenIDs[table.ID]; ok {
			return nil, fmt.Errorf("manifest has duplicate table id %d: %w", table.ID, ErrCorruptionDetected)
		}
		seenIDs[table.ID] = struct{}{}

		if _, ok := seenFiles[table.File]; ok {
			return nil, fmt.Errorf("manifest has duplicate table file %q: %w", table.File, ErrCorruptionDetected)
		}
		seenFiles[table.File] = struct{}{}
	}

	return &m, nil
}

// saveManifest upisuje novi manifest u privremeni fajl, pa ga rename-uje na finalnu putanju.
// Tako se ne objavljuje delimično upisan manifest ako proces padne tokom zapisa.
func saveManifest(cfg Config, m *Manifest) error {
	path := manifestPath(cfg)
	tmp := path + ".tmp"

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write manifest tmp: %w", err)
	}

	// Rename objavljuje celu novu verziju manifesta odjednom.
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	return nil
}

// withNewTable vraća novu verziju manifesta sa tabelom na početku niza.
// Original se ne menja, što olakšava bezbedno objavljivanje nove verzije store-a.
func (m *Manifest) withNewTable(t ManifestTable) *Manifest {
	tables := make([]ManifestTable, 0, len(m.Tables)+1)
	tables = append(tables, t)
	tables = append(tables, m.Tables...)

	return &Manifest{
		Version: m.Version,
		Epoch:   m.Epoch + 1,
		Tables:  tables,
	}
}

// nextTableID vraća sledeći slobodan ID na osnovu najvećeg ID-a u manifestu.
func (m *Manifest) nextTableID() uint64 {
	var max uint64
	for _, t := range m.Tables {
		if t.ID > max {
			max = t.ID
		}
	}
	return max + 1
}
