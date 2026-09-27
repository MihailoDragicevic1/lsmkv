package lsm

import (
	"errors"
	"os"
	"testing"
)

func TestLoadManifestMissingReturnsEmpty(t *testing.T) {
	cfg := testConfig(t)

	m, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if m.Version != 1 {
		t.Fatalf("Version = %d, want 1", m.Version)
	}
	if m.Epoch != 0 {
		t.Fatalf("Epoch = %d, want 0", m.Epoch)
	}
	if len(m.Tables) != 0 {
		t.Fatalf("Tables len = %d, want 0", len(m.Tables))
	}
}

func TestSaveAndLoadManifestRoundTrip(t *testing.T) {
	cfg := testConfig(t)

	want := &Manifest{
		Version: 1,
		Epoch:   3,
		Tables: []ManifestTable{
			{
				ID:       1,
				File:     "000001.sst",
				MinKey:   "apple",
				MaxKey:   "banana",
				MinSeqNo: 1,
				MaxSeqNo: 5,
				FileSize: 123,
			},
		},
	}

	if err := saveManifest(cfg, want); err != nil {
		t.Fatalf("saveManifest: %v", err)
	}

	got, err := loadManifest(cfg)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}

	if got.Version != want.Version || got.Epoch != want.Epoch || len(got.Tables) != 1 {
		t.Fatalf("loaded manifest mismatch: got %+v want %+v", got, want)
	}
	if got.Tables[0].File != want.Tables[0].File {
		t.Fatalf("table file = %q, want %q", got.Tables[0].File, want.Tables[0].File)
	}
}

func TestLoadManifestCorruptedReturnsError(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	if err := os.WriteFile(path, []byte(`{"version":1,"epoch":`), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected corruption error")
	}
}
func TestLoadManifestRejectsUnsupportedVersion(t *testing.T) { //dodato: testovi za izmenu u manifest.go(metoda loadManifest)
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 2,
        "epoch": 1,
        "tables": []
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected unsupported version error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestLoadManifestRejectsDuplicateTableID(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "000001.sst", "min_key": "a", "max_key": "b", "min_seq_no": 1, "max_seq_no": 2, "file_size": 10},
            {"id": 1, "file": "000002.sst", "min_key": "c", "max_key": "d", "min_seq_no": 3, "max_seq_no": 4, "file_size": 20}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected duplicate table id error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestLoadManifestRejectsDuplicateTableFile(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "000001.sst", "min_key": "a", "max_key": "b", "min_seq_no": 1, "max_seq_no": 2, "file_size": 10},
            {"id": 2, "file": "000001.sst", "min_key": "c", "max_key": "d", "min_seq_no": 3, "max_seq_no": 4, "file_size": 20}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected duplicate table file error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestLoadManifestRejectsInvalidSeqRange(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "000001.sst", "min_key": "a", "max_key": "b", "min_seq_no": 5, "max_seq_no": 4, "file_size": 10}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected invalid seq range error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestLoadManifestRejectsEmptyFile(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "", "min_key": "a", "max_key": "b", "min_seq_no": 1, "max_seq_no": 4, "file_size": 10}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected empty file error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}

func TestLoadManifestRejectsPathTraversalFile(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "../000001.sst", "min_key": "a", "max_key": "b", "min_seq_no": 1, "max_seq_no": 4, "file_size": 10}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected path traversal file error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}
func TestLoadManifestRejectsInvalidKeyRange(t *testing.T) {
	cfg := testConfig(t)

	path := manifestPath(cfg)
	data := []byte(`{
        "version": 1,
        "epoch": 1,
        "tables": [
            {"id": 1, "file": "000001.sst", "min_key": "z", "max_key": "a", "min_seq_no": 1, "max_seq_no": 4, "file_size": 10}
        ]
    }`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadManifest(cfg)
	if err == nil {
		t.Fatal("expected invalid key range error")
	}
	if !errors.Is(err, ErrCorruptionDetected) {
		t.Fatalf("expected ErrCorruptionDetected, got %v", err)
	}
}
