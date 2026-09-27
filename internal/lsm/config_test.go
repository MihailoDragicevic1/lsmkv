package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig should be valid, got error: %v", err)
	}
}

func TestConfigRejectsEmptyDataDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = ""

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected empty DataDir to be rejected")
	}
}

func TestConfigRejectsNonPositiveMemtableMaxBytes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MemtableMaxBytes = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-positive MemtableMaxBytes to be rejected")
	}
}

func TestConfigRejectsZeroMaxImmutableTables(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxImmutableTables = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected zero MaxImmutableTables to be rejected")
	}
}

func TestConfigRejectsNonPositiveBlockSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BlockSize = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-positive BlockSize to be rejected")
	}
}

func TestConfigRejectsInvalidBloomFalsePositive(t *testing.T) {
	tests := []float64{0, -0.1, 1, 1.5}

	for _, v := range tests {
		t.Run("bloom", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.BloomFalsePositive = v

			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected BloomFalsePositive=%v to be rejected", v)
			}
		})
	}
}

func TestConfigRejectsNonPositiveWALFsyncEveryN(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALFsyncEveryN = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected non-positive WALFsyncEveryN to be rejected")
	}
}

func TestConfigRejectsUnsupportedCompression(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Compression = "gzip"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unsupported compression to be rejected")
	}
}

func TestConfigRejectsUnsupportedLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogLevel = "trace"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected unsupported log level to be rejected")
	}
}

func TestLoadConfigMissingFileReturnsDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig should return defaults for missing file, got error: %v", err)
	}

	def := DefaultConfig()
	if cfg != def {
		t.Fatalf("expected defaults %+v, got %+v", def, cfg)
	}
}

func TestLoadConfigEmptyFileReturnsDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	if err := os.WriteFile(path, []byte("   \n\t  "), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig should return defaults for empty file, got error: %v", err)
	}

	def := DefaultConfig()
	if cfg != def {
		t.Fatalf("expected defaults %+v, got %+v", def, cfg)
	}
}

func TestLoadConfigParsesJSONOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	data := []byte(`{
		"data_dir": "./custom-data",
		"memtable_max_bytes": 12345,
		"max_immutable_tables": 7,
		"block_size": 4096,
		"bloom_false_positive": 0.05,
		"wal_fsync_every_n": 3,
		"compression": "snappy",
		"log_level": "debug"
	}`)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.DataDir != "./custom-data" {
		t.Fatalf("expected DataDir ./custom-data, got %q", cfg.DataDir)
	}
	if cfg.MemtableMaxBytes != 12345 {
		t.Fatalf("expected MemtableMaxBytes 12345, got %d", cfg.MemtableMaxBytes)
	}
	if cfg.MaxImmutableTables != 7 {
		t.Fatalf("expected MaxImmutableTables 7, got %d", cfg.MaxImmutableTables)
	}
	if cfg.BlockSize != 4096 {
		t.Fatalf("expected BlockSize 4096, got %d", cfg.BlockSize)
	}
	if cfg.BloomFalsePositive != 0.05 {
		t.Fatalf("expected BloomFalsePositive 0.05, got %v", cfg.BloomFalsePositive)
	}
	if cfg.WALFsyncEveryN != 3 {
		t.Fatalf("expected WALFsyncEveryN 3, got %d", cfg.WALFsyncEveryN)
	}
	if cfg.Compression != "snappy" {
		t.Fatalf("expected Compression snappy, got %q", cfg.Compression)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("expected LogLevel debug, got %q", cfg.LogLevel)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	data := []byte(`{
		"data_dir": "./data",
		"unknown_field": 123
	}`)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected unknown JSON field to be rejected")
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	data := []byte(`{
		"data_dir": "./data",
		"memtable_max_bytes": 0
	}`)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected invalid config values to be rejected")
	}
}

func TestConfigRejectsNonPositiveWALSegmentRollBytes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WALSegmentRollBytes = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for zero wal_segment_roll_bytes")
	}
}
