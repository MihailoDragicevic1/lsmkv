package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"lsmkv/internal/lsm"
)

// Peer opisuje drugi node u cluster-u i adresu na kojoj njegov gRPC server sluša.
type Peer struct {
	NodeID     string `json:"node_id"`
	ListenAddr string `json:"listen_addr"`
}

// Config sadrži identitet lokalnog node-a, poznate peer-ove, lokalni storage
// i pravila za replikaciju i quorum operacije.
type Config struct {
	NodeID            string     `json:"node_id"`
	ListenAddr        string     `json:"listen_addr"`
	SeedNodes         []Peer     `json:"seed_nodes"`
	Storage           lsm.Config `json:"storage"`
	ReplicationFactor int        `json:"replication_factor"`
	WriteQuorum       int        `json:"write_quorum"`
	ReadQuorum        int        `json:"read_quorum"`
}

// DefaultConfig vraća podrazumevana podešavanja za lokalni node.
func DefaultConfig() Config {
	return Config{
		NodeID:            "node-1",
		ListenAddr:        "127.0.0.1:18080",
		SeedNodes:         nil,
		Storage:           lsm.DefaultConfig(),
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}
}

// LoadConfig učitava JSON preko default vrednosti, pa zatim validira konačnu konfiguraciju.
// Prazna ili nepostojeća putanja vraća validnu podrazumevanu konfiguraciju.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()

	if path == "" {
		if err := cfg.Validate(); err != nil {
			return Config{}, err
		}
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := cfg.Validate(); err != nil {
				return Config{}, err
			}
			return cfg, nil
		}
		return Config{}, fmt.Errorf("read node config: %w", err)
	}

	if len(bytes.TrimSpace(data)) == 0 {
		if err := cfg.Validate(); err != nil {
			return Config{}, err
		}
		return cfg, nil
	}

	// Nepoznata JSON polja odbijamo da typo ne ostane neprimećen u produkcionoj konfiguraciji.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse node config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate proverava da lokalni node, peer-ovi, quorum vrednosti i storage konfiguracija
// imaju smisla pre pokretanja servera.
func (c Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", lsm.ErrInvalidArgument)
	}
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("%w: listen_addr cannot be empty", lsm.ErrInvalidArgument)
	}

	// Lokalni node je već zauzet, pa peer ne sme imati isti ID ili istu adresu.
	seenIDs := map[string]struct{}{c.NodeID: {}}
	seenAddrs := map[string]struct{}{c.ListenAddr: {}}

	for i, peer := range c.SeedNodes {
		if strings.TrimSpace(peer.NodeID) == "" {
			return fmt.Errorf("%w: seed_nodes[%d].node_id cannot be empty", lsm.ErrInvalidArgument, i)
		}
		if strings.TrimSpace(peer.ListenAddr) == "" {
			return fmt.Errorf("%w: seed_nodes[%d].listen_addr cannot be empty", lsm.ErrInvalidArgument, i)
		}
		if _, exists := seenIDs[peer.NodeID]; exists {
			return fmt.Errorf("%w: duplicate node_id %q", lsm.ErrInvalidArgument, peer.NodeID)
		}
		if _, exists := seenAddrs[peer.ListenAddr]; exists {
			return fmt.Errorf("%w: duplicate listen_addr %q", lsm.ErrInvalidArgument, peer.ListenAddr)
		}
		seenIDs[peer.NodeID] = struct{}{}
		seenAddrs[peer.ListenAddr] = struct{}{}
	}

	if c.ReplicationFactor <= 0 {
		return fmt.Errorf("replication_factor must be > 0")
	}
	if c.WriteQuorum <= 0 || c.WriteQuorum > c.ReplicationFactor {
		return fmt.Errorf("write_quorum must be in range 1..replication_factor")
	}
	if c.ReadQuorum <= 0 || c.ReadQuorum > c.ReplicationFactor {
		return fmt.Errorf("read_quorum must be in range 1..replication_factor")
	}

	// Storage podešavanja se validiraju odvojeno u lsm paketu.
	if err := c.Storage.Validate(); err != nil {
		return err
	}
	return nil
}
