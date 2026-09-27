package node

import (
	"fmt"

	"lsmkv/internal/coordinator"
	"lsmkv/internal/ring"
)

// Runtime čuva jednom izgrađen pogled na cluster koji server koristi za rutiranje
// zahteva, izbor replika i proveru quorum podešavanja.
type Runtime struct {
	Config            Config
	ClusterNodes      []ring.Node
	Ring              *ring.Ring
	Coordinator       *coordinator.Coordinator
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
}

// NewRuntime validira konfiguraciju i pravi sve distribuirane zavisnosti jednog node-a.
func NewRuntime(cfg Config, virtualNodes int) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Virtualni node-ovi daju ravnomerniju raspodelu ključeva po hash ring-u.
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	clusterNodes := BuildClusterNodes(cfg)

	r, err := BuildRing(cfg, virtualNodes)
	if err != nil {
		return nil, fmt.Errorf("build ring: %w", err)
	}

	// Coordinator zna identitet lokalnog node-a i bira replike za dati ključ.
	c, err := coordinator.New(cfg.NodeID, r, cfg.ReplicationFactor)
	if err != nil {
		return nil, fmt.Errorf("build coordinator: %w", err)
	}

	return &Runtime{
		Config:            cfg,
		ClusterNodes:      clusterNodes,
		Ring:              r,
		Coordinator:       c,
		ReplicationFactor: cfg.ReplicationFactor,
		WriteQuorum:       cfg.WriteQuorum,
		ReadQuorum:        cfg.ReadQuorum,
	}, nil
}
