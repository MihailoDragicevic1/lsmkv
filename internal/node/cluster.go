package node

import "lsmkv/internal/ring"

const DefaultVirtualNodes = 32

// BuildClusterNodes pravi kompletan skup node-ova koji ulaze u hash ring.
// Prvi je lokalni node, a zatim slede peer-ovi iz konfiguracije.
func BuildClusterNodes(cfg Config) []ring.Node {
	nodes := make([]ring.Node, 0, 1+len(cfg.SeedNodes))
	nodes = append(nodes, ring.Node{
		ID:   cfg.NodeID,
		Addr: cfg.ListenAddr,
	})

	for _, peer := range cfg.SeedNodes {
		nodes = append(nodes, ring.Node{
			ID:   peer.NodeID,
			Addr: peer.ListenAddr,
		})
	}

	return nodes
}

// BuildRing pravi consistent-hash ring iz lokalnog node-a i svih konfigurisanih peer-ova.
func BuildRing(cfg Config, virtualNodes int) (*ring.Ring, error) {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	return ring.New(BuildClusterNodes(cfg), virtualNodes)
}
