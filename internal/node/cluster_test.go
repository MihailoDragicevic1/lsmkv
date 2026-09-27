package node

import (
	"reflect"
	"testing"

	"lsmkv/internal/ring"
)

func TestBuildClusterNodes_IncludesSelfAndSeeds(t *testing.T) {
	cfg := Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:18080",
		SeedNodes: []Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:18081"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:18082"},
		},
	}

	got := BuildClusterNodes(cfg)
	want := []ring.Node{
		{ID: "node-1", Addr: "127.0.0.1:18080"},
		{ID: "node-2", Addr: "127.0.0.1:18081"},
		{ID: "node-3", Addr: "127.0.0.1:18082"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected cluster nodes: got=%v want=%v", got, want)
	}
}

func TestBuildRing_UsesClusterNodes(t *testing.T) {
	cfg := Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:18080",
		SeedNodes: []Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:18081"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:18082"},
		},
	}

	r, err := BuildRing(cfg, 16)
	if err != nil {
		t.Fatalf("build ring: %v", err)
	}

	prefs := r.PreferenceList([]byte("alpha"), 3)
	if len(prefs) != 3 {
		t.Fatalf("expected 3 preference nodes, got %d", len(prefs))
	}
}
