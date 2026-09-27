package node

import "testing"

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.NodeID = "node-1"
	cfg.ListenAddr = "127.0.0.1:18080"
	cfg.SeedNodes = []Peer{
		{NodeID: "node-2", ListenAddr: "127.0.0.1:18081"},
		{NodeID: "node-3", ListenAddr: "127.0.0.1:18082"},
	}
	cfg.ReplicationFactor = 3
	cfg.WriteQuorum = 2
	cfg.ReadQuorum = 2
	return cfg
}

func TestNewRuntime_BuildsDependencies(t *testing.T) {
	rt, err := NewRuntime(testConfig(), 16)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	if rt.Ring == nil {
		t.Fatalf("expected ring to be initialized")
	}
	if rt.Coordinator == nil {
		t.Fatalf("expected coordinator to be initialized")
	}
	if len(rt.ClusterNodes) != 3 {
		t.Fatalf("expected 3 cluster nodes, got %d", len(rt.ClusterNodes))
	}
	if rt.ReplicationFactor != 3 {
		t.Fatalf("expected replication factor 3, got %d", rt.ReplicationFactor)
	}
	if rt.WriteQuorum != 2 {
		t.Fatalf("expected write quorum 2, got %d", rt.WriteQuorum)
	}
	if rt.ReadQuorum != 2 {
		t.Fatalf("expected read quorum 2, got %d", rt.ReadQuorum)
	}
}

func TestNewRuntime_DefaultsInvalidVirtualNodes(t *testing.T) {
	rt, err := NewRuntime(testConfig(), 0)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	prefs := rt.Coordinator.PreferenceList([]byte("alpha"))
	if len(prefs) == 0 {
		t.Fatalf("expected non-empty preference list")
	}
}

func TestNewRuntime_RejectsInvalidConfig(t *testing.T) {
	cfg := testConfig()
	cfg.NodeID = ""

	if _, err := NewRuntime(cfg, 16); err == nil {
		t.Fatalf("expected invalid config error")
	}
}