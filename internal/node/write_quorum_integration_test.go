package node_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"
	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc"
)

// jasan dokaz da replication putanja stvarno radi na 3 noda.
type quorumRunningNode struct {
	name  string
	addr  string
	store *lsm.Store
	stop  func()
}

func startQuorumNode(t *testing.T, cfg node.Config) *quorumRunningNode {
	t.Helper()

	store, err := lsm.Open(cfg.Storage)
	if err != nil {
		t.Fatalf("open store for %s: %v", cfg.NodeID, err)
	}

	rt, err := node.NewRuntime(cfg, 16)
	if err != nil {
		_ = store.Close()
		t.Fatalf("new runtime for %s: %v", cfg.NodeID, err)
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		_ = store.Close()
		t.Fatalf("listen for %s on %s: %v", cfg.NodeID, cfg.ListenAddr, err)
	}

	grpcServer := grpc.NewServer()
	lsmkvv1.RegisterKVServiceServer(grpcServer, node.NewServer(store, rt))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	stop := func() {
		grpcServer.Stop()
		_ = lis.Close()
		_ = store.Close()
	}

	return &quorumRunningNode{
		name:  cfg.NodeID,
		addr:  lis.Addr().String(),
		store: store,
		stop:  stop,
	}
}

func mustFindKeyForReplicaSet(t *testing.T, cfg node.Config, want []string) []byte {
	t.Helper()

	rt, err := node.NewRuntime(cfg, 16)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	candidates := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
		[]byte("epsilon"),
		[]byte("zeta"),
		[]byte("eta"),
		[]byte("theta"),
		[]byte("iota"),
		[]byte("kappa"),
		[]byte("lambda"),
		[]byte("mu"),
		[]byte("nu"),
		[]byte("xi"),
		[]byte("omicron"),
		[]byte("pi"),
		[]byte("rho"),
		[]byte("sigma"),
		[]byte("tau"),
		[]byte("upsilon"),
	}

	for _, key := range candidates {
		prefs := rt.Coordinator.PreferenceList(key)
		if len(prefs) != len(want) {
			continue
		}

		match := true
		for i := range want {
			if prefs[i].ID != want[i] {
				match = false
				break
			}
		}
		if match {
			return key
		}
	}

	t.Fatalf("could not find key for replica set %v", want)
	return nil
}

func TestWriteQuorum_ReplicatesAcrossThreeNodes(t *testing.T) {
	base1 := lsm.DefaultConfig()
	base1.DataDir = filepath.Join(t.TempDir(), "node-1-data")

	base2 := lsm.DefaultConfig()
	base2.DataDir = filepath.Join(t.TempDir(), "node-2-data")

	base3 := lsm.DefaultConfig()
	base3.DataDir = filepath.Join(t.TempDir(), "node-3-data")

	node1Cfg := node.Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:19181",
		SeedNodes: []node.Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19182"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19183"},
		},
		Storage:           base1,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node2Cfg := node.Config{
		NodeID:     "node-2",
		ListenAddr: "127.0.0.1:19182",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19181"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19183"},
		},
		Storage:           base2,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node3Cfg := node.Config{
		NodeID:     "node-3",
		ListenAddr: "127.0.0.1:19183",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19181"},
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19182"},
		},
		Storage:           base3,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	n1 := startQuorumNode(t, node1Cfg)
	defer n1.stop()

	n2 := startQuorumNode(t, node2Cfg)
	defer n2.stop()

	n3 := startQuorumNode(t, node3Cfg)
	defer n3.stop()

	key := mustFindKeyForReplicaSet(t, node1Cfg, []string{"node-1", "node-2", "node-3"})
	value := []byte("quorum-value")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientToNode1, err := node.Dial(ctx, n1.addr, time.Second, time.Second)
	if err != nil {
		t.Fatalf("dial node1: %v", err)
	}
	defer func() { _ = clientToNode1.Close() }()

	if err := clientToNode1.Put(ctx, key, value); err != nil {
		t.Fatalf("put through node1: %v", err)
	}

	checkStoredValue := func(t *testing.T, rn *quorumRunningNode) {
		t.Helper()

		got, found, err := rn.store.Get(key)
		if err != nil {
			t.Fatalf("direct local read from %s: %v", rn.name, err)
		}
		if !found {
			t.Fatalf("expected key on %s but it was not found", rn.name)
		}
		if string(got) != string(value) {
			t.Fatalf("unexpected value on %s: got=%q want=%q", rn.name, got, value)
		}
	}

	checkStoredValue(t, n1)
	checkStoredValue(t, n2)
	checkStoredValue(t, n3)
}
