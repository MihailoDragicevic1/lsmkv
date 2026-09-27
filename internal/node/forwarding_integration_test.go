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

// test: dokaz da jedan node može da primi zahtev i da ga prosledi pravom coordinator-u na drugom node-u.
type runningNode struct {
	name  string
	addr  string
	store *lsm.Store
	stop  func()
}

func startConfiguredNode(t *testing.T, cfg node.Config) *runningNode {
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

	return &runningNode{
		name:  cfg.NodeID,
		addr:  lis.Addr().String(),
		store: store,
		stop:  stop,
	}
}

func mustFindKeyForCoordinator(t *testing.T, cfg node.Config, wantNodeID string) []byte {
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
	}

	for _, key := range candidates {
		n, ok := rt.Coordinator.CoordinatorNode(key)
		if ok && n.ID == wantNodeID {
			return key
		}
	}

	t.Fatalf("could not find key for coordinator %s", wantNodeID)
	return nil
}

func TestForwarding_PutAndGetReachCoordinator(t *testing.T) {
	baseStorage1 := lsm.DefaultConfig()
	baseStorage1.DataDir = filepath.Join(t.TempDir(), "node-1-data")

	baseStorage2 := lsm.DefaultConfig()
	baseStorage2.DataDir = filepath.Join(t.TempDir(), "node-2-data")

	node1Cfg := node.Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:19081",
		SeedNodes: []node.Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19082"},
		},
		Storage:           baseStorage1,
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	node2Cfg := node.Config{
		NodeID:     "node-2",
		ListenAddr: "127.0.0.1:19082",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19081"},
		},
		Storage:           baseStorage2,
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}

	n1 := startConfiguredNode(t, node1Cfg)
	defer n1.stop()

	n2 := startConfiguredNode(t, node2Cfg)
	defer n2.stop()

	key := mustFindKeyForCoordinator(t, node1Cfg, "node-2")
	value := []byte("forwarded-value")

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

	clientToNode2, err := node.Dial(ctx, n2.addr, time.Second, time.Second)
	if err != nil {
		t.Fatalf("dial node2: %v", err)
	}
	defer func() { _ = clientToNode2.Close() }()

	got, err := clientToNode2.Get(ctx, key)
	if err != nil {
		t.Fatalf("get directly from node2: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value on node2: got=%q want=%q", got, value)
	}

	_, foundOnNode1, err := n1.store.Get(key)
	if err != nil {
		t.Fatalf("direct local read from node1 store: %v", err)
	}
	if foundOnNode1 {
		t.Fatalf("expected key to be forwarded to coordinator only, but found it on node1 local store")
	}
}
