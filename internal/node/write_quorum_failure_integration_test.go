package node_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"
)

func TestWriteQuorum_SucceedsWhenOneReplicaIsDown(t *testing.T) {
	base1 := lsm.DefaultConfig()
	base1.DataDir = filepath.Join(t.TempDir(), "node-1-data")

	base2 := lsm.DefaultConfig()
	base2.DataDir = filepath.Join(t.TempDir(), "node-2-data")

	base3 := lsm.DefaultConfig()
	base3.DataDir = filepath.Join(t.TempDir(), "node-3-data")

	node1Cfg := node.Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:19581",
		SeedNodes: []node.Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19582"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19583"},
		},
		Storage:           base1,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node2Cfg := node.Config{
		NodeID:     "node-2",
		ListenAddr: "127.0.0.1:19582",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19581"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19583"},
		},
		Storage:           base2,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node3Cfg := node.Config{
		NodeID:     "node-3",
		ListenAddr: "127.0.0.1:19583",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19581"},
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19582"},
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

	key := mustFindKeyForReplicaSet(t, node1Cfg, []string{"node-1", "node-2", "node-3"})
	value := []byte("write-with-one-replica-down")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	clientToNode1, err := node.Dial(ctx, n1.addr, time.Second, time.Second)
	if err != nil {
		t.Fatalf("dial node1: %v", err)
	}
	defer func() { _ = clientToNode1.Close() }()

	n3.stop()

	if err := clientToNode1.Put(ctx, key, value); err != nil {
		t.Fatalf("put with node-3 down: %v", err)
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
}
