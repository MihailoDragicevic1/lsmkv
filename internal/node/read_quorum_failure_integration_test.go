package node_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"
)

func TestReadQuorum_SucceedsWhenOneReplicaIsDown(t *testing.T) {
	base1 := lsm.DefaultConfig()
	base1.DataDir = filepath.Join(t.TempDir(), "node-1-data")

	base2 := lsm.DefaultConfig()
	base2.DataDir = filepath.Join(t.TempDir(), "node-2-data")

	base3 := lsm.DefaultConfig()
	base3.DataDir = filepath.Join(t.TempDir(), "node-3-data")

	node1Cfg := node.Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:19381",
		SeedNodes: []node.Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19382"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19383"},
		},
		Storage:           base1,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node2Cfg := node.Config{
		NodeID:     "node-2",
		ListenAddr: "127.0.0.1:19382",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19381"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19383"},
		},
		Storage:           base2,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node3Cfg := node.Config{
		NodeID:     "node-3",
		ListenAddr: "127.0.0.1:19383",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19381"},
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19382"},
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
	value := []byte("value-survives-one-replica-down")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	clientToNode1, err := node.Dial(ctx, n1.addr, time.Second, time.Second)
	if err != nil {
		t.Fatalf("dial node1: %v", err)
	}
	defer func() { _ = clientToNode1.Close() }()

	if err := clientToNode1.Put(ctx, key, value); err != nil {
		t.Fatalf("put through node1: %v", err)
	}

	n3.stop()

	got, err := clientToNode1.Get(ctx, key)
	if err != nil {
		t.Fatalf("get with node-3 down: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected get value: got=%q want=%q", got, value)
	}
}
