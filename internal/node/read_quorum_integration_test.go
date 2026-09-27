package node_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"
)

/*
	Ovaj test proverava baš ono što nam sada treba:

- 3 noda rade zajedno,
- write prvo upisuje vrednost na replike,
- zatim Get kroz coordinator vraća ispravnu vrednost,i time potvrđujemo da novi read path radi u normalnom healthy cluster scenariju.
*/
func TestReadQuorum_ReadsAcrossThreeNodes(t *testing.T) {
	base1 := lsm.DefaultConfig()
	base1.DataDir = filepath.Join(t.TempDir(), "node-1-data")

	base2 := lsm.DefaultConfig()
	base2.DataDir = filepath.Join(t.TempDir(), "node-2-data")

	base3 := lsm.DefaultConfig()
	base3.DataDir = filepath.Join(t.TempDir(), "node-3-data")

	node1Cfg := node.Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:19281",
		SeedNodes: []node.Peer{
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19282"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19283"},
		},
		Storage:           base1,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node2Cfg := node.Config{
		NodeID:     "node-2",
		ListenAddr: "127.0.0.1:19282",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19281"},
			{NodeID: "node-3", ListenAddr: "127.0.0.1:19283"},
		},
		Storage:           base2,
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
	}

	node3Cfg := node.Config{
		NodeID:     "node-3",
		ListenAddr: "127.0.0.1:19283",
		SeedNodes: []node.Peer{
			{NodeID: "node-1", ListenAddr: "127.0.0.1:19281"},
			{NodeID: "node-2", ListenAddr: "127.0.0.1:19282"},
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
	value := []byte("read-quorum-value")

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

	got, err := clientToNode1.Get(ctx, key)
	if err != nil {
		t.Fatalf("get through node1: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected get value: got=%q want=%q", got, value)
	}
}
