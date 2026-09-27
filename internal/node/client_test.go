package node_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"lsmkv/internal/lsm"
	"lsmkv/internal/node"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClient_PutGetDelete(t *testing.T) {
	cfg := lsm.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data-client")

	store, err := lsm.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	rt, err := node.NewRuntime(node.Config{
		NodeID:            "node-1",
		ListenAddr:        "127.0.0.1:0",
		SeedNodes:         nil,
		Storage:           lsm.DefaultConfig(),
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}, 16)

	srv := node.NewServer(store, rt)
	addr, stop := startStandaloneGRPCServer(t, srv)
	defer stop()

	ctx := context.Background()

	c, err := node.Dial(ctx, addr, time.Second, time.Second)
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	defer func() { _ = c.Close() }()

	key := []byte("name")
	value := []byte("matija")

	if err := c.Put(ctx, key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("unexpected value: got=%q want=%q", got, value)
	}

	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = c.Get(ctx, key)
	if err == nil {
		t.Fatalf("expected not found after delete")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("unexpected code: got=%v want=%v", st.Code(), codes.NotFound)
	}
}

func TestClient_RejectsEmptyAddr(t *testing.T) {
	_, err := node.Dial(context.Background(), "", time.Second, time.Second)
	if err == nil {
		t.Fatalf("expected error for empty addr")
	}
}
