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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func startTestServer(t *testing.T) (lsmkvv1.KVServiceClient, func()) {
	t.Helper()

	cfg := lsm.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")

	store, err := lsm.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = store.Close()
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	rt, err := node.NewRuntime(node.Config{
		NodeID:            "node-1",
		ListenAddr:        lis.Addr().String(),
		SeedNodes:         nil,
		Storage:           lsm.DefaultConfig(),
		ReplicationFactor: 1,
		WriteQuorum:       1,
		ReadQuorum:        1,
	}, 16)

	if err != nil {
		grpcServer.Stop()
		_ = lis.Close()
		_ = store.Close()
		t.Fatalf("new runtime: %v", err)
	}
	lsmkvv1.RegisterKVServiceServer(grpcServer, node.NewServer(store, rt))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcServer.Stop()
		_ = store.Close()
		t.Fatalf("dial server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = store.Close()
	}

	return lsmkvv1.NewKVServiceClient(conn), cleanup
}

func TestKVServer_PutGetDelete(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := []byte("name")
	value := []byte("matija")

	if _, err := client.Put(ctx, &lsmkvv1.PutRequest{
		Key:   key,
		Value: value,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	getResp, err := client.Get(ctx, &lsmkvv1.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("get after put: %v", err)
	}
	if string(getResp.Value) != string(value) {
		t.Fatalf("unexpected value: got=%q want=%q", getResp.Value, value)
	}

	if _, err := client.Delete(ctx, &lsmkvv1.DeleteRequest{
		Key: key,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = client.Get(ctx, &lsmkvv1.GetRequest{
		Key: key,
	})
	if err == nil {
		t.Fatalf("expected not found after delete")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("unexpected status code: got=%v want=%v", st.Code(), codes.NotFound)
	}
}

func TestKVServer_RejectsEmptyKey(t *testing.T) {
	client, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Put(ctx, &lsmkvv1.PutRequest{
		Key:   []byte{},
		Value: []byte("x"),
	})
	if err == nil {
		t.Fatalf("expected invalid argument for empty key")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("unexpected status code: got=%v want=%v", st.Code(), codes.InvalidArgument)
	}
}
