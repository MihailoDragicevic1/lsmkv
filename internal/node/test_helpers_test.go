package node_test

import (
	"net"
	"testing"

	"lsmkv/internal/node"
	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc"
)

func startStandaloneGRPCServer(t *testing.T, srv *node.Server) (string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	lsmkvv1.RegisterKVServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return lis.Addr().String(), cleanup
}