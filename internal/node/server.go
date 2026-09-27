package node

import (
	"context"
	"errors"

	"lsmkv/internal/lsm"
	"lsmkv/internal/ring"
	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implementira KVService gRPC API nad lokalnim LSM store-om i cluster runtime-om.
type Server struct {
	lsmkvv1.UnimplementedKVServiceServer
	store *lsm.Store
	rt    *Runtime
}

// NewServer pravi gRPC handler koji koristi lokalni storage i informacije o cluster-u.
func NewServer(store *lsm.Store, rt *Runtime) *Server {
	return &Server{
		store: store,
		rt:    rt,
	}
}

// Put validira zahtev, po potrebi ga prosleđuje coordinator-u, a zatim pokreće
// lokalni upis ili write replication za spoljne zahteve.
func (s *Server) Put(ctx context.Context, req *lsmkvv1.PutRequest) (*lsmkvv1.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		forwarded, err := s.forwardPutIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return &lsmkvv1.PutResponse{}, nil
		}
	}

	if req.Forwarded {
		// Coordinator prima već rutiran zahtev i primenjuje lokalnu kopiju bez novog forwarding-a.
		if err := s.store.Put(req.Key, req.Value); err != nil {
			return nil, toGRPCError(err)
		}
		return &lsmkvv1.PutResponse{}, nil
	}

	// Spoljni zahtev na coordinator-u pokreće write quorum prema replica set-u.
	if err := s.replicatePut(ctx, req.Key, req.Value); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &lsmkvv1.PutResponse{}, nil
}

// Get validira zahtev, po potrebi ga prosleđuje coordinator-u, a zatim izvršava
// lokalni read ili read quorum za spoljne zahteve.
func (s *Server) Get(ctx context.Context, req *lsmkvv1.GetRequest) (*lsmkvv1.GetResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		resp, forwarded, err := s.forwardGetIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return resp, nil
		}
	}

	if req.Forwarded {
		// Interno prosleđen read služi kao pojedinačno čitanje sa coordinator noda.
		value, found, err := s.store.Get(req.Key)
		if err != nil {
			return nil, toGRPCError(err)
		}
		if !found {
			return nil, status.Error(codes.NotFound, "key not found")
		}

		return &lsmkvv1.GetResponse{Value: value}, nil
	}

	// Spoljni zahtev na coordinator-u prikuplja odgovore iz replica set-a.
	value, err := s.replicateGet(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &lsmkvv1.GetResponse{Value: value}, nil
}

// Delete validira zahtev, po potrebi ga prosleđuje coordinator-u, a zatim lokalno
// upisuje tombstone ili pokreće delete quorum za spoljne zahteve.
func (s *Server) Delete(ctx context.Context, req *lsmkvv1.DeleteRequest) (*lsmkvv1.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}

	if !req.Forwarded {
		forwarded, err := s.forwardDeleteIfNeeded(ctx, req)
		if err != nil {
			return nil, err
		}
		if forwarded {
			return &lsmkvv1.DeleteResponse{}, nil
		}
	}

	if req.Forwarded {
		// Lokalni LSM Delete čuva tombstone koji kasnije sakriva starije vrednosti.
		if err := s.store.Delete(req.Key); err != nil {
			return nil, toGRPCError(err)
		}
		return &lsmkvv1.DeleteResponse{}, nil
	}

	if err := s.replicateDelete(ctx, req.Key); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &lsmkvv1.DeleteResponse{}, nil
}

// forwardPutIfNeeded prosleđuje spoljašnji Put samo kada lokalni node nije coordinator.
func (s *Server) forwardPutIfNeeded(ctx context.Context, req *lsmkvv1.PutRequest) (bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return false, err
	}
	if !shouldForward {
		return false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	if err := client.ForwardPut(ctx, req.Key, req.Value); err != nil {
		return false, err
	}
	return true, nil
}

// forwardGetIfNeeded vraća odgovor sa coordinator noda kada lokalni node nije odgovoran za key.
func (s *Server) forwardGetIfNeeded(ctx context.Context, req *lsmkvv1.GetRequest) (*lsmkvv1.GetResponse, bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return nil, false, err
	}
	if !shouldForward {
		return nil, false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return nil, false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	value, err := client.ForwardGet(ctx, req.Key)
	if err != nil {
		return nil, false, err
	}
	return &lsmkvv1.GetResponse{Value: value}, true, nil
}

// forwardDeleteIfNeeded prosleđuje spoljašnji Delete samo kada lokalni node nije coordinator.
func (s *Server) forwardDeleteIfNeeded(ctx context.Context, req *lsmkvv1.DeleteRequest) (bool, error) {
	target, shouldForward, err := s.forwardTarget(req.Key)
	if err != nil {
		return false, err
	}
	if !shouldForward {
		return false, nil
	}

	client, err := Dial(ctx, target.Addr, DefaultDialTimeout, DefaultRPCTimeout)
	if err != nil {
		return false, status.Errorf(codes.Unavailable, "dial coordinator %s: %v", target.Addr, err)
	}
	defer func() { _ = client.Close() }()

	if err := client.ForwardDelete(ctx, req.Key); err != nil {
		return false, err
	}
	return true, nil
}

// forwardTarget vraća coordinator node i odluku da li zahtev treba poslati drugom node-u.
func (s *Server) forwardTarget(key []byte) (ring.Node, bool, error) {
	// Bez runtime-a server radi kao lokalni single-node handler.
	if s.rt == nil || s.rt.Coordinator == nil {
		return ring.Node{}, false, nil
	}

	target, ok := s.rt.Coordinator.CoordinatorNode(key)
	if !ok {
		return ring.Node{}, false, status.Error(codes.Internal, "no coordinator found")
	}

	if target.ID == s.rt.Config.NodeID {
		return target, false, nil
	}
	if target.Addr == "" {
		return ring.Node{}, false, status.Error(codes.Internal, "coordinator address is empty")
	}

	return target, true, nil
}

// toGRPCError prevodi interne LSM greške u gRPC status kodove koje client može tumačiti.
func toGRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, lsm.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, lsm.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, lsm.ErrTooManyImmutables), errors.Is(err, lsm.ErrWriteStall):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, lsm.ErrStoreClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, lsm.ErrCorruptionDetected):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, lsm.ErrNotImplemented):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, lsm.ErrIOFailure):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
