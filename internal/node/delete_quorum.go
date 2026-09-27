package node

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type deleteReplicaResult struct {
	err error
}

func (s *Server) replicateDelete(ctx context.Context, key []byte) error {
	if s.rt == nil || s.rt.Coordinator == nil {
		return s.store.Delete(key)
	}

	prefs := s.rt.Coordinator.PreferenceList(key)
	if len(prefs) == 0 {
		return fmt.Errorf("empty preference list")
	}

	required := s.rt.WriteQuorum
	if required <= 0 {
		required = 1
	}

	results := make(chan deleteReplicaResult, len(prefs))

	var wg sync.WaitGroup
	for _, replica := range prefs {
		replica := replica
		wg.Add(1)

		go func() {
			defer wg.Done()

			if replica.ID == s.rt.Config.NodeID {
				if err := s.store.Delete(key); err != nil {
					results <- deleteReplicaResult{err: err}
					return
				}
				results <- deleteReplicaResult{}
				return
			}

			client, err := Dial(ctx, replica.Addr, DefaultDialTimeout, DefaultRPCTimeout)
			if err != nil {
				results <- deleteReplicaResult{
					err: status.Errorf(codes.Unavailable, "dial replica %s: %v", replica.Addr, err),
				}
				return
			}
			defer func() { _ = client.Close() }()

			if err := client.ForwardDelete(ctx, key); err != nil {
				results <- deleteReplicaResult{err: err}
				return
			}

			results <- deleteReplicaResult{}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		success  int
		firstErr error
	)

	for {
		select {
		case <-ctx.Done():
			return status.Error(codes.DeadlineExceeded, ctx.Err().Error())

		case res, ok := <-results:
			if !ok {
				if success >= required {
					return nil
				}
				if firstErr != nil {
					return fmt.Errorf("delete quorum not reached: %w", firstErr)
				}
				return fmt.Errorf("delete quorum not reached: success=%d required=%d", success, required)
			}

			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				continue
			}

			success++
			if success >= required {
				return nil
			}
		}
	}
}
