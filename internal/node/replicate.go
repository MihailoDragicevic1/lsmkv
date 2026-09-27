package node

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// writeReplicaResult predstavlja rezultat upisa na jednu repliku.
type writeReplicaResult struct {
	err error
}

// replicatePut šalje isti Put svim node-ovima iz preference liste i vraća uspeh
// čim write quorum potvrda bude dostignut.
func (s *Server) replicatePut(ctx context.Context, key, value []byte) error {
	// Bez runtime-a server radi kao lokalni single-node store.
	if s.rt == nil || s.rt.Coordinator == nil {
		return s.store.Put(key, value)
	}

	prefs := s.rt.Coordinator.PreferenceList(key)
	if len(prefs) == 0 {
		return fmt.Errorf("empty preference list")
	}

	required := s.rt.WriteQuorum
	if required <= 0 {
		required = 1
	}

	// Bafer veličine preference liste garantuje da worker-i ne blokiraju pri slanju rezultata.
	results := make(chan writeReplicaResult, len(prefs))

	var wg sync.WaitGroup
	for _, replica := range prefs {
		replica := replica
		wg.Add(1)

		go func() {
			defer wg.Done()

			if replica.ID == s.rt.Config.NodeID {
				// Lokalna replika se upisuje direktno, bez gRPC poziva ka samom sebi.
				if err := s.store.Put(key, value); err != nil {
					results <- writeReplicaResult{err: err}
					return
				}
				results <- writeReplicaResult{}
				return
			}

			client, err := Dial(ctx, replica.Addr, DefaultDialTimeout, DefaultRPCTimeout)
			if err != nil {
				results <- writeReplicaResult{
					err: status.Errorf(codes.Unavailable, "dial replica %s: %v", replica.Addr, err),
				}
				return
			}
			defer func() { _ = client.Close() }()

			// Forwarded=true sprečava repliku da zahtev ponovo rutira ili replicira.
			if err := client.ForwardPut(ctx, key, value); err != nil {
				results <- writeReplicaResult{err: err}
				return
			}

			results <- writeReplicaResult{}
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
					return fmt.Errorf("write quorum not reached: %w", firstErr)
				}
				return fmt.Errorf("write quorum not reached: success=%d required=%d", success, required)
			}

			if res.err != nil {
				// Čuvamo prvu grešku kao koristan uzrok ako quorum na kraju ne uspe.
				if firstErr == nil {
					firstErr = res.err
				}
				continue
			}

			success++
			// Odgovor se vraća čim imamo dovoljno potvrda; ne čekamo sporije replike.
			if success >= required {
				return nil
			}
		}
	}
}
