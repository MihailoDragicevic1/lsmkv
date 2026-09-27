package node

import (
	"context"
	"fmt"
	"time"

	lsmkvv1 "lsmkv/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultDialTimeout = 3 * time.Second
	DefaultRPCTimeout  = 3 * time.Second
)

// Client predstavlja jednu gRPC vezu ka drugom node-u u cluster-u.
type Client struct {
	addr       string
	rpcTimeout time.Duration
	conn       *grpc.ClientConn
	client     lsmkvv1.KVServiceClient
}

// Dial otvara blokirajuću gRPC vezu do adrese u okviru zadatog timeout-a.
func Dial(ctx context.Context, addr string, dialTimeout time.Duration, rpcTimeout time.Duration) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr cannot be empty")
	}
	if dialTimeout <= 0 {
		dialTimeout = DefaultDialTimeout
	}
	if rpcTimeout <= 0 {
		rpcTimeout = DefaultRPCTimeout
	}

	// Odvojen context ograničava samo vreme uspostavljanja mrežne veze.
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		// Trenutni projekat koristi nešifrovanu lokalnu gRPC vezu bez TLS-a.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Dial vraća grešku ako se veza ne uspostavi pre isteka dial timeout-a.
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return &Client{
		addr:       addr,
		rpcTimeout: rpcTimeout,
		conn:       conn,
		client:     lsmkvv1.NewKVServiceClient(conn),
	}, nil
}

// Close zatvara gRPC konekciju. Bezbedno je pozvati ga nad nil ili već praznim client-om.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Put šalje spoljašnji Put zahtev. forwarded=false dozvoljava serveru da rutira zahtev.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.put(ctx, key, value, false)
}

// ForwardPut šalje Put koji je drugi node već prosledio odgovornom node-u.
func (c *Client) ForwardPut(ctx context.Context, key, value []byte) error {
	return c.put(ctx, key, value, true)
}

// put deli zajedničku RPC logiku i postavlja forwarded flag u protobuf zahtevu.
func (c *Client) put(ctx context.Context, key, value []byte, forwarded bool) error {
	// Svaki RPC dobija sopstveni deadline nezavisno od vremena potrebnog za Dial.
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	_, err := c.client.Put(callCtx, &lsmkvv1.PutRequest{
		Key:       key,
		Value:     value,
		Forwarded: forwarded,
	})
	return err
}

// Get šalje spoljašnji Get zahtev. Server po potrebi bira coordinator node.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, false)
}

// ForwardGet šalje interni Get zahtev koji ne treba ponovo prosleđivati.
func (c *Client) ForwardGet(ctx context.Context, key []byte) ([]byte, error) {
	return c.get(ctx, key, true)
}

// get šalje Get RPC i vraća value iz protobuf odgovora.
func (c *Client) get(ctx context.Context, key []byte, forwarded bool) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	resp, err := c.client.Get(callCtx, &lsmkvv1.GetRequest{
		Key:       key,
		Forwarded: forwarded,
	})
	if err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// Delete šalje spoljašnji delete zahtev. Server upisuje tombstone lokalno ili kroz quorum.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.delete(ctx, key, false)
}

// ForwardDelete šalje interni delete zahtev koji je već rutiran od drugog node-a.
func (c *Client) ForwardDelete(ctx context.Context, key []byte) error {
	return c.delete(ctx, key, true)
}

// delete deli zajedničku RPC logiku i postavlja forwarded flag u protobuf zahtevu.
func (c *Client) delete(ctx context.Context, key []byte, forwarded bool) error {
	callCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()

	_, err := c.client.Delete(callCtx, &lsmkvv1.DeleteRequest{
		Key:       key,
		Forwarded: forwarded,
	})
	return err
}
