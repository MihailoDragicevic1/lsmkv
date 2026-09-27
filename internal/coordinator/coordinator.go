package coordinator

import (
	"fmt"

	"lsmkv/internal/ring"
)

const DefaultReplicationFactor = 3

// Coordinator povezuje lokalni node sa hash ring-om i računa ko je odgovoran
// za dati ključ, kao i koji node-ovi čine njegov replica set.
type Coordinator struct {
	localNodeID string
	replicas    int
	ring        *ring.Ring
}

// New pravi coordinator za lokalni node i podešava broj željenih replika.
func New(localNodeID string, r *ring.Ring, replicas int) (*Coordinator, error) {
	if localNodeID == "" {
		return nil, fmt.Errorf("localNodeID cannot be empty")
	}
	if r == nil {
		return nil, fmt.Errorf("ring cannot be nil")
	}
	if replicas <= 0 {
		replicas = DefaultReplicationFactor
	}

	return &Coordinator{
		localNodeID: localNodeID,
		replicas:    replicas,
		ring:        r,
	}, nil
}

// CoordinatorNode vraća primarni node za ključ, odnosno prvi node iz preference liste.
func (c *Coordinator) CoordinatorNode(key []byte) (ring.Node, bool) {
	if c == nil || c.ring == nil {
		return ring.Node{}, false
	}
	return c.ring.Coordinator(key)
}

// PreferenceList vraća node-ove koji treba da čuvaju replike ključa.
// Prvi node je coordinator, a sledeći su različiti node-ovi u smeru ring-a.
func (c *Coordinator) PreferenceList(key []byte) []ring.Node {
	if c == nil || c.ring == nil {
		return nil
	}
	return c.ring.PreferenceList(key, c.replicas)
}

// IsLocalReplica govori da li lokalni node pripada replica set-u za ključ.
func (c *Coordinator) IsLocalReplica(key []byte) bool {
	_, ok := c.LocalIndex(key)
	return ok
}

// LocalIndex vraća poziciju lokalnog node-a u preference listi.
// Indeks 0 znači da je lokalni node coordinator za dati ključ.
func (c *Coordinator) LocalIndex(key []byte) (int, bool) {
	if c == nil || c.ring == nil {
		return -1, false
	}

	prefs := c.PreferenceList(key)
	for i, n := range prefs {
		if n.ID == c.localNodeID {
			return i, true
		}
	}
	return -1, false
}
