package ring

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// Node predstavlja jedan fizički node u cluster-u.
type Node struct {
	ID   string
	Addr string
}

// token je jedna virtualna pozicija fizičkog node-a na hash ring-u.
type token struct {
	value uint64
	node  Node
}

// Ring čuva sortirane virtualne tokene za deterministički izbor coordinator-a i replika.
type Ring struct {
	tokens       []token
	virtualNodes int
}

// New pravi consistent-hash ring za sve node-ove u cluster-u.
func New(nodes []Node, virtualNodes int) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, fmt.Errorf("virtualNodes must be > 0")
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("at least one node is required")
	}

	seen := make(map[string]struct{}, len(nodes))
	tokens := make([]token, 0, len(nodes)*virtualNodes)

	for _, node := range nodes {
		if strings.TrimSpace(node.ID) == "" {
			return nil, fmt.Errorf("node ID cannot be empty")
		}
		if strings.TrimSpace(node.Addr) == "" {
			return nil, fmt.Errorf("node Addr cannot be empty")
		}
		if _, ok := seen[node.ID]; ok {
			return nil, fmt.Errorf("duplicate node ID: %s", node.ID)
		}
		seen[node.ID] = struct{}{}

		// Svaki fizički node dobija više tokena radi ravnomernije raspodele ključeva.
		for i := 0; i < virtualNodes; i++ {
			tokenKey := fmt.Sprintf("%s#%d", node.ID, i)
			tokens = append(tokens, token{
				value: hashBytes([]byte(tokenKey)),
				node:  node,
			})
		}
	}

	// Sortiran ring omogućava binary search pri pronalaženju prvog tokena posle key hash-a.
	sort.Slice(tokens, func(i, j int) bool {
		if tokens[i].value == tokens[j].value {
			return tokens[i].node.ID < tokens[j].node.ID
		}
		return tokens[i].value < tokens[j].value
	})

	return &Ring{
		tokens:       tokens,
		virtualNodes: virtualNodes,
	}, nil
}

// Coordinator vraća prvi token u smeru kazaljke na satu od hash-a datog ključa.
// Ako ključ padne iza poslednjeg tokena, pretraga se vraća na početak ring-a.
func (r *Ring) Coordinator(key []byte) (Node, bool) {
	if r == nil || len(r.tokens) == 0 {
		return Node{}, false
	}

	pos := hashBytes(key)
	idx := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].value >= pos
	})
	if idx == len(r.tokens) {
		idx = 0
	}

	return r.tokens[idx].node, true
}

// PreferenceList vraća do n različitih node-ova počevši od coordinator-a.
// Virtualni tokeni istog fizičkog node-a se preskaču da replike budu na različitim node-ovima.
func (r *Ring) PreferenceList(key []byte, n int) []Node {
	if r == nil || len(r.tokens) == 0 || n <= 0 {
		return nil
	}

	// Ne možemo vratiti više replika nego što postoji fizičkih node-ova.
	if n > r.distinctNodeCount() {
		n = r.distinctNodeCount()
	}

	pos := hashBytes(key)
	start := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i].value >= pos
	})
	if start == len(r.tokens) {
		start = 0
	}

	out := make([]Node, 0, n)
	seen := make(map[string]struct{}, n)

	// Kružimo kroz tokene dok ne sakupimo traženi broj različitih fizičkih node-ova.
	for step := 0; step < len(r.tokens) && len(out) < n; step++ {
		idx := (start + step) % len(r.tokens)
		node := r.tokens[idx].node
		if _, ok := seen[node.ID]; ok {
			continue
		}
		seen[node.ID] = struct{}{}
		out = append(out, node)
	}

	return out
}

// distinctNodeCount vraća broj fizičkih node-ova, nezavisno od broja virtualnih tokena.
func (r *Ring) distinctNodeCount() int {
	if r == nil {
		return 0
	}

	seen := make(map[string]struct{})
	for _, tok := range r.tokens {
		seen[tok.node.ID] = struct{}{}
	}
	return len(seen)
}

// hashBytes mapira ključ ili token identitet na 64-bitnu poziciju na ring-u.
func hashBytes(b []byte) uint64 {
	sum := sha1.Sum(b)
	return binary.BigEndian.Uint64(sum[:8])
}
