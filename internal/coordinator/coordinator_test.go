package coordinator

import (
	"reflect"
	"testing"

	"lsmkv/internal/ring"
)

func testRing(t *testing.T) *ring.Ring {
	t.Helper()

	r, err := ring.New([]ring.Node{
		{ID: "node-1", Addr: "127.0.0.1:18080"},
		{ID: "node-2", Addr: "127.0.0.1:18081"},
		{ID: "node-3", Addr: "127.0.0.1:18082"},
	}, 32)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}
	return r
}

func TestNew_RejectsInvalidInput(t *testing.T) {
	r := testRing(t)

	if _, err := New("", r, 3); err == nil {
		t.Fatalf("expected error for empty local node id")
	}
	if _, err := New("node-1", nil, 3); err == nil {
		t.Fatalf("expected error for nil ring")
	}
}

func TestCoordinatorNode_MatchesPreferenceListHead(t *testing.T) {
	r := testRing(t)

	c, err := New("node-1", r, 3)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	key := []byte("alpha")

	coord, ok := c.CoordinatorNode(key)
	if !ok {
		t.Fatalf("expected coordinator node")
	}

	prefs := c.PreferenceList(key)
	if len(prefs) == 0 {
		t.Fatalf("expected non-empty preference list")
	}

	if !reflect.DeepEqual(coord, prefs[0]) {
		t.Fatalf("coordinator mismatch: coord=%v prefs[0]=%v", coord, prefs[0])
	}
}

func TestPreferenceList_UsesReplicationFactor(t *testing.T) {
	r := testRing(t)

	c, err := New("node-1", r, 2)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	prefs := c.PreferenceList([]byte("beta"))
	if len(prefs) != 2 {
		t.Fatalf("expected 2 replicas in preference list, got %d", len(prefs))
	}
}

func TestIsLocalReplica_AgreesWithLocalIndex(t *testing.T) {
	r := testRing(t)

	c, err := New("node-2", r, 2)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	key := []byte("gamma")

	idx, ok := c.LocalIndex(key)
	isReplica := c.IsLocalReplica(key)

	if ok != isReplica {
		t.Fatalf("LocalIndex and IsLocalReplica disagree: ok=%v isReplica=%v idx=%d", ok, isReplica, idx)
	}

	if ok && idx < 0 {
		t.Fatalf("expected non-negative index when local node is a replica")
	}
}

func TestLocalIndex_ReturnsFalseWhenLocalNodeNotInTopN(t *testing.T) {
	r := testRing(t)

	c, err := New("node-3", r, 1)
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	foundAnyMiss := false
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
		[]byte("epsilon"),
	}

	for _, key := range keys {
		if _, ok := c.LocalIndex(key); !ok {
			foundAnyMiss = true
			break
		}
	}

	if !foundAnyMiss {
		t.Fatalf("expected at least one key where local node is not in top-1 preference list")
	}
}
