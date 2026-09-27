package ring

import (
	"reflect"
	"testing"
)

func testNodes() []Node {
	return []Node{
		{ID: "node-a", Addr: "127.0.0.1:18081"},
		{ID: "node-b", Addr: "127.0.0.1:18082"},
		{ID: "node-c", Addr: "127.0.0.1:18083"},
	}
}

func TestNewRing_RejectsInvalidInput(t *testing.T) {
	_, err := New(nil, 3)
	if err == nil {
		t.Fatalf("expected error for empty nodes")
	}

	_, err = New(testNodes(), 0)
	if err == nil {
		t.Fatalf("expected error for virtualNodes <= 0")
	}

	_, err = New([]Node{
		{ID: "node-a", Addr: "127.0.0.1:1"},
		{ID: "node-a", Addr: "127.0.0.1:2"},
	}, 3)
	if err == nil {
		t.Fatalf("expected error for duplicate node IDs")
	}
}

func TestCoordinator_IsDeterministic(t *testing.T) {
	r, err := New(testNodes(), 16)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	key := []byte("alpha")

	n1, ok := r.Coordinator(key)
	if !ok {
		t.Fatalf("expected coordinator")
	}
	n2, ok := r.Coordinator(key)
	if !ok {
		t.Fatalf("expected coordinator on second lookup")
	}

	if !reflect.DeepEqual(n1, n2) {
		t.Fatalf("coordinator not deterministic: first=%v second=%v", n1, n2)
	}
}

func TestPreferenceList_ReturnsDistinctPhysicalNodes(t *testing.T) {
	r, err := New(testNodes(), 32)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	prefs := r.PreferenceList([]byte("alpha"), 3)
	if len(prefs) != 3 {
		t.Fatalf("expected 3 preference nodes, got %d", len(prefs))
	}

	seen := make(map[string]struct{}, len(prefs))
	for _, n := range prefs {
		if _, ok := seen[n.ID]; ok {
			t.Fatalf("duplicate physical node in preference list: %s", n.ID)
		}
		seen[n.ID] = struct{}{}
	}
}

func TestPreferenceList_CapsAtDistinctNodeCount(t *testing.T) {
	r, err := New(testNodes(), 8)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	prefs := r.PreferenceList([]byte("alpha"), 10)
	if len(prefs) != 3 {
		t.Fatalf("expected preference list capped at 3 nodes, got %d", len(prefs))
	}
}

func TestPreferenceList_FirstNodeMatchesCoordinator(t *testing.T) {
	r, err := New(testNodes(), 32)
	if err != nil {
		t.Fatalf("new ring: %v", err)
	}

	key := []byte("beta")

	coord, ok := r.Coordinator(key)
	if !ok {
		t.Fatalf("expected coordinator")
	}

	prefs := r.PreferenceList(key, 3)
	if len(prefs) == 0 {
		t.Fatalf("expected non-empty preference list")
	}

	if !reflect.DeepEqual(coord, prefs[0]) {
		t.Fatalf("expected coordinator to equal first preference node, got coord=%v prefs[0]=%v", coord, prefs[0])
	}
}
