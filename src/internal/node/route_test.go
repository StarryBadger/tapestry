package node

import (
	"testing"
	"tapestry/internal/id"
)

func TestComputeNextHop_SurrogateFix(t *testing.T) {
	// Setup: 
	// Local Node: Ends in ...0 (Level 0 digit = 0)
	// Routing Table: Empty (Sparse network)
	// Target: ...5 (Level 0 digit = 5)
	
	// If logic is correct, it should scan 5, 6... 15, 0.
	// It finds 0 (itself) and returns (Self, true).
	
	localID := id.ZeroID // All zeros
	n := &Node{
		ID: localID,
		Address: "local",
		Table: NewRoutingTable(localID),
	}

	target := id.ZeroID.SetDigit(0, 5) // Digit 5

	nextHop, isRoot := n.computeNextHop(target)

	if !isRoot {
		t.Errorf("Node should have declared itself root")
	}
	if !nextHop.ID.Equals(localID) {
		t.Errorf("Node should have returned self")
	}
}

func TestComputeNextHop_FindsNeighbor(t *testing.T) {
	localID := id.ZeroID
	n := &Node{
		ID: localID,
		Address: "local",
		Table: NewRoutingTable(localID),
	}

	// Add a neighbor at digit 5
	nbID := id.ZeroID.SetDigit(0, 5)
	neighbor := Neighbor{ID: nbID, Address: "remote"}
	n.Table.Add(neighbor)

	// Target digit 5
	target := id.ZeroID.SetDigit(0, 5)

	nextHop, isRoot := n.computeNextHop(target)

	if isRoot {
		t.Errorf("Should not be root if neighbor exists")
	}
	if !nextHop.ID.Equals(nbID) {
		t.Errorf("Should have routed to neighbor")
	}
}