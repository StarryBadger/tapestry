package node

import (
	"testing"
	"tapestry/internal/id"
)

func TestComputeNextHop_SurrogateFix(t *testing.T) {
	
	localID := id.ZeroID
	n := &Node{
		ID: localID,
		Address: "local",
		Table: NewRoutingTable(localID),
	}

	target := id.ZeroID.SetDigit(0, 5) 

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

	nbID := id.ZeroID.SetDigit(0, 5)
	neighbor := Neighbor{ID: nbID, Address: "remote"}
	n.Table.Add(neighbor)

	target := id.ZeroID.SetDigit(0, 5)

	nextHop, isRoot := n.computeNextHop(target)

	if isRoot {
		t.Errorf("Should not be root if neighbor exists")
	}
	if !nextHop.ID.Equals(nbID) {
		t.Errorf("Should have routed to neighbor")
	}
}