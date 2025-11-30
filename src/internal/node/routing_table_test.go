package node

import (
	"testing"
	"time"
	"tapestry/internal/id"
)

func TestRoutingTableAdd(t *testing.T) {
	localID := id.NewRandomID()
	rt := NewRoutingTable(localID)

	// Create a neighbor that shares 0 prefix digits (Level 0)
	// We force the first digit to be different
	localDigit := localID.GetDigit(0)
	neighborDigit := (localDigit + 1) % id.RADIX
	
	nbID := localID.SetDigit(0, neighborDigit)
	
	n1 := Neighbor{ID: nbID, Address: "1", Latency: 100 * time.Millisecond}
	
	// Add first neighbor
	if !rt.Add(n1) {
		t.Errorf("Failed to add valid neighbor")
	}

	// Verify it exists at the correct slot
	list := rt.Get(0, neighborDigit)
	if len(list) != 1 {
		t.Errorf("Expected 1 neighbor, got %d", len(list))
	}

	// Add a closer neighbor (Latency 50ms)
	n2 := Neighbor{ID: nbID, Address: "2", Latency: 50 * time.Millisecond}
	rt.Add(n2)

	list = rt.Get(0, neighborDigit)
	if len(list) != 1 {
		// Since IDs are identical, it should update, not append (duplicates check)
		// Wait, in our implementation, duplicates are checked by ID equality.
		// Since n1 and n2 have SAME ID, n2 should update n1.
		t.Errorf("Expected 1 neighbor (update), got %d", len(list))
	}
	if list[0].Latency != 50*time.Millisecond {
		t.Errorf("Expected latency update to 50ms")
	}

	// Add distinct neighbor ID but same slot
	// To do this, we need same digit at level 0, but different ID elsewhere
	nbID2 := nbID
	nbID2 = nbID2.SetDigit(1, (nbID.GetDigit(1)+1)%id.RADIX)
	
	n3 := Neighbor{ID: nbID2, Address: "3", Latency: 10 * time.Millisecond}
	rt.Add(n3)

	list = rt.Get(0, neighborDigit)
	if len(list) != 2 {
		t.Errorf("Expected 2 neighbors, got %d", len(list))
	}
	
	// Verify sorting: n3 (10ms) should be before n2 (50ms)
	if list[0].ID != n3.ID {
		t.Errorf("Routing table not sorted by latency")
	}
}

func TestRoutingTableTrim(t *testing.T) {
	localID := id.NewRandomID()
	rt := NewRoutingTable(localID)
	
	digit := (localID.GetDigit(0) + 1) % id.RADIX

	// Add 5 neighbors to the same slot
	// Capacity is 1 + K_BACKUPS (2) = 3
	for i := 0; i < 5; i++ {
		// Make IDs distinct
		nbID := localID.SetDigit(0, digit)
		nbID = nbID.SetDigit(1, i) 
		
		// Latency decreases with i (400, 300, 200, 100, 0)
		lat := time.Duration(500 - i*100)
		
		rt.Add(Neighbor{ID: nbID, Address: "addr", Latency: lat})
	}

	list := rt.Get(0, digit)
	if len(list) > 3 {
		t.Errorf("Routing table failed to trim. Size: %d", len(list))
	}

	// The ones kept should be the lowest latency ones (0, 100, 200)
	if list[0].Latency != 100 {
		// Wait, 500-(4*100) = 100. 
		// i=0 -> 500
		// i=1 -> 400
		// i=2 -> 300
		// i=3 -> 200
		// i=4 -> 100
		// Sorted: 100, 200, 300.
		t.Errorf("Expected best latency 100, got %v", list[0].Latency)
	}
}