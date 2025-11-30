package node

import (
	"testing"
	"time"
	"tapestry/internal/id"
)

func TestRoutingTableAdd(t *testing.T) {
	localID := id.NewRandomID()
	rt := NewRoutingTable(localID)

	localDigit := localID.GetDigit(0)
	neighborDigit := (localDigit + 1) % id.RADIX
	
	nbID := localID.SetDigit(0, neighborDigit)
	
	n1 := Neighbor{ID: nbID, Address: "1", Latency: 100 * time.Millisecond}
	
	if !rt.Add(n1) {
		t.Errorf("Failed to add valid neighbor")
	}

	list := rt.Get(0, neighborDigit)
	if len(list) != 1 {
		t.Errorf("Expected 1 neighbor, got %d", len(list))
	}

	n2 := Neighbor{ID: nbID, Address: "2", Latency: 50 * time.Millisecond}
	rt.Add(n2)

	list = rt.Get(0, neighborDigit)
	if len(list) != 1 {
		t.Errorf("Expected 1 neighbor (update), got %d", len(list))
	}
	if list[0].Latency != 50*time.Millisecond {
		t.Errorf("Expected latency update to 50ms")
	}

	nbID2 := nbID
	nbID2 = nbID2.SetDigit(1, (nbID.GetDigit(1)+1)%id.RADIX)
	
	n3 := Neighbor{ID: nbID2, Address: "3", Latency: 10 * time.Millisecond}
	rt.Add(n3)

	list = rt.Get(0, neighborDigit)
	if len(list) != 2 {
		t.Errorf("Expected 2 neighbors, got %d", len(list))
	}
	
	if list[0].ID != n3.ID {
		t.Errorf("Routing table not sorted by latency")
	}
}

func TestRoutingTableTrim(t *testing.T) {
	localID := id.NewRandomID()
	rt := NewRoutingTable(localID)
	
	digit := (localID.GetDigit(0) + 1) % id.RADIX

	for i := 0; i < 5; i++ {
		nbID := localID.SetDigit(0, digit)
		nbID = nbID.SetDigit(1, i) 
		
		lat := time.Duration(500 - i*100)
		
		rt.Add(Neighbor{ID: nbID, Address: "addr", Latency: lat})
	}

	list := rt.Get(0, digit)
	if len(list) > 3 {
		t.Errorf("Routing table failed to trim. Size: %d", len(list))
	}

	if list[0].Latency != 100 {
		t.Errorf("Expected best latency 100, got %v", list[0].Latency)
	}
}