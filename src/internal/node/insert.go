package node

import (
	"context"
	"fmt"
	"log"
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// Join connects the node to the Tapestry network via a bootstrap node.
func (n *Node) Join(bootstrapAddr string) error {
	log.Printf("Node %s joining via %s...", n.ID, bootstrapAddr)

	// 1. Connect to Bootstrap Node
	bsClient, err := GetClient(bootstrapAddr)
	if err != nil {
		return fmt.Errorf("failed to contact bootstrap: %v", err)
	}
	defer bsClient.Close()

	// 2. Find Surrogate Root for My ID
	req := &pb.RouteRequest{
		TargetId: &pb.NodeID{Bytes: n.ID.Bytes()},
		SourceId: &pb.NodeID{Bytes: n.ID.Bytes()},
	}
	
	resp, err := bsClient.GetNextHop(context.Background(), req)
	if err != nil {
		return fmt.Errorf("bootstrap route failed: %v", err)
	}

	var surrogateNeighbor Neighbor
	if resp.NextHop != nil {
		surrogateNeighbor, _ = NeighborFromProto(resp.NextHop)
	}

	// Use bootstrap as fallback if surrogate is missing (shouldn't happen)
	if surrogateNeighbor.Address == "" {
		log.Println("Surrogate lookup returned empty, using bootstrap as surrogate.")
		// We don't have the ID easily here unless we ask, but let's assume the bootstrap logic holds.
		// In a real fix, we'd ping bootstrap for ID. For now, rely on surrogate.
		return fmt.Errorf("surrogate lookup failed")
	}

	log.Printf("Found Surrogate Root: %s (%s)", surrogateNeighbor.ID, surrogateNeighbor.Address)

	// CRITICAL FIX: Add the surrogate to our routing table immediately!
	// This ensures we have at least one link into the network.
	added := n.Table.Add(surrogateNeighbor)
	if added {
		log.Printf("Added Surrogate %s to routing table.", surrogateNeighbor.ID)
	}

	// 3. Copy Routing Table from Surrogate
	copyClient, err := GetClient(surrogateNeighbor.Address)
	if err == nil {
		defer copyClient.Close()
		rtResp, err := copyClient.GetRoutingTable(context.Background(), &pb.Nothing{})
		if err == nil {
			n.populateTable(rtResp)
		} else {
			log.Printf("Failed to copy table from surrogate: %v", err)
		}
	}

	// 4. Notify Neighbors (Backpointer / Optimization)
	// Since we added the surrogate to our table in Step 2, this will now notify the surrogate.
	// The surrogate will then add US to THEIR table.
	n.notifyNeighbors()

	return nil
}

func (n *Node) populateTable(resp *pb.RTCopyResponse) {
	n.Table.lock.Lock()
	defer n.Table.lock.Unlock()

	idx := 0
	count := 0
	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			if idx < len(resp.Entries) {
				entry := resp.Entries[idx]
				for _, nbProto := range entry.Neighbors {
					nb, _ := NeighborFromProto(nbProto)
					// Only add if slot is empty (don't overwrite what we might have found)
					// and don't add ourselves.
					if len(n.Table.rows[i][j]) == 0 && !nb.ID.Equals(n.ID) {
						n.Table.rows[i][j] = append(n.Table.rows[i][j], nb)
						count++
					}
				}
				idx++
			}
		}
	}
	log.Printf("Bootstrap: Copied %d entries from surrogate.", count)
}

func (n *Node) notifyNeighbors() {
	var neighbors []Neighbor
	
	// Snapshot the table to avoid holding lock during network calls
	n.Table.lock.RLock()
	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			neighbors = append(neighbors, n.Table.rows[i][j]...)
		}
	}
	n.Table.lock.RUnlock()

	log.Printf("Notifying %d neighbors of existence...", len(neighbors))

	for _, nb := range neighbors {
		go func(target Neighbor) {
			client, err := GetClient(target.Address)
			if err == nil {
				defer client.Close()
				level := id.SharedPrefixLength(target.ID, n.ID)
				
				req := &pb.BackpointerRequest{
					From:  &pb.Neighbor{Id: &pb.NodeID{Bytes: n.ID.Bytes()}, Address: n.Address},
					Level: int32(level),
				}
				_, err := client.AddBackpointer(context.Background(), req)
				if err != nil {
					log.Printf("Failed to notify %s: %v", target.Address, err)
				}
			}
		}(nb)
	}
}