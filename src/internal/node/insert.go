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

	bsClient, err := GetClient(bootstrapAddr)
	if err != nil {
		return fmt.Errorf("failed to contact bootstrap: %v", err)
	}
	defer bsClient.Close()

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

	if surrogateNeighbor.Address == "" {
		log.Println("Surrogate lookup returned empty, using bootstrap as surrogate.")
		return fmt.Errorf("surrogate lookup failed")
	}

	log.Printf("Found Surrogate Root: %s (%s)", surrogateNeighbor.ID, surrogateNeighbor.Address)

	// Add surrogate (Use AddNeighborSafe to measure latency)
	n.AddNeighborSafe(surrogateNeighbor)

	// Copy Routing Table from Surrogate
	copyClient, err := GetClient(surrogateNeighbor.Address)
	if err == nil {
		defer copyClient.Close()
		rtResp, err := copyClient.GetRoutingTable(context.Background(), &pb.Nothing{})
		if err == nil {
			n.populateTable(rtResp)
		}
	}

	n.notifyNeighbors()

	return nil
}

func (n *Node) populateTable(resp *pb.RTCopyResponse) {
	// Don't lock here; AddNeighborSafe handles its own locking.
	// We iterate the response and aggressively try to add everyone.
	
	count := 0
	for _, entry := range resp.Entries {
		for _, nbProto := range entry.Neighbors {
			nb, _ := NeighborFromProto(nbProto)
			
			// optimization: don't add ourselves
			if nb.ID.Equals(n.ID) {
				continue
			}

			// Aggressively add. The Table logic handles sorting and trimming.
			// We run this in goroutines to speed up the join process (parallel pings)
			go func(neighbor Neighbor) {
				if n.AddNeighborSafe(neighbor) {
					// We can't safely increment a counter here without atomic/lock, 
					// but for logging we can just ignore or use atomic.
				}
			}(nb)
			count++
		}
	}
	log.Printf("Bootstrap: Processing %d candidates from surrogate table...", count)
}

func (n *Node) notifyNeighbors() {
	var neighbors []Neighbor
	
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
				client.AddBackpointer(context.Background(), req)
			}
		}(nb)
	}
}