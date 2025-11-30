package node

import (
	"context"
	"fmt"
	"log"
	"time" 
	"sync"
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

	// FIX: Retry adding surrogate to ensure we are connected
	added := false
	for i := 0; i < 3; i++ {
		if n.AddNeighborSafe(surrogateNeighbor) {
			added = true
			log.Printf("Added Surrogate %s to routing table.", surrogateNeighbor.ID)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	
	if !added {
		log.Printf("[WARNING] Failed to bond with Surrogate %s. Node might be isolated!", surrogateNeighbor.Address)
	}

	// Copy Routing Table
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
	// Use a semaphore to limit concurrent bonding attempts
	sem := make(chan struct{}, 5) // Max 5 concurrent dials
	var wg sync.WaitGroup

	count := 0
	for _, entry := range resp.Entries {
		for _, nbProto := range entry.Neighbors {
			nb, _ := NeighborFromProto(nbProto)
			if nb.ID.Equals(n.ID) { continue }

			wg.Add(1)
			go func(neighbor Neighbor) {
				defer wg.Done()
				sem <- struct{}{} // Acquire
				n.AddNeighborSafe(neighbor)
				<-sem // Release
			}(nb)
			count++
		}
	}
	
	// Wait for all attempts to finish so we don't spam logs immediately after
	go func() {
		wg.Wait()
		log.Printf("Bootstrap: Processed %d candidates from surrogate table.", count)
	}()
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