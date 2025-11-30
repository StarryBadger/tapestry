package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
	"sync"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// Join connects the node to the Tapestry network via a list of potential bootstrap nodes.
func (n *Node) Join(bootstrapAddrs []string) error {
	// 1. Shuffle the list to spread load and avoid thundering herd on the first node
	rand.Shuffle(len(bootstrapAddrs), func(i, j int) {
		bootstrapAddrs[i], bootstrapAddrs[j] = bootstrapAddrs[j], bootstrapAddrs[i]
	})

	var bsClient *TapestryClient
	var err error
	var connectedAddr string

	// 2. Iterate until we find a live gateway
	for _, addr := range bootstrapAddrs {
		if addr == n.Address { continue } // Don't join via self

		log.Printf("Attempting to join via %s...", addr)
		bsClient, err = GetClient(addr)
		if err == nil {
			// Check if it's actually responsive
			_, pingErr := bsClient.Ping(context.Background(), &pb.Nothing{})
			if pingErr == nil {
				connectedAddr = addr
				break // Success!
			}
			bsClient.Close()
		}
	}

	if connectedAddr == "" {
		return fmt.Errorf("failed to connect to any bootstrap node in list: %v", bootstrapAddrs)
	}
	defer bsClient.Close()

	log.Printf("Successfully bonded with Gateway: %s", connectedAddr)

	// 3. Find Surrogate Root for My ID
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

	// Retry adding surrogate (Bonding)
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