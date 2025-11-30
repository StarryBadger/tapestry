package node

import (
	"context"
	"log"
	"sync"
	"time"

	pb "tapestry/api/proto"
)

// Leave initiates a graceful exit from the network.
func (n *Node) Leave() error {
	log.Printf("Node %s initiating graceful exit...", n.ID)

	// 1. Redistribute Data
	// We MUST wait for this to finish before shutting down.
	n.redistributeData()

	// 2. Notify Backpointers
	n.bpLock.RLock()
	var backpointers []Neighbor
	for _, bp := range n.Backpointers {
		backpointers = append(backpointers, bp)
	}
	n.bpLock.RUnlock()

	var wg sync.WaitGroup
	for _, bp := range backpointers {
		wg.Add(1)
		go func(target Neighbor) {
			defer wg.Done()
			client, err := GetClient(target.Address)
			if err == nil {
				defer client.Close()
				client.NotifyLeave(context.Background(), n.toProtoNeighbor())
			}
		}(bp)
	}
	// Wait for notifications with a short timeout so we don't hang forever if a neighbor is down
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		log.Println("[LEAVE] Backpointer notification timed out")
	}

	// 3. Signal Shutdown
	n.Stop()
	
	// Signal main.go to exit the process
	close(n.ExitChan)
	
	return nil
}

// redistributeData pushes local objects to neighbors before dying.
func (n *Node) redistributeData() {
	n.objLock.RLock()
	defer n.objLock.RUnlock()

	if len(n.LocalObjects) == 0 {
		return
	}

	log.Printf("[LEAVE] Redistributing %d objects to neighbors...", len(n.LocalObjects))

	candidates := n.SelectRandomNeighbors(3)
	if len(candidates) == 0 {
		log.Println("[LEAVE] No neighbors found! Data will be lost.")
		return
	}

	var wg sync.WaitGroup
	i := 0
	for _, obj := range n.LocalObjects {
		target := candidates[i%len(candidates)]
		i++

		wg.Add(1)
		go func(t Neighbor, k, d string) {
			defer wg.Done()
			client, err := GetClient(t.Address)
			if err != nil {
				log.Printf("[LEAVE] Failed to handoff object to %s", t.Address)
				return
			}
			defer client.Close()

			_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{
				Key:  k,
				Data: d,
			})
			if err == nil {
				log.Printf("[LEAVE] Handed off '%s' to %s", k, t.Address)
			} else {
				log.Printf("[LEAVE] Error replicating to %s: %v", t.Address, err)
			}
		}(target, obj.Key, obj.Data)
	}
	
	// Wait for all replications to complete or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("[LEAVE] Redistribution complete.")
	case <-time.After(5 * time.Second):
		log.Println("[LEAVE] Redistribution timed out. Some data may be lost.")
	}
}

// NotifyLeave is the RPC handler. 
func (n *Node) NotifyLeave(ctx context.Context, req *pb.Neighbor) (*pb.Nothing, error) {
	leavingNode, err := NeighborFromProto(req)
	if err != nil { return nil, err }

	log.Printf("[LEAVE] Notification: Node %s is leaving. Removing from table.", leavingNode.ID)
	
	n.Table.Remove(leavingNode.ID)
	
	n.bpLock.Lock()
	delete(n.Backpointers, leavingNode.ID.String())
	n.bpLock.Unlock()

	return &pb.Nothing{}, nil
}

func (n *Node) toProtoNeighbor() *pb.Neighbor {
	return &pb.Neighbor{
		Id:      &pb.NodeID{Bytes: n.ID.Bytes()},
		Address: n.Address,
	}
}