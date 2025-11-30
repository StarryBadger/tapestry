package node

import (
	"context"
	"log"
	"time"

	pb "tapestry/api/proto"
)

// Leave initiates a graceful exit from the network.
func (n *Node) Leave() error {
	log.Printf("Node %s initiating graceful exit...", n.ID)

	// 1. Redistribute Data (Fix for Issue #1)
	// Hand off local objects to neighbors so they aren't lost.
	n.redistributeData()

	// 2. Notify Backpointers
	n.bpLock.RLock()
	var backpointers []Neighbor
	for _, bp := range n.Backpointers {
		backpointers = append(backpointers, bp)
	}
	n.bpLock.RUnlock()

	for _, bp := range backpointers {
		go func(target Neighbor) {
			client, err := GetClient(target.Address)
			if err == nil {
				defer client.Close()
				client.NotifyLeave(context.Background(), n.toProtoNeighbor())
			}
		}(bp)
	}

	// 3. Signal Shutdown
	time.Sleep(500 * time.Millisecond) // Allow RPCs to flush
	n.Stop()
	
	// FIX for Issue #2: Signal main.go to exit the process
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

	// Get a list of candidates (Backups)
	// We try to find 1 neighbor per object to take over.
	candidates := n.SelectRandomNeighbors(3)
	if len(candidates) == 0 {
		log.Println("[LEAVE] No neighbors found! Data will be lost.")
		return
	}

	// Round-robin distribution
	i := 0
	for _, obj := range n.LocalObjects {
		target := candidates[i%len(candidates)]
		i++

		go func(t Neighbor, k, d string) {
			client, err := GetClient(t.Address)
			if err != nil {
				log.Printf("[LEAVE] Failed to handoff object to %s", t.Address)
				return
			}
			defer client.Close()

			// We use Replicate, which stores and re-publishes
			_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{
				Key:  k,
				Data: d,
			})
			if err == nil {
				log.Printf("[LEAVE] Handed off '%s' to %s", k, t.Address)
			}
		}(target, obj.Key, obj.Data)
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