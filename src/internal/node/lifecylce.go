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

	// 1. Notify Backpointers (The nodes that point to ME)
	// We want them to remove me from their Routing Tables immediately.
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
				// Tell them I am leaving
				client.NotifyLeave(context.Background(), n.toProtoNeighbor())
			}
		}(bp)
	}

	// 2. Stop Services
	// Give a moment for messages to send
	time.Sleep(500 * time.Millisecond)
	n.Stop()
	
	return nil
}

// NotifyLeave is the RPC handler. 
// When Node A calls this on Node B, Node B removes Node A from its routing table.
func (n *Node) NotifyLeave(ctx context.Context, req *pb.Neighbor) (*pb.Nothing, error) {
	leavingNode, err := NeighborFromProto(req)
	if err != nil { return nil, err }

	log.Printf("[LEAVE] Notification: Node %s is leaving. Removing from table.", leavingNode.ID)
	
	// Remove from Routing Table
	n.Table.Remove(leavingNode.ID)
	
	// Remove from Backpointers
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