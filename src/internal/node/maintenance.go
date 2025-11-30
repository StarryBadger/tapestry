package node

import (
	"context"
	"log"
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// GetRoutingTable returns the current node's routing table (flattened).
func (n *Node) GetRoutingTable(ctx context.Context, req *pb.Nothing) (*pb.RTCopyResponse, error) {
	n.Table.lock.RLock()
	defer n.Table.lock.RUnlock()

	var entries []*pb.RoutingTableEntry

	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			pbNeighbors := []*pb.Neighbor{}
			for _, neighbor := range n.Table.rows[i][j] {
				pbNeighbors = append(pbNeighbors, neighbor.ToProto())
			}
			entries = append(entries, &pb.RoutingTableEntry{Neighbors: pbNeighbors})
		}
	}

	return &pb.RTCopyResponse{
		Entries: entries,
		Rows:    int32(id.DIGITS),
		Cols:    int32(id.RADIX),
	}, nil
}

// AddBackpointer adds a node to the backpointer set.
func (n *Node) AddBackpointer(ctx context.Context, req *pb.BackpointerRequest) (*pb.Nothing, error) {
	neighbor, err := NeighborFromProto(req.From)
	if err != nil {
		return nil, err
	}

	n.bpLock.Lock()
	n.Backpointers[neighbor.ID.String()] = neighbor
	n.bpLock.Unlock()

	log.Printf("Node %s added backpointer from %s", n.ID, neighbor.ID)

	// Optimization: Add backpointer source to routing table with latency check
	// We run this in a goroutine to not block the RPC response while pinging
	go func() {
		added := n.AddNeighborSafe(neighbor)
		if added {
			log.Printf("Optimization: Added backpointer source %s to routing table.", neighbor.ID)
		}
	}()

	return &pb.Nothing{}, nil
}

func (n *Node) RemoveBackpointer(ctx context.Context, req *pb.Neighbor) (*pb.Nothing, error) {
	neighbor, err := NeighborFromProto(req)
	if err != nil {
		return nil, err
	}

	n.bpLock.Lock()
	delete(n.Backpointers, neighbor.ID.String())
	n.bpLock.Unlock()

	return &pb.Nothing{}, nil
}

func (n *Node) NotifyMulticast(ctx context.Context, req *pb.MulticastRequest) (*pb.Nothing, error) {
	newNode, err := NeighborFromProto(req.NewNode)
	if err != nil {
		return nil, err
	}

	log.Printf("Node %s received Multicast notification for new node %s", n.ID, newNode.ID)
	
	go n.AddNeighborSafe(newNode)
	
	return &pb.Nothing{}, nil
}