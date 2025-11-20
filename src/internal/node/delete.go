package node

import (
	"context"
	"log"

	pb "tapestry/api/proto"
	"tapestry/internal/util"
)

func (n *Node) GracefulLeave() {
	log.Printf("Node %v starting graceful leave...", n.ID)
	var replacementPort int = -1
	var replacementID uint64

	n.rtLock.RLock()
	for i := util.DIGITS - 1; i >= 0; i-- {
		for j := 0; j < util.RADIX; j++ {
			if n.RoutingTable[i][j] != -1 && n.RoutingTable[i][j] != n.Port {
				replacementPort = n.RoutingTable[i][j]
				break
			}
		}
		if replacementPort != -1 {
			break
		}
	}
	n.rtLock.RUnlock()

	if replacementPort != -1 {
		client, conn, err := GetNodeClient(replacementPort)
		if err == nil {
			defer conn.Close()
			resp, err := client.GetID(context.Background(), &pb.GetIDRequest{})
			if err == nil {
				replacementID = resp.ID
			}
		}
	}

	n.bpLock.RLock()
	for port := range n.Backpointers.Set {
		go func(p int) {
			client, conn, err := GetNodeClient(p)
			if err == nil {
				defer conn.Close()
				client.RTUpdate(context.Background(), &pb.RTUpdateRequest{
					ReplacementID:   replacementID,
					ReplacementPort: int32(replacementPort),
					ID:              n.ID,
					Port:            int32(n.Port),
				})
			}
		}(port)
	}
	n.bpLock.RUnlock()

	n.rtLock.RLock()
	for _, row := range n.RoutingTable {
		for _, port := range row {
			if port != -1 && port != n.Port {
				go func(p int) {
					client, conn, err := GetNodeClient(p)
					if err == nil {
						defer conn.Close()
						client.BPRemove(context.Background(), &pb.BPRemoveRequest{Port: int32(n.Port)})
					}
				}(port)
			}
		}
	}
	n.rtLock.RUnlock()
}

func (n *Node) GetID(ctx context.Context, req *pb.GetIDRequest) (*pb.GetIDResponse, error) {
	return &pb.GetIDResponse{ID: n.ID}, nil
}

func (n *Node) BPUpdate(ctx context.Context, req *pb.BPUpdateRequest) (*pb.BPUpdateResponse, error) {
	port := int(req.Port)
	n.bpLock.Lock()
	n.Backpointers.Set[port] = struct{}{}
	n.bpLock.Unlock()
	log.Printf("Node %v added %v to backpointers", n.ID, port)
	return &pb.BPUpdateResponse{Success: true}, nil
}

func (n *Node) BPRemove(ctx context.Context, req *pb.BPRemoveRequest) (*pb.BPRemoveResponse, error) {
	port := int(req.Port)
	n.bpLock.Lock()
	delete(n.Backpointers.Set, port)
	n.bpLock.Unlock()
	log.Printf("Node %v removed %v from backpointers", n.ID, port)
	return &pb.BPRemoveResponse{Success: true}, nil
}

func (n *Node) RTUpdate(ctx context.Context, req *pb.RTUpdateRequest) (*pb.RTUpdateResponse, error) {
	leavingPort := int(req.Port)
	replacementPort := int(req.ReplacementPort)
	replacementID := req.ReplacementID

	n.rtLock.Lock()
	defer n.rtLock.Unlock()

	for i, row := range n.RoutingTable {
		for j, port := range row {
			if port == leavingPort {
				n.RoutingTable[i][j] = -1
			}
		}
	}

	if replacementPort != -1 {
		sharedPrefixLen := util.CommonPrefixLen(n.ID, replacementID)
		for i := 0; i <= sharedPrefixLen; i++ {
			digit := util.GetDigit(replacementID, i)
			if n.RoutingTable[i][digit] == -1 {
				n.RoutingTable[i][digit] = replacementPort
			}
		}
	}

	return &pb.RTUpdateResponse{Success: true}, nil
}