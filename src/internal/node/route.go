package node

import (
	"context"
	"fmt"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

func (n *Node) GetNextHop(ctx context.Context, req *pb.RouteRequest) (*pb.RouteResponse, error) {
	var targetID id.ID
	if len(req.TargetId.Bytes) != id.BYTES {
		return nil, fmt.Errorf("invalid target ID length: %d", len(req.TargetId.Bytes))
	}
	copy(targetID[:], req.TargetId.Bytes)

	nextHop, isRoot := n.computeNextHop(targetID)

	return &pb.RouteResponse{
		NextHop: nextHop.ToProto(),
		IsRoot:  isRoot,
	}, nil
}

func (n *Node) computeNextHop(target id.ID) (Neighbor, bool) {
	n.Table.lock.RLock()
	defer n.Table.lock.RUnlock()

	if n.ID.Equals(target) {
		return Neighbor{ID: n.ID, Address: n.Address}, true
	}

	level := id.SharedPrefixLength(n.ID, target)

	if level >= id.DIGITS {
		return Neighbor{ID: n.ID, Address: n.Address}, true
	}

	desiredDigit := target.GetDigit(level)

	primaryCandidates := n.Table.rows[level][desiredDigit]
	if len(primaryCandidates) > 0 {
		return primaryCandidates[0], false
	}

	for offset := 1; offset < id.RADIX; offset++ {
		surrogateDigit := (desiredDigit + offset) % id.RADIX
		
		if n.ID.GetDigit(level) == surrogateDigit {
			return Neighbor{ID: n.ID, Address: n.Address}, true
		}

		surrogateCandidates := n.Table.rows[level][surrogateDigit]
		if len(surrogateCandidates) > 0 {
			return surrogateCandidates[0], false
		}
	}

	return Neighbor{ID: n.ID, Address: n.Address}, true
}