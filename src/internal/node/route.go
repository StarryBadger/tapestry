package node

import (
	"context"
	"fmt"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// GetNextHop determines the next node in the path towards a target ID.
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

// computeNextHop calculates the best next hop from the local routing table.
func (n *Node) computeNextHop(target id.ID) (Neighbor, bool) {
	n.Table.lock.RLock()
	defer n.Table.lock.RUnlock()

	// 1. Exact Match Check
	if n.ID.Equals(target) {
		return Neighbor{ID: n.ID, Address: n.Address}, true
	}

	// 2. Determine the level of matching prefix
	level := id.SharedPrefixLength(n.ID, target)

	// If we match all digits, we are the node.
	if level >= id.DIGITS {
		return Neighbor{ID: n.ID, Address: n.Address}, true
	}

	// 3. Identify the desired digit for the next hop
	desiredDigit := target.GetDigit(level)

	// 4. Primary Lookup: Do we have a node with this digit?
	primaryCandidates := n.Table.rows[level][desiredDigit]
	if len(primaryCandidates) > 0 {
		// Return the closest primary neighbor
		return primaryCandidates[0], false
	}

	// 5. Surrogate Routing (Handling Holes)
	// Iterate (digit + 1, digit + 2 ...) mod RADIX.
	for offset := 1; offset < id.RADIX; offset++ {
		surrogateDigit := (desiredDigit + offset) % id.RADIX
		
		// CRITICAL FIX: Check if WE are the match for this surrogate digit.
		// If the routing table doesn't have the desired digit, and the next closest
		// digit in the namespace is OUR digit, then WE are the surrogate root.
		if n.ID.GetDigit(level) == surrogateDigit {
			return Neighbor{ID: n.ID, Address: n.Address}, true
		}

		// Check Routing Table for this surrogate digit
		surrogateCandidates := n.Table.rows[level][surrogateDigit]
		if len(surrogateCandidates) > 0 {
			return surrogateCandidates[0], false
		}
	}

	// 6. Fallback (Should be unreachable if logic above is correct, 
    // but implies we are the only node we know of)
	return Neighbor{ID: n.ID, Address: n.Address}, true
}