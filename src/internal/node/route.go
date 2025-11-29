package node

import (
	"context"
	"fmt"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// GetNextHop determines the next node in the path towards a target ID.
// It implements the core routing logic described in the Tapestry papers.
func (n *Node) GetNextHop(ctx context.Context, req *pb.RouteRequest) (*pb.RouteResponse, error) {
	// FIX: Do not use id.Parse here. Copy raw bytes directly.
	var targetID id.ID
	if len(req.TargetId.Bytes) != id.BYTES {
		return nil, fmt.Errorf("invalid target ID length: %d", len(req.TargetId.Bytes))
	}
	copy(targetID[:], req.TargetId.Bytes)

	// 2. Compute Next Hop locally
	nextHop, isRoot := n.computeNextHop(targetID)

	// 3. Construct Response
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

	// If we match all digits (should be caught by step 1, but safety check)
	if level >= id.DIGITS {
		return Neighbor{ID: n.ID, Address: n.Address}, true
	}

	// 3. Identify the desired digit for the next hop
	desiredDigit := target.GetDigit(level)

	// 4. Primary Lookup: Do we have a node with this digit?
	primaryCandidates := n.Table.rows[level][desiredDigit]
	if len(primaryCandidates) > 0 {
		return primaryCandidates[0], false
	}

	// 5. Surrogate Routing (Handling Holes)
	// Iterate (digit + 1, digit + 2 ...) mod RADIX.
	for offset := 1; offset < id.RADIX; offset++ {
		surrogateDigit := (desiredDigit + offset) % id.RADIX
		surrogateCandidates := n.Table.rows[level][surrogateDigit]

		if len(surrogateCandidates) > 0 {
			return surrogateCandidates[0], false
		}
	}

	// 6. Surrogate Root
	// If NO entries exist in this level (except potentially ourselves),
	// then WE are the root for this object.
	return Neighbor{ID: n.ID, Address: n.Address}, true
}