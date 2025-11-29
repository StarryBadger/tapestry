package node

import (
	"context"
	"log"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

const MAX_HOPS = 20 // Sufficient for small-medium networks (Log16(N) is small)

// Publish advertises an object's location to the network.
func (n *Node) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.Nothing, error) {
	var objectID id.ID
	copy(objectID[:], req.ObjectId.Bytes)

	publisher, err := NeighborFromProto(req.Publisher)
	if err != nil {
		return nil, err
	}

	// 1. Hop Limit Check (Cycle Detection)
	if req.HopLimit <= 0 {
		// If 0, assume it's a new request and initialize (unless it really is 0 from a loop)
		// But wait, if it came from another node as 0, we should stop.
		// Let's assume the initiator sets it. If it's 0, we set default.
		// If it's -1 (exhausted), we stop.
		// Since proto defaults to 0, we need a way to distinguish "Fresh" from "Exhausted".
		// We'll treat 0 as "Fresh" and set to MAX. 
		// We will send decremented values. If we receive 1, we send 0.
		// If we receive 0 *and it's recursive*, we stop.
		// Actually, simpler: The StoreAndPublish (initiator) sets it to MAX.
		if req.HopLimit == 0 {
			req.HopLimit = MAX_HOPS
		}
	}

	log.Printf("Node %s handling Publish for %s (Hops Left: %d)", n.ID, objectID, req.HopLimit)

	// 2. Cache the Pointer Locally
	n.addLocationPointer(objectID, publisher)

	// 3. Compute Next Hop
	nextHop, isRoot := n.computeNextHop(objectID)

	// 4. Termination Condition
	// Stop if:
	// a) We are root
	// b) Next hop is us
	// c) Hop limit exhausted (Safety break for cycles)
	if isRoot || nextHop.ID.Equals(n.ID) || req.HopLimit <= 1 {
		log.Printf("Node %s terminating Publish for %s (Root=%v, Limit=%d)", n.ID, objectID, isRoot, req.HopLimit)
		return &pb.Nothing{}, nil
	}

	// 5. Recursive Step
	client, err := GetClient(nextHop.Address)
	if err != nil {
		log.Printf("Failed to forward Publish to %s: %v", nextHop.Address, err)
		return nil, err
	}
	defer client.Close()

	// Decrement hops
	req.HopLimit--
	return client.Publish(ctx, req)
}

// Lookup searches for an object.
func (n *Node) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	var objectID id.ID
	copy(objectID[:], req.ObjectId.Bytes)

	// 1. Hop Limit
	if req.HopLimit == 0 {
		req.HopLimit = MAX_HOPS
	}

	// 2. Check Local Pointers
	publishers := n.getLocationPointers(objectID)
	if len(publishers) > 0 {
		bestPub := publishers[0]
		return &pb.LookupResponse{
			Publisher: bestPub.ToProto(),
			Found:     true,
		}, nil
	}

	// 3. Compute Next Hop
	nextHop, isRoot := n.computeNextHop(objectID)

	// 4. Termination
	if isRoot || nextHop.ID.Equals(n.ID) || req.HopLimit <= 1 {
		return &pb.LookupResponse{Found: false}, nil
	}

	// 5. Recursive Step
	client, err := GetClient(nextHop.Address)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	req.HopLimit--
	return client.Lookup(ctx, req)
}

// --- Helper Methods for Pointers ---

func (n *Node) addLocationPointer(objID id.ID, publisher Neighbor) {
	n.lpLock.Lock()
	defer n.lpLock.Unlock()

	pointers := n.LocationPointers[objID]
	for _, p := range pointers {
		if p.ID.Equals(publisher.ID) {
			return 
		}
	}
	n.LocationPointers[objID] = append(pointers, publisher)
}

func (n *Node) getLocationPointers(objID id.ID) []Neighbor {
	n.lpLock.RLock()
	defer n.lpLock.RUnlock()

	original := n.LocationPointers[objID]
	copySlice := make([]Neighbor, len(original))
	copy(copySlice, original)
	return copySlice
}