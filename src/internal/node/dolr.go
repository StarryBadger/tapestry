package node

import (
	"context"
	"log"
	"time" 

	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

const MAX_HOPS = 20

func (n *Node) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.Nothing, error) {
	var objectID id.ID
	copy(objectID[:], req.ObjectId.Bytes)

	publisher, err := NeighborFromProto(req.Publisher)
	if err != nil {
		return nil, err
	}

	if req.HopLimit <= 0 {
		if req.HopLimit == 0 {
			req.HopLimit = MAX_HOPS
		}
	}

	log.Printf("Node %s handling Publish for %s (Hops Left: %d)", n.ID, objectID, req.HopLimit)

	n.addLocationPointer(objectID, publisher)

	nextHop, isRoot := n.computeNextHop(objectID)

	if isRoot || nextHop.ID.Equals(n.ID) || req.HopLimit <= 1 {
		log.Printf("Node %s terminating Publish for %s (Root=%v, Limit=%d)", n.ID, objectID, isRoot, req.HopLimit)
		return &pb.Nothing{}, nil
	}

	client, err := GetClient(nextHop.Address)
	if err != nil {
		log.Printf("Failed to forward Publish to %s: %v", nextHop.Address, err)
		return nil, err
	}
	defer client.Close()

	req.HopLimit--
	return client.Publish(ctx, req)
}

func (n *Node) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	var objectID id.ID
	copy(objectID[:], req.ObjectId.Bytes)

	if req.HopLimit == 0 {
		req.HopLimit = MAX_HOPS
	}

	publishers := n.getLocationPointers(objectID)
	if len(publishers) > 0 {
		log.Printf("Node %s found %d pointers for %s.", n.ID, len(publishers), objectID)
		
		var pbPublishers []*pb.Neighbor
		for _, p := range publishers {
			pbPublishers = append(pbPublishers, p.ToProto())
		}

		return &pb.LookupResponse{
			Publishers: pbPublishers, 
			Found:      true,
		}, nil
	}

	nextHop, isRoot := n.computeNextHop(objectID)

	if isRoot || nextHop.ID.Equals(n.ID) || req.HopLimit <= 1 {
		return &pb.LookupResponse{Found: false}, nil
	}

	client, err := GetClient(nextHop.Address)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	req.HopLimit--
	return client.Lookup(ctx, req)
}


func (n *Node) addLocationPointer(objID id.ID, publisher Neighbor) {
	n.lpLock.Lock()
	defer n.lpLock.Unlock()

	entries := n.LocationPointers[objID]
	
	for _, entry := range entries {
		if entry.Neighbor.ID.Equals(publisher.ID) {
			entry.LastUpdated = time.Now() 
			return 
		}
	}

	n.LocationPointers[objID] = append(entries, &PointerEntry{
		Neighbor:    publisher,
		LastUpdated: time.Now(),
	})
}

func (n *Node) getLocationPointers(objID id.ID) []Neighbor {
	n.lpLock.RLock()
	defer n.lpLock.RUnlock()

	var results []Neighbor
	entries := n.LocationPointers[objID]

	for _, entry := range entries {
		results = append(results, entry.Neighbor)
	}
	return results
}