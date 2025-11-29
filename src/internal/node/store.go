package node

import (
	"context"
	"fmt"
	"log"
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// Object represents the actual data stored on a node.
type Object struct {
	Key  string
	Data string
}

// StoreAndPublish saves data locally and advertises it to the network.
func (n *Node) StoreAndPublish(key string, data string) error {
	// 1. Store Locally
	objID := id.Hash(key)
	n.objLock.Lock()
	n.LocalObjects[objID] = Object{Key: key, Data: data}
	n.objLock.Unlock()

	log.Printf("[STORE] Node %s stored object '%s'", n.ID, key)

	// 2. Create Publish Request
	req := &pb.PublishRequest{
		ObjectId: &pb.NodeID{Bytes: objID.Bytes()},
		Publisher: &pb.Neighbor{
			Id:      &pb.NodeID{Bytes: n.ID.Bytes()},
			Address: n.Address,
		},
		HopLimit: 20, // INITIALIZE HOP LIMIT
	}

	// 3. Initiate Publish (Recursive)
	_, err := n.Publish(context.Background(), req)
	return err
}

// Get searches for a key in the network and retrieves the data.
func (n *Node) Get(key string) (Object, error) {
	objID := id.Hash(key)

	// 1. Check if I have it locally
	n.objLock.RLock()
	if obj, ok := n.LocalObjects[objID]; ok {
		n.objLock.RUnlock()
		log.Printf("[GET] Found '%s' locally.", key)
		return obj, nil
	}
	n.objLock.RUnlock()

	// 2. Lookup the location
	lookupReq := &pb.LookupRequest{
		ObjectId: &pb.NodeID{Bytes: objID.Bytes()},
		HopLimit: 20, // INITIALIZE HOP LIMIT
	}
	
	log.Printf("[GET] Looking up location for '%s'...", key)
	resp, err := n.Lookup(context.Background(), lookupReq)
	if err != nil {
		return Object{}, err
	}

	if !resp.Found {
		return Object{}, fmt.Errorf("object not found in network")
	}

	publisherAddr := resp.Publisher.Address
	log.Printf("[GET] Object '%s' found at %s. Fetching...", key, publisherAddr)

	// 3. Fetch data from the Publisher
	client, err := GetClient(publisherAddr)
	if err != nil {
		return Object{}, fmt.Errorf("failed to connect to publisher %s: %v", publisherAddr, err)
	}
	defer client.Close()

	fetchResp, err := client.Fetch(context.Background(), &pb.FetchRequest{Key: key})
	if err != nil {
		return Object{}, fmt.Errorf("failed to fetch data: %v", err)
	}

	if !fetchResp.Found {
		return Object{}, fmt.Errorf("publisher claimed to have object but Fetch returned not found")
	}

	return Object{
		Key:  key, 
		Data: fetchResp.Data,
	}, nil
}

func (n *Node) Remove(key string) {
	objID := id.Hash(key)
	n.objLock.Lock()
	delete(n.LocalObjects, objID)
	n.objLock.Unlock()
	log.Printf("[REMOVE] Deleted '%s' locally.", key)
}

func (n *Node) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	objID := id.Hash(req.Key)

	n.objLock.RLock()
	obj, ok := n.LocalObjects[objID]
	n.objLock.RUnlock()

	if !ok {
		return &pb.FetchResponse{Found: false}, nil
	}

	return &pb.FetchResponse{
		Data:  obj.Data,
		Found: true,
	}, nil
}