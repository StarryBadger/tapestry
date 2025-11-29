package node

import (
	"context"
	"fmt"
	"log"
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

const (
	REPLICATION_FACTOR = 3
	SALT_COUNT         = 3 // Publish to 3 different roots for redundancy
)

type Object struct {
	Key  string
	Data string
}

// StoreAndPublish saves data locally, replicates, and advertises to MULTIPLE roots.
func (n *Node) StoreAndPublish(key string, data string) error {
	// 1. Store Locally
	n.storeLocal(key, data)

	// 2. Publish Self (Salted)
	// We publish to multiple random IDs derived from the key.
	// This ensures that even in a sparse network, paths are likely to cross.
	go n.publishSelfSalted(key)

	// 3. Select Backups
	backups := n.SelectRandomNeighbors(REPLICATION_FACTOR - 1)
	
	for _, backup := range backups {
		go func(target Neighbor) {
			client, err := GetClient(target.Address)
			if err != nil {
				return
			}
			defer client.Close()
			client.Replicate(context.Background(), &pb.ReplicateRequest{Key: key, Data: data})
		}(backup)
	}

	return nil
}

func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.Ack, error) {
	log.Printf("Node %s received Replica for '%s'", n.ID, req.Key)
	n.storeLocal(req.Key, req.Data)
	// Replicas also publish to multiple roots
	go n.publishSelfSalted(req.Key)
	return &pb.Ack{Success: true}, nil
}

// publishSelfSalted publishes the location to multiple calculated roots.
func (n *Node) publishSelfSalted(key string) {
	for i := 0; i < SALT_COUNT; i++ {
		// Create a salted ID: Hash("key" + "0"), Hash("key" + "1")...
		saltedKey := fmt.Sprintf("%s-%d", key, i)
		targetID := id.Hash(saltedKey)

		req := &pb.PublishRequest{
			ObjectId: &pb.NodeID{Bytes: targetID.Bytes()},
			Publisher: &pb.Neighbor{
				Id:      &pb.NodeID{Bytes: n.ID.Bytes()},
				Address: n.Address,
			},
			HopLimit: 20,
		}
		
		// We don't wait for these; they run in background
		go func(r *pb.PublishRequest, idx int) {
			n.Publish(context.Background(), r)
		}(req, i)
	}
}

func (n *Node) Get(key string) (Object, error) {
	// 1. Check Local
	objID := id.Hash(key) // Local storage still uses exact hash
	n.objLock.RLock()
	if obj, ok := n.LocalObjects[objID]; ok {
		n.objLock.RUnlock()
		return obj, nil
	}
	n.objLock.RUnlock()

	// 2. Lookup using Salts
	// Try finding the object using any of the salted paths
	for i := 0; i < SALT_COUNT; i++ {
		saltedKey := fmt.Sprintf("%s-%d", key, i)
		targetID := id.Hash(saltedKey)

		log.Printf("[GET] Lookup attempt %d/3 for '%s' (Salt: %s)...", i+1, key, saltedKey)

		lookupReq := &pb.LookupRequest{
			ObjectId: &pb.NodeID{Bytes: targetID.Bytes()},
			HopLimit: 20,
		}
		
		resp, err := n.Lookup(context.Background(), lookupReq)
		if err != nil || !resp.Found || len(resp.Publishers) == 0 {
			continue // Try next salt
		}

		// 3. Found Pointers! Try to Fetch.
		// (Same failover logic as before)
		for _, pubProto := range resp.Publishers {
			pubAddr := pubProto.Address
			client, err := GetClient(pubAddr)
			if err != nil {
				continue
			}
			
			// Note: We fetch using the ORIGINAL key, not the salt
			fetchResp, err := client.Fetch(context.Background(), &pb.FetchRequest{Key: key})
			client.Close()
			
			if err == nil && fetchResp.Found {
				return Object{Key: key, Data: fetchResp.Data}, nil
			}
		}
	}

	return Object{}, fmt.Errorf("object not found after checking %d paths", SALT_COUNT)
}

func (n *Node) storeLocal(key, data string) {
	objID := id.Hash(key)
	n.objLock.Lock()
	n.LocalObjects[objID] = Object{Key: key, Data: data}
	n.objLock.Unlock()
}

// Remove deletes local object
func (n *Node) Remove(key string) {
	objID := id.Hash(key)
	n.objLock.Lock()
	delete(n.LocalObjects, objID)
	n.objLock.Unlock()
}

func (n *Node) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	objID := id.Hash(req.Key)
	n.objLock.RLock()
	obj, ok := n.LocalObjects[objID]
	n.objLock.RUnlock()

	if !ok {
		return &pb.FetchResponse{Found: false}, nil
	}
	return &pb.FetchResponse{Data: obj.Data, Found: true}, nil
}