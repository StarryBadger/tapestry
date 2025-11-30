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
	SALT_COUNT         = 3 
)

type Object struct {
	Key  string
	Data string
}

func (n *Node) StoreAndPublish(key string, data string) error {
	n.storeLocal(key, data)

	go n.publishSelfSalted(key)

	backups := n.SelectRandomNeighbors(REPLICATION_FACTOR - 1)
	
	if len(backups) == 0 {
		log.Printf("[CRITICAL] No neighbors found for replication of '%s'. Data is NOT fault tolerant.", key)
	} else if len(backups) < REPLICATION_FACTOR-1 {
		log.Printf("[WARNING] Only found %d/%d backups for '%s'.", len(backups), REPLICATION_FACTOR-1, key)
	}
	
	for _, backup := range backups {
		go func(target Neighbor) {
			client, err := GetClient(target.Address)
			if err != nil {
				log.Printf("Failed to connect to replica %s: %v", target.Address, err)
				return
			}
			defer client.Close()
			_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{Key: key, Data: data})
			if err == nil {
				log.Printf("Replicated '%s' to %s", key, target.Address)
			} else {
				log.Printf("Failed to replicate '%s' to %s: %v", key, target.Address, err)
			}
		}(backup)
	}

	return nil
}

func (n *Node) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.Ack, error) {
	log.Printf("Node %s received Replica for '%s'", n.ID, req.Key)
	n.storeLocal(req.Key, req.Data)
	go n.publishSelfSalted(req.Key)
	return &pb.Ack{Success: true}, nil
}

func (n *Node) publishSelfSalted(key string) {
	for i := 0; i < SALT_COUNT; i++ {
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
		
		go func(r *pb.PublishRequest, idx int) {
			n.Publish(context.Background(), r)
		}(req, i)
	}
}

func (n *Node) Get(key string) (Object, error) {
	objID := id.Hash(key)
	n.objLock.RLock()
	if obj, ok := n.LocalObjects[objID]; ok {
		n.objLock.RUnlock()
		return obj, nil
	}
	n.objLock.RUnlock()

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
			continue 
		}

		for _, pubProto := range resp.Publishers {
			pubAddr := pubProto.Address
			client, err := GetClient(pubAddr)
			if err != nil {
				continue
			}
			
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