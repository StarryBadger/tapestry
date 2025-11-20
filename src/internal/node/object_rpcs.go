package node

import (
	"context"
	"fmt"
	"log"

	pb "tapestry/api/proto"
)

func (n *Node) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	publisherPort := int(req.Port)
	objectID := req.ObjectID

	n.publishersLock.Lock()
	defer n.publishersLock.Unlock()

	if _, ok := n.ObjectPublishers[objectID]; !ok {
		n.ObjectPublishers[objectID] = make(map[int]struct{})
	}
	n.ObjectPublishers[objectID][publisherPort] = struct{}{}

	log.Printf("[REGISTER] Node %v is now tracking publisher %v for object %v", n.ID, publisherPort, objectID)
	return &pb.RegisterResponse{}, nil
}

func (n *Node) UnRegister(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	objectID := req.ObjectID

	n.publishersLock.Lock()
	defer n.publishersLock.Unlock()

	portSet, ok := n.ObjectPublishers[objectID]
	if ok {
		for port := range portSet {
			go func(p int) {
				client, conn, err := GetNodeClient(p)
				if err == nil {
					defer conn.Close()
					client.RemoveObject(context.Background(), &pb.RemoveObjectRequest{ObjectID: objectID})
				}
			}(port)
		}
		delete(n.ObjectPublishers, objectID)
		log.Printf("[UNREGISTER] Node %v is no longer tracking object %v", n.ID, objectID)
	}

	return &pb.RegisterResponse{}, nil
}

func (n *Node) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	objectID := req.ObjectID

	n.publishersLock.Lock()
	defer n.publishersLock.Unlock()

	portSet, ok := n.ObjectPublishers[objectID]
	if !ok || len(portSet) == 0 {
		return &pb.LookupResponse{Port: -1}, nil
	}

	for port := range portSet {
		client, conn, err := GetNodeClient(port)
		if err != nil {
			delete(portSet, port) 
			continue
		}
		_, err = client.Ping(context.Background(), &pb.Nothing{})
		conn.Close()
		if err != nil {
			delete(portSet, port) 
			continue
		}
		return &pb.LookupResponse{Port: int32(port)}, nil
	}

	return &pb.LookupResponse{Port: -1}, nil
}

func (n *Node) GetObject(ctx context.Context, req *pb.ObjectRequest) (*pb.ObjectResponse, error) {
	objectID := req.ObjectID

	n.objectsLock.RLock()
	obj, ok := n.Objects[objectID]
	n.objectsLock.RUnlock()

	if !ok {
		return nil, fmt.Errorf("object %v not found locally on node %v", objectID, n.ID)
	}
	return &pb.ObjectResponse{
		Name:    obj.Name,
		Content: obj.Content,
	}, nil
}

func (n *Node) StoreObject(ctx context.Context, obj *pb.Object) (*pb.Ack, error) {
	object := Object{
		Name:    obj.Name,
		Content: obj.Content,
	}
	objectID := StringToUint64(object.Name)

	n.objectsLock.Lock()
	n.Objects[objectID] = object
	n.objectsLock.Unlock()

	log.Printf("[STORE] Node %v stored a replica of object '%s'", n.ID, object.Name)
	return &pb.Ack{Success: true}, nil
}

func (n *Node) RemoveObject(ctx context.Context, req *pb.RemoveObjectRequest) (*pb.RemoveObjectResponse, error) {
	objectID := req.ObjectID

	n.objectsLock.Lock()
	delete(n.Objects, objectID)
	n.objectsLock.Unlock()

	log.Printf("[REMOVE] Node %v removed object %v from local store", n.ID, objectID)
	return &pb.RemoveObjectResponse{}, nil
}