package node

import (
	"context"
	"fmt"
	"log"

	pb "tapestry/api/proto"
)

func (n *Node) AddObject(obj Object) error {
	objectID := StringToUint64(obj.Name)
	n.objectsLock.Lock()
	n.Objects[objectID] = obj
	n.objectsLock.Unlock()
	log.Printf("[ADD] Object '%s' stored locally.", obj.Name)

	seen := make(map[int]struct{})
	replicatedCount := 0
	n.rtLock.RLock()
	defer n.rtLock.RUnlock()

	for _, row := range n.RoutingTable {
		for _, port := range row {
			if port != -1 && port != n.Port {
				if _, exists := seen[port]; !exists {
					seen[port] = struct{}{}
					go func(p int) {
						client, conn, err := GetNodeClient(p)
						if err == nil {
							defer conn.Close()
							client.StoreObject(context.Background(), &pb.Object{Name: obj.Name, Content: obj.Content})
						}
					}(port)
					replicatedCount++
					if replicatedCount >= 2 {
						return nil 
					}
				}
			}
		}
	}
	return nil
}

func (n *Node) Publish(object Object) error {
	objectID := StringToUint64(object.Name)
	rootPort, err := n.FindRoot(objectID)
	if err != nil {
		return err
	}

	client, conn, err := GetNodeClient(rootPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.Register(context.Background(), &pb.RegisterRequest{
		Port:     int32(n.Port),
		ObjectID: objectID,
	})
	if err != nil {
		return fmt.Errorf("failed to register with root: %v", err)
	}

	log.Printf("[PUBLISH] Registered as publisher for '%s' with root %d", object.Name, rootPort)
	return nil
}

func (n *Node) UnPublish(name string) error {
	objectID := StringToUint64(name)

	// First, remove it locally.
	n.objectsLock.Lock()
	delete(n.Objects, objectID)
	n.objectsLock.Unlock()

	rootPort, err := n.FindRoot(objectID)
	if err != nil {
		return err
	}

	client, conn, err := GetNodeClient(rootPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.UnRegister(context.Background(), &pb.RegisterRequest{
		Port:     int32(n.Port),
		ObjectID: objectID,
	})
	if err != nil {
		return fmt.Errorf("failed to unregister with root: %v", err)
	}

	log.Printf("[UNPUBLISH] Unregistered object '%s' from the network.", name)
	return nil
}

func (n *Node) FindObject(name string) (Object, error) {
	objectID := StringToUint64(name)
	rootPort, err := n.FindRoot(objectID)
	if err != nil {
		return Object{}, err
	}

	rootClient, rootConn, err := GetNodeClient(rootPort)
	if err != nil {
		return Object{}, err
	}
	defer rootConn.Close()

	lookupResp, err := rootClient.Lookup(context.Background(), &pb.LookupRequest{ObjectID: objectID})
	if err != nil {
		return Object{}, fmt.Errorf("lookup failed: %v", err)
	}

	publisherPort := int(lookupResp.Port)
	if publisherPort == -1 {
		return Object{}, fmt.Errorf("no live publishers found for object '%s'", name)
	}

	pubClient, pubConn, err := GetNodeClient(publisherPort)
	if err != nil {
		return Object{}, err
	}
	defer pubConn.Close()

	objResp, err := pubClient.GetObject(context.Background(), &pb.ObjectRequest{ObjectID: objectID})
	if err != nil {
		return Object{}, fmt.Errorf("failed to get object from publisher %d: %v", publisherPort, err)
	}

	return Object{Name: objResp.Name, Content: objResp.Content}, nil
}