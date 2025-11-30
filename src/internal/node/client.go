package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "tapestry/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/connectivity"
)

var (
	connPool = make(map[string]*grpc.ClientConn)
	poolLock sync.RWMutex
)

type TapestryClient struct {
	pb.NodeServiceClient
	connAddr string 
}

func GetClient(address string) (*TapestryClient, error) {
	poolLock.RLock()
	conn, exists := connPool[address]
	poolLock.RUnlock()

	if exists {
		state := conn.GetState()
		if state != connectivity.Shutdown {
			return &TapestryClient{
				NodeServiceClient: pb.NewNodeServiceClient(conn),
				connAddr:          address,
			}, nil
		}
	}

	poolLock.Lock()
	defer poolLock.Unlock()

	if conn, exists = connPool[address]; exists {
		if conn.GetState() != connectivity.Shutdown {
			return &TapestryClient{
				NodeServiceClient: pb.NewNodeServiceClient(conn),
				connAddr:          address,
			}, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newConn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	connPool[address] = newConn

	return &TapestryClient{
		NodeServiceClient: pb.NewNodeServiceClient(newConn),
		connAddr:          address,
	}, nil
}

func (c *TapestryClient) Close() {
}

func CloseAllConnections() {
	poolLock.Lock()
	defer poolLock.Unlock()
	for addr, conn := range connPool {
		conn.Close()
		delete(connPool, addr)
	}
}