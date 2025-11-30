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

// Global connection pool to prevent port exhaustion
var (
	connPool = make(map[string]*grpc.ClientConn)
	poolLock sync.RWMutex
)

type TapestryClient struct {
	pb.NodeServiceClient
	connAddr string // Keep track of address to not close the shared conn
}

func GetClient(address string) (*TapestryClient, error) {
	poolLock.RLock()
	conn, exists := connPool[address]
	poolLock.RUnlock()

	if exists {
		// Verify connection state (optional, gRPC handles this mostly)
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

	// Double check after lock
	if conn, exists = connPool[address]; exists {
		if conn.GetState() != connectivity.Shutdown {
			return &TapestryClient{
				NodeServiceClient: pb.NewNodeServiceClient(conn),
				connAddr:          address,
			}, nil
		}
	}

	// Create new connection
	// Use a reasonable timeout for the handshake
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newConn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be ready
		// Add Keepalive params if needed for long running systems
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

// Close does NOT close the underlying TCP connection anymore.
// It just acts as a cleanup hook if we need per-request cleanup.
func (c *TapestryClient) Close() {
	// We do NOT close c.conn here because it is shared in the pool.
	// The pool holds the connection open for reuse.
}

// CloseAllConnections is used when the process shuts down
func CloseAllConnections() {
	poolLock.Lock()
	defer poolLock.Unlock()
	for addr, conn := range connPool {
		conn.Close()
		delete(connPool, addr)
	}
}