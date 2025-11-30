package node

import (
	"context"
	"fmt"
	"time"

	pb "tapestry/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TapestryClient is a wrapper around the generated gRPC client
type TapestryClient struct {
	pb.NodeServiceClient
	conn *grpc.ClientConn
}

// GetClient creates a connection to a remote Tapestry node.
func GetClient(address string) (*TapestryClient, error) {
	// Set a timeout for connection establishment
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Wait for connection to be ready
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	client := pb.NewNodeServiceClient(conn)
	return &TapestryClient{
		NodeServiceClient: client,
		conn:              conn,
	}, nil
}

// Close closes the underlying connection.
func (c *TapestryClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}