package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	
	pb "tapestry/api/proto"
	"tapestry/internal/id"
	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedNodeServiceServer

	// Identity
	ID      id.ID
	Port    int
	Address string // "localhost:Port"

	// Network
	GrpcServer *grpc.Server
	Listener   net.Listener

	// Tapestry Core
	Table *RoutingTable

	// Backpointers (Who points to me?)
	// Used for Optimizing the Mesh and Voluntary Delete
	Backpointers map[string]Neighbor // Key: Hex ID string
	bpLock       sync.RWMutex

	// Decentralized Object Location (DOLR)
	// LocationPointers: Objects cached on this node pointing to the publisher
	// Map: ObjectID -> List of Publishers
	LocationPointers map[id.ID][]Neighbor
	lpLock           sync.RWMutex

	// Local Object Store (The actual data this node hosts)
	LocalObjects map[id.ID]Object
	objLock      sync.RWMutex
}

// NewNode creates a new Tapestry node.
// bootstrapAddr can be empty if this is the first node.
func NewNode(port int) (*Node, error) {
	// Generate 160-bit SHA-1 ID
	nodeID := id.NewRandomID()
	address := fmt.Sprintf("localhost:%d", port)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	n := &Node{
		ID:               nodeID,
		Port:             port,
		Address:          address,
		GrpcServer:       grpc.NewServer(),
		Listener:         listener,
		Table:            NewRoutingTable(nodeID),
		Backpointers:     make(map[string]Neighbor),
		LocationPointers: make(map[id.ID][]Neighbor),
		LocalObjects:     make(map[id.ID]Object),
	}

	pb.RegisterNodeServiceServer(n.GrpcServer, n)
	log.Printf("Created Node %s at %s", n.ID.String(), n.Address)
	
	return n, nil
}

func (n *Node) Start() error {
	log.Printf("Starting gRPC server on %s", n.Address)
	return n.GrpcServer.Serve(n.Listener)
}

func (n *Node) Stop() {
	n.GrpcServer.GracefulStop()
}

// Ping is a basic liveness check
func (n *Node) Ping(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

// --- Placeholders for Future Steps ---

// TODO: Step 3 - Implement GetNextHop (Surrogate Routing)
// TODO: Step 4 - Implement Publish/Lookup (DOLR)
// TODO: Step 5 - Implement Join/Insert