package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
	"google.golang.org/grpc"
)

// PointerEntry wraps a Neighbor with a timestamp for Soft-State
type PointerEntry struct {
	Neighbor Neighbor
	LastUpdated time.Time
}

type Node struct {
	pb.UnimplementedNodeServiceServer

	ID               id.ID
	Port             int
	Address          string 
	GrpcServer       *grpc.Server
	Listener         net.Listener
	Table            *RoutingTable
	Backpointers     map[string]Neighbor
	bpLock           sync.RWMutex
	LocationPointers map[id.ID][]*PointerEntry
	lpLock           sync.RWMutex	
	LocalObjects     map[id.ID]Object
	objLock          sync.RWMutex
	stopChan chan struct{}
}

func NewNode(port int) (*Node, error) {
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
		LocationPointers: make(map[id.ID][]*PointerEntry),
		LocalObjects:     make(map[id.ID]Object),
		stopChan:         make(chan struct{}),
	}

	pb.RegisterNodeServiceServer(n.GrpcServer, n)
	log.Printf("Created Node %s at %s", n.ID.String(), n.Address)
	
	return n, nil
}

func (n *Node) Start() error {
	// Start Background Maintenance Threads
	go n.StartMaintenanceLoop()
	go n.StartRepublishLoop()

	log.Printf("Starting gRPC server on %s", n.Address)
	return n.GrpcServer.Serve(n.Listener)
}

func (n *Node) Stop() {
	close(n.stopChan) // Signal threads to stop
	n.GrpcServer.GracefulStop()
}

// Ping is a basic liveness check
func (n *Node) Ping(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

// SelectRandomNeighbors picks 'count' distinct neighbors from the routing table.
func (n *Node) SelectRandomNeighbors(count int) []Neighbor {
	n.Table.lock.RLock()
	defer n.Table.lock.RUnlock()

	var candidates []Neighbor
	// Collect all neighbors
	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			candidates = append(candidates, n.Table.rows[i][j]...)
		}
	}

	// Shuffle (simple version) or just pick first 'count' distinct ones
	// In a real app, use math/rand to shuffle. 
    // Since map iteration is random-ish in Go, and we appended in order, 
    // let's just pick distinct ones.
    
	selected := make([]Neighbor, 0, count)
	seen := make(map[string]bool)

	for _, nb := range candidates {
		if len(selected) >= count {
			break
		}
		// Don't pick ourselves or duplicates
		if !nb.ID.Equals(n.ID) && !seen[nb.ID.String()] {
			selected = append(selected, nb)
			seen[nb.ID.String()] = true
		}
	}
	return selected
}

// Probe measures the RTT to a neighbor address.
func (n *Node) Probe(address string) (time.Duration, error) {
	start := time.Now()
	
	client, err := GetClient(address)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	
	_, err = client.Ping(context.Background(), &pb.Nothing{})
	if err != nil {
		return 0, err
	}
	
	return time.Since(start), nil
}

// AddNeighborSafe measures latency and adds the neighbor.
// Returns true if the table was updated.
func (n *Node) AddNeighborSafe(nb Neighbor) bool {
	// 1. Measure Latency
	rtt, err := n.Probe(nb.Address)
	if err != nil {
		return false // Node unreachable
	}
	
	nb.Latency = rtt
	
	// 2. Add to Table (Table handles sorting)
	return n.Table.Add(nb)
}