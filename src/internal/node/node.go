package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"math/rand"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
	"google.golang.org/grpc"
)

type PointerEntry struct {
	Neighbor    Neighbor
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
	stopChan     chan struct{} // For internal threads (maintenance)
	ExitChan     chan struct{} // For main.go to know we are done
	shutdownOnce sync.Once     // NEW: Ensure Stop() is idempotent
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
		ExitChan:         make(chan struct{}),
	}

	pb.RegisterNodeServiceServer(n.GrpcServer, n)
	log.Printf("Created Node %s at %s", n.ID.String(), n.Address)

	return n, nil
}

func (n *Node) Start() error {
	go n.StartMaintenanceLoop()
	go n.StartRepublishLoop()
	log.Printf("Starting gRPC server on %s", n.Address)
	return n.GrpcServer.Serve(n.Listener)
}

func (n *Node) Stop() {
	n.shutdownOnce.Do(func() {
		close(n.stopChan)
		if n.GrpcServer != nil {
			n.GrpcServer.GracefulStop()
		}
	})
}

func (n *Node) Ping(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

func (n *Node) SelectRandomNeighbors(count int) []Neighbor {
	n.Table.lock.RLock()
	defer n.Table.lock.RUnlock()

	var candidates []Neighbor
	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			candidates = append(candidates, n.Table.rows[i][j]...)
		}
	}
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	selected := make([]Neighbor, 0, count)
	seen := make(map[string]bool)

	for _, nb := range candidates {
		if len(selected) >= count {
			break
		}
		if !nb.ID.Equals(n.ID) && !seen[nb.ID.String()] {
			selected = append(selected, nb)
			seen[nb.ID.String()] = true
		}
	}
	return selected
}


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

func (n *Node) AddNeighborSafe(nb Neighbor) bool {
	rtt, err := n.Probe(nb.Address)
	if err != nil {
		return false
	}
	nb.Latency = rtt
	return n.Table.Add(nb)
}

func (n *Node) GetLocalObjectCount() int {
	n.objLock.RLock()
	defer n.objLock.RUnlock()
	return len(n.LocalObjects)
}

func (n *Node) GetLocationPointerCount() int {
	n.lpLock.RLock()
	defer n.lpLock.RUnlock()
	return len(n.LocationPointers)
}