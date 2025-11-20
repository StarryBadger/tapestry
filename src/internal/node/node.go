package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"math/rand"
	"time"
	"google.golang.org/grpc"
	pb "tapestry/api/proto"
	"tapestry/internal/util"
)

type Node struct {
	pb.UnimplementedNodeServiceServer

	ID  uint64
	Port int
	GrpcServer *grpc.Server
	Listener net.Listener
	RoutingTable [][]int
	Backpointers *util.BackPointerTable 
	rtLock sync.RWMutex
	bpLock sync.RWMutex
}

func NewNode(port int) (*Node, error) {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	rt := make([][]int, util.DIGITS)
	for i := 0; i< util.DIGITS; i++ {
		rt[i] = make([]int, util.RADIX)
		for j := 0; j< util.RADIX; j++ {
			rt[i][j] = -1
		}	
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	nodeID := rng.Uint64()

	node := &Node{
		ID:        nodeID,
		Port:      listener.Addr().(*net.TCPAddr).Port,
		GrpcServer: grpc.NewServer(),
		Listener:  listener,
		RoutingTable: rt,
		Backpointers: util.NewBackPointerTable(), 
	}

	pb.RegisterNodeServiceServer(node.GrpcServer, node)

	log.Printf("Node created with ID %d on port %d", node.ID, node.Port)
	return node, nil
}

func (node *Node) Start() error {
	log.Printf("Starting gRPC server on port %d", node.Port)
	return node.GrpcServer.Serve(node.Listener)
}

func (node *Node) Stop() {
	log.Printf("Stopping gRPC server on port %d", node.Port)
	node.GrpcServer.GracefulStop()
}

func (node *Node) Ping(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	log.Printf("Received a Ping from a client on node %d!", node.ID)
	return &pb.Nothing{}, nil
}