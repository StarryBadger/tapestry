package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"math/rand"
	"time"
	"google.golang.org/grpc"
	pb "tapestry/src/api/proto"
)

type Node struct {
	pb.UnimplementedNodeServiceServer
	
	ID  uint64
	Port int
	GrpcServer *grpc.Server
	Listener net.Listener
}

func NewNode(port int) (*Node, error) {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	rng:= rand.Seed(time.Now().UnixNano())
	nodeID := rand.Uint64()

	node := &Node{
		ID:        nodeID,
		Port:      lis.Addr().(*net.TCPAddr).Port,
		GrpcServer: grpc.NewServer(),
		Listener:  listener,
	}

	pb.RegisterNodeServiceServer(n.GrpcServer, node)

	log.Printf("Node created with ID %d on port %d", node.ID, node.Port)
	return node, nil
}

func (n *Node) Start() error {
	log.Printf("Starting gRPC server on port %d", n.Port)
	return n.GrpcServer.Serve(n.Listener)
}

func (n *Node) Stop()
 {
	log.Printf("Stopping gRPC server on port %d", n.Port)
	n.GrpcServer.GracefulStop()
}

func (n *Node) Ping(ctx context.Context, req *pb.Nothing) (*pb.Nothing, error) {
	log.Printf("Received a Ping from a client on node %d!", n.ID)
	return &pb.Nothing{}, nil
}