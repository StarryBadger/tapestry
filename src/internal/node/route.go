package node

import (
	"context"
	"fmt"
	"log"

	pb "tapestry/api/proto"
	"tapestry/internal/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (n *Node) Route(ctx context.Context, req *pb.RouteRequest) (*pb.RouteResponse, error) {
	log.Printf("Node %v received Route request for ID %v at level %d", n.ID, req.ID, req.Level)

	if int(req.Level) == util.DIGITS {
		log.Printf("Node %v is the root (perfect match). Terminating route.", n.ID)
		return &pb.RouteResponse{
			ID:   n.ID,
			Port: int32(n.Port),
		}, nil
	}

	nextDigit := util.GetDigit(req.ID, int(req.Level))

	for i := 0; i < util.RADIX; i++ {
		currentDigit := (nextDigit + uint64(i)) % util.RADIX
		nextHopPort := n.RoutingTable[req.Level][currentDigit]

		if nextHopPort == -1 || nextHopPort == n.Port {
			continue
		}

		log.Printf("Forwarding request to next hop: Port %d (at level %d)", nextHopPort, req.Level)
		client, conn, err := GetNodeClient(nextHopPort)
		if err != nil {
			log.Printf("Error connecting to next hop %d: %v. Trying next surrogate.", nextHopPort, err)
			continue
		}
		defer conn.Close()

		res, err := client.Route(ctx, &pb.RouteRequest{
			ID:    req.ID,
			Level: req.Level + 1,
		})


		if err != nil {
			log.Printf("RPC to next hop %d failed: %v. Trying next surrogate.", nextHopPort, err)
			continue
		}

		return res, nil
	}

	log.Printf("Node %v could not forward, terminating as closest root.", n.ID)
	return &pb.RouteResponse{
		ID:   n.ID,
		Port: int32(n.Port),
	}, nil
}

func GetNodeClient(port int) (pb.NodeServiceClient, *grpc.ClientConn, error) {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	client := pb.NewNodeServiceClient(conn)
	return client, conn, nil
}