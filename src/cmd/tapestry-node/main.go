package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "tapestry/api/proto"
	"tapestry/internal/node"
)

func main() {
	nodeA, err := node.NewNode(8000)
	if err != nil { log.Fatalf("Failed to create nodeA: %v", err) }
	nodeB, err := node.NewNode(8001)
	if err != nil { log.Fatalf("Failed to create nodeB: %v", err) }
	nodeC, err := node.NewNode(8002)
	if err != nil { log.Fatalf("Failed to create nodeC: %v", err) }

	nodeA.ID = 0
	nodeB.ID = 1
	nodeC.ID = 2
	log.Printf("Node A initialized with ID %v on Port %d", nodeA.ID, nodeA.Port)
	log.Printf("Node B initialized with ID %v on Port %d", nodeB.ID, nodeB.Port)
	log.Printf("Node C initialized with ID %v on Port %d", nodeC.ID, nodeC.Port)

	log.Println("Configuring routing tables...")
	nodeA.RoutingTable[0][1] = nodeB.Port 
	nodeA.RoutingTable[0][2] = nodeC.Port 

	nodeB.RoutingTable[0][0] = nodeA.Port 
	nodeB.RoutingTable[0][2] = nodeC.Port 

	nodeC.RoutingTable[0][0] = nodeA.Port 
	nodeC.RoutingTable[0][1] = nodeB.Port 

	log.Println("Starting all nodes...")
	go nodeA.Start()
	go nodeB.Start()
	go nodeC.Start()
	time.Sleep(2 * time.Second)

	var targetID uint64 = 6
	testRoute(nodeA, targetID)

	log.Println("Network is running. Press Ctrl+C to shut down.")
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChan

	log.Println("Shutting down all nodes...")
	nodeA.Stop()
	nodeB.Stop()
	nodeC.Stop()
}

func testRoute(startNode *node.Node, targetId uint64) {
	log.Printf("--- [Test Client] Asking Node %v to find root for %v ---", startNode.ID, targetId)

	client, conn, err := node.GetNodeClient(startNode.Port)
	if err != nil {
		log.Fatalf("[Test Client] Failed to connect to start node: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := client.Route(ctx, &pb.RouteRequest{
		ID:    targetId,
		Level: 0,
	})

	if err != nil {
		log.Printf("[Test Client] Route failed: %v", err)
	} else {
		log.Printf("[Test Client] SUCCESS! Route completed. Final node is %v at port %d", res.ID, res.Port)
	}
}