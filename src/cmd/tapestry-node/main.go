package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "tapestry/api/proto"
	"tapestry/internal/node"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	portPtr := flag.Int("port", 0, "Port to listen on")
	flag.Parse()
	fmt.Printf("Starting node on port %d...\n", *portPtr)

	n , err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("Failed to start node: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
	testPing(n.Port)

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	<-shutdownChan

	log.Println("Shutdown signal received, stopping node...")
	n.Stop()
	log.Println("Node stopped.")

}

func testPing(port int) {
	log.Println("--- [Test Client] Starting Ping Test ---")
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Test Client] Could not connect to %s: %v", addr, err)
		return
	}
	defer conn.Close()

	client := pb.NewNodeServiceClient(conn)

	log.Printf("[Test Client] Sending Ping to %s...", addr)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel() 

	_, err = client.Ping(ctx, &pb.Nothing{})

	if err != nil {
		log.Printf("[Test Client] Received an error from Ping: %v", err)
	} else {
		log.Println("[Test Client] Received successful reply for Ping!")
	}
}

  