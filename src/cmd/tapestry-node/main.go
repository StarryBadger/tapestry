package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tapestry/internal/node"
)

func main() {
	portPtr := flag.Int("port", 0, "gRPC port for the node.")
	httpPortPtr := flag.Int("httpport", 0, "HTTP port for the node's API.")
	bootPtr := flag.Int("boot", 0, "Bootstrap node gRPC port (0 if none).")
	flag.Parse()

	if *portPtr == 0 {
		log.Fatal("Port is required")
	}

	// Create Node
	n, err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// Start gRPC server in goroutine
	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Start HTTP server in goroutine
	go n.StartHttpServer(*httpPortPtr)

	// Join the network if a bootstrap port is provided
	// Note: The manager passes just the port integer. We construct the address.
	if *bootPtr != 0 && *bootPtr != *portPtr {
		// Wait a second for the bootstrap node to be ready if we are launching simultaneously
		time.Sleep(2 * time.Second)
		
		bootstrapAddr := fmt.Sprintf("localhost:%d", *bootPtr)
		if err := n.Join(bootstrapAddr); err != nil {
			log.Fatalf("Failed to join network via %s: %v", bootstrapAddr, err)
		}
		log.Printf("Node %s successfully joined network.", n.ID)
	} else {
		log.Printf("Node %s started as standalone/bootstrap.", n.ID)
	}

	// Wait for shutdown signal
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChan

	log.Println("Shutting down...")
	n.Stop()
	log.Println("Node stopped.")
}