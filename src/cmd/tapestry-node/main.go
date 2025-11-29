// tapestry/cmd/tapestry-node/main.go
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tapestry/internal/node"
)

func main() {
	// Node now takes flags to be started by the manager process
	portPtr := flag.Int("port", 0, "gRPC port for the node.")
	httpPortPtr := flag.Int("httpport", 0, "HTTP port for the node's API.")
	bootPtr := flag.Int("boot", 0, "Bootstrap node gRPC port.")
	flag.Parse()

	n, err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// Start gRPC server
	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Start HTTP server for frontend API
	go n.StartHttpServer(*httpPortPtr)

	// Join the network
	if err := n.Insert(*bootPtr); err != nil {
		log.Fatalf("Failed to join network: %v", err)
	}
	log.Printf("Node %v successfully joined network.", n.ID)

	// Republishing goroutine
	go func() {
		for {
			time.Sleep(10 * time.Second)
			n.ObjectsLock.RLock()
			objectsToPublish := make([]node.Object, 0, len(n.Objects))
			for _, obj := range n.Objects {
				objectsToPublish = append(objectsToPublish, obj)
			}
			n.ObjectsLock.RUnlock()

			for _, obj := range objectsToPublish {
				n.Publish(obj)
			}
		}
	}()

	// Wait for shutdown signal
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChan

	n.GracefulLeave()
	time.Sleep(1 * time.Second)
	n.Stop()
	log.Println("Node stopped.")
}