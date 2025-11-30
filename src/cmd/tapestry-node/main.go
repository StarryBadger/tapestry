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

	n, err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	go func() {
		if err := n.Start(); err != nil {
			// This might error when n.Stop() is called, which is expected
			// log.Printf("gRPC server stopped: %v", err)
		}
	}()

	go n.StartHttpServer(*httpPortPtr)

	if *bootPtr != 0 && *bootPtr != *portPtr {
		time.Sleep(2 * time.Second)
		bootstrapAddr := fmt.Sprintf("localhost:%d", *bootPtr)
		if err := n.Join(bootstrapAddr); err != nil {
			log.Fatalf("Failed to join network via %s: %v", bootstrapAddr, err)
		}
		log.Printf("Node %s successfully joined network.", n.ID)
	} else {
		log.Printf("Node %s started as standalone/bootstrap.", n.ID)
	}

	// Wait for shutdown signal OR internal exit
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-shutdownChan:
		log.Println("Received OS interrupt. Leaving network...")
		n.Leave() // Trigger graceful leave on Ctrl+C
	case <-n.ExitChan:
		log.Println("Node initiated self-shutdown via API.")
	}
	node.CloseAllConnections()

	log.Println("Process exiting.")
	os.Exit(0)
}