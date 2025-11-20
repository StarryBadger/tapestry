package main

import (
	"bufio"
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
	portPtr := flag.Int("port", 0, "Port for the node to listen on (0 for random).")
	bootPtr := flag.Int("boot", 0, "Bootstrap node port to connect to (0 for a new network).")
	flag.Parse()
	n, err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	go func() {
		if err := n.Start(); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	log.Printf("Attempting to join network via bootstrap port %d...", *bootPtr)
	err = n.Insert(*bootPtr)
	if err != nil {
		log.Fatalf("Failed to join network: %v", err)
	}
	log.Println("Successfully joined network.")

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\nNode is running. Enter 'exit' or press Ctrl+C to leave.")
	scanner := bufio.NewScanner(os.Stdin)
	inputChan := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			inputChan <- scanner.Text()
		}
	}()

	select {
	case <-shutdownChan:
		log.Println("Shutdown signal received.")
	case input := <-inputChan:
		if input == "exit" {
			log.Println("'exit' command received.")
		}
	}

	n.GracefulLeave()
	time.Sleep(1 * time.Second) 
	n.Stop()
	log.Println("Node stopped.")
}