package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tapestry/internal/node"
)

func main() {
	portPtr := flag.Int("port", 0, "gRPC port for the node.")
	httpPortPtr := flag.Int("httpport", 0, "HTTP port for the node's API.")
	bootPtr := flag.String("boot", "", "Comma-separated list of bootstrap ports.")
	flag.Parse()

	if *portPtr == 0 {
		log.Fatal("Port is required")
	}

	n, err := node.NewNode(*portPtr)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	go func() {
		if err := n.Start(); err != nil {}
	}()

	go n.StartHttpServer(*httpPortPtr)

	if *bootPtr != "" {
		time.Sleep(1 * time.Second) 
		
		ports := strings.Split(*bootPtr, ",")
		var bootstrapAddrs []string
		for _, p := range ports {
			if p != "" {
				bootstrapAddrs = append(bootstrapAddrs, fmt.Sprintf("localhost:%s", p))
			}
		}

		if len(bootstrapAddrs) > 0 {
			if err := n.Join(bootstrapAddrs); err != nil {
				log.Fatalf("Failed to join network: %v", err)
			}
			log.Printf("Node %s successfully joined network.", n.ID)
		}
	} else {
		log.Printf("Node %s started as standalone (Genesis node).", n.ID)
	}


	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-shutdownChan:
		log.Println("Received OS interrupt. Leaving network...")
		n.Leave() 
	case <-n.ExitChan:
		log.Println("Node initiated self-shutdown via API.")
	}

	node.CloseAllConnections()
	log.Println("Process exiting.")
	os.Exit(0)
}