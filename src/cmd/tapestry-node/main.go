package main

import (
	"bufio"
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

func RepublishObjects(n *node.Node) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		log.Println("[RE-PUBLISH] Starting periodic republish cycle...")
		n.ObjectsLock.RLock()
		objectsToPublish := make([]node.Object, 0, len(n.Objects))
		for _, obj := range n.Objects {
			objectsToPublish = append(objectsToPublish, obj)
		}
		n.ObjectsLock.RUnlock()

		for _, obj := range objectsToPublish {
			err := n.Publish(obj)
			if err != nil {
				log.Printf("[RE-PUBLISH ERROR] for object '%s': %v", obj.Name, err)
			}
		}
	}
}

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

	go RepublishObjects(n)

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1. Add/Publish Object")
		fmt.Println("2. Find Object")
		fmt.Println("3. Unpublish Object")
		fmt.Println("4. Exit")
		fmt.Print("Enter choice: ")

		inputChan := make(chan string, 1)
		go func() {
			if scanner.Scan() {
				inputChan <- scanner.Text()
			}
		}()

		select {
		case <-shutdownChan:
			goto shutdown
		case choice := <-inputChan:
			switch strings.TrimSpace(choice) {
			case "1":
				fmt.Print("Enter object name: ")
				scanner.Scan()
				name := scanner.Text()
				fmt.Print("Enter object content: ")
				scanner.Scan()
				content := scanner.Text()
				obj := node.Object{Name: name, Content: content}
				err := n.AddObject(obj)
				if err != nil {
					fmt.Printf("Error adding object: %v\n", err)
				} else {
					n.Publish(obj)
					fmt.Println("Object added, replicated, and published!")
				}
			case "2":
				fmt.Print("Enter object name: ")
				scanner.Scan()
				name := scanner.Text()
				obj, err := n.FindObject(name)
				if err != nil {
					fmt.Printf("Error finding object: %v\n", err)
				} else {
					fmt.Printf("Object found! Name: %s, Content: %s\n", obj.Name, obj.Content)
				}
			case "3":
				fmt.Print("Enter object name: ")
				scanner.Scan()
				name := scanner.Text()
				err := n.UnPublish(name)
				if err != nil {
					fmt.Printf("Error unpublishing object: %v\n", err)
				} else {
					fmt.Println("Object unpublished successfully!")
				}
			case "4":
				goto shutdown
			default:
				fmt.Println("Invalid choice. Try again.")
			}
		}
	}

shutdown:
	log.Println("Shutdown initiated...")
	n.GracefulLeave()
	time.Sleep(1 * time.Second)
	n.Stop()
	log.Println("Node stopped.")
}