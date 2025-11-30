package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	// Safely manage our list of running node processes
	runningNodes      = make(map[int]*exec.Cmd)
	nodesMutex        = &sync.Mutex{}
	nextPort     int  = 8000
	bootstrapPort int = 8000

	// WebSocket connections
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // Allow all connections
	}
	clients   = make(map[*websocket.Conn]bool)
	clientsMutex = &sync.Mutex{}
	logChannel = make(chan []byte)
)

// addNodeHandler starts a new tapestry-node process.
func addNodeHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()

	port := nextPort
	httpPort := port + 1000 // Separate port for the node's own HTTP API
	boot := 0
	if port != bootstrapPort {
		boot = bootstrapPort
	}

	// Command to execute the tapestry-node binary
	cmd := exec.Command("go", "run", "tapestry/cmd/tapestry-node",
		"-port", strconv.Itoa(port),
		"-httpport", strconv.Itoa(httpPort),
		"-boot", strconv.Itoa(boot))

	// Capture the output of the node process to stream it as logs
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	go io.Copy(os.Stdout, stdout) // Also print to manager's console
	go io.Copy(os.Stderr, stderr)
	go streamLogs(stdout)
	go streamLogs(stderr)

	if err := cmd.Start(); err != nil {
		http.Error(w, "Failed to start node", 500)
		log.Printf("Failed to start node on port %d: %v", port, err)
		return
	}

	runningNodes[port] = cmd
	
	// --- FIX: Monitor Process Exit ---
	go func(p int, c *exec.Cmd) {
		c.Wait() // Blocks until the process exits
		
		nodesMutex.Lock()
		delete(runningNodes, p)
		nodesMutex.Unlock()
		
		msg := fmt.Sprintf("Node on port %d exited.", p)
		log.Println(msg)
		logChannel <- []byte(msg)
	}(port, cmd)
	// --------------------------------

	nextPort++
	log.Printf("Started new node on gRPC port %d and HTTP port %d", port, httpPort)
	w.WriteHeader(http.StatusOK)
}

// getNodesHandler returns a list of all running nodes.
func getNodesHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()

	type NodeInfo struct {
		Port     int `json:"port"`
		HttpPort int `json:"httpPort"`
	}
	var nodes []NodeInfo
	for port := range runningNodes {
		nodes = append(nodes, NodeInfo{Port: port, HttpPort: port + 1000})
	}
	json.NewEncoder(w).Encode(nodes)
}

// logStreamerHandler handles WebSocket connections for live logs.
func logStreamerHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	clientsMutex.Lock()
	clients[conn] = true
	clientsMutex.Unlock()

	// Keep the connection open
	for {
		if _, _, err := conn.NextReader(); err != nil {
			break
		}
	}

	clientsMutex.Lock()
	delete(clients, conn)
	clientsMutex.Unlock()
}

// streamLogs reads from a node's output and broadcasts to all WebSocket clients.
func streamLogs(reader io.Reader) {
	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if err != nil {
			return
		}
		logChannel <- buf[:n]
	}
}

// broadcastLogs sends messages from the logChannel to all connected clients.
func broadcastLogs() {
	for msg := range logChannel {
		clientsMutex.Lock()
		for client := range clients {
			if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
				client.Close()
				delete(clients, client)
			}
		}
		clientsMutex.Unlock()
	}
}

func main() {
	go broadcastLogs()

	http.HandleFunc("/add-node", addNodeHandler)
	http.HandleFunc("/nodes", getNodesHandler)
	http.HandleFunc("/logs", logStreamerHandler)

	// Serve the frontend files
	fs := http.FileServer(http.Dir("./frontend"))
	http.Handle("/", fs)

	log.Println("Manager server starting on :3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatalf("Manager server failed: %v", err)
	}
}