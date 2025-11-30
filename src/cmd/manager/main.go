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
	runningNodes      = make(map[int]*exec.Cmd)
	nodesMutex        = &sync.Mutex{}
	nextPort     int  = 8000
	bootstrapPort int = 8000

	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	clients   = make(map[*websocket.Conn]bool)
	clientsMutex = &sync.Mutex{}
	logChannel = make(chan []byte)
)

func getBootstrapList() string {
	var ports []string
	for port := range runningNodes {
		ports = append(ports, strconv.Itoa(port))
	}
	if len(ports) == 0 {
		return ""
	}
	return fmt.Sprintf("%s", join(ports, ","))
}

func join(strs []string, sep string) string {
	if len(strs) == 0 { return "" }
	if len(strs) == 1 { return strs[0] }
	res := strs[0]
	for _, s := range strs[1:] {
		res += sep + s
	}
	return res
}

func addNodeHandler(w http.ResponseWriter, r *http.Request) {
	nodesMutex.Lock()
	defer nodesMutex.Unlock()

	port := nextPort
	httpPort := port + 1000 
	
	bootList := getBootstrapList()

	cmd := exec.Command("go", "run", "tapestry/cmd/tapestry-node",
		"-port", strconv.Itoa(port),
		"-httpport", strconv.Itoa(httpPort),
		"-boot", bootList)

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	go io.Copy(os.Stdout, stdout) 
	go io.Copy(os.Stderr, stderr)
	go streamLogs(stdout)
	go streamLogs(stderr)

	if err := cmd.Start(); err != nil {
		http.Error(w, "Failed to start node", 500)
		log.Printf("Failed to start node on port %d: %v", port, err)
		return
	}

	runningNodes[port] = cmd
	
	go func(p int, c *exec.Cmd) {
		c.Wait() 
		
		nodesMutex.Lock()
		delete(runningNodes, p)
		nodesMutex.Unlock()
		
		msg := fmt.Sprintf("Node on port %d exited.", p)
		log.Println(msg)
		logChannel <- []byte(msg)
	}(port, cmd)

	nextPort++
	log.Printf("Started new node on gRPC port %d and HTTP port %d", port, httpPort)
	w.WriteHeader(http.StatusOK)
}

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

	for {
		if _, _, err := conn.NextReader(); err != nil {
			break
		}
	}

	clientsMutex.Lock()
	delete(clients, conn)
	clientsMutex.Unlock()
}

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

	fs := http.FileServer(http.Dir("./frontend"))
	http.Handle("/", fs)

	log.Println("Manager server starting on :3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatalf("Manager server failed: %v", err)
	}
}