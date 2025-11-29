// tapestry/internal/node/http_server.go
package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"tapestry/internal/util" // Make sure util is imported
)

// Status represents the full state of a node for frontend display.
type Status struct {
	ID           string   `json:"id"`
	Port         int      `json:"port"`
	RoutingTable [][]int  `json:"routingTable"`
	Backpointers []int    `json:"backpointers"`
	Objects      []Object `json:"objects"`
}

// --- THIS IS THE NEW MIDDLEWARE FUNCTION ---
// allowCORS is a middleware that adds the necessary CORS headers to a response.
func allowCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set the header to allow requests from any origin.
		// For production, you might want to restrict this to a specific domain.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Browsers will sometimes send an "OPTIONS" request first (a "preflight" check).
		// We need to handle this by just returning a success status.
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// If it's not an OPTIONS request, just call the original handler.
		h.ServeHTTP(w, r)
	}
}

// StartHttpServer starts a new HTTP server for this node.
func (n *Node) StartHttpServer(port int) {
	log.Printf("Node %v starting HTTP server on port %d", util.HashToString(n.ID), port)

	// --- APPLY THE MIDDLEWARE TO EACH HANDLER ---
	http.HandleFunc("/status", allowCORS(n.statusHandler))
	http.HandleFunc("/publish", allowCORS(n.publishHandler))
	http.HandleFunc("/find", allowCORS(n.findHandler))
	http.HandleFunc("/unpublish", allowCORS(n.unpublishHandler))

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatalf("Node HTTP server failed: %v", err)
	}
}

// statusHandler returns the node's current status as JSON.
func (n *Node) statusHandler(w http.ResponseWriter, r *http.Request) {
	n.rtLock.RLock()
	n.bpLock.RLock()
	n.ObjectsLock.RLock()
	defer n.rtLock.RUnlock()
	defer n.bpLock.RUnlock()
	defer n.ObjectsLock.RUnlock()

	bpSlice := []int{}
	for p := range n.Backpointers.Set {
		bpSlice = append(bpSlice, p)
	}

	objSlice := []Object{}
	for _, o := range n.Objects {
		objSlice = append(objSlice, o)
	}

	status := Status{
		ID:           util.HashToString(n.ID), // Use HashToString for readability
		Port:         n.Port,
		RoutingTable: n.RoutingTable,
		Backpointers: bpSlice,
		Objects:      objSlice,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// Handlers for DHT operations (no changes needed here)

func (n *Node) publishHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	obj := Object{Name: data["key"], Content: data["value"]}
	n.AddObject(obj)
	n.Publish(obj)
	w.WriteHeader(http.StatusOK)
}

func (n *Node) findHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	obj, err := n.FindObject(data["key"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

func (n *Node) unpublishHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n.UnPublish(data["key"])
	w.WriteHeader(http.StatusOK)
}