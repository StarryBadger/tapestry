package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"tapestry/internal/id"
)

// Status represents the full state of a node for frontend display.
type Status struct {
	ID           string     `json:"id"`
	Port         int        `json:"port"`
	RoutingTable [][]string `json:"routingTable"` // Simplified for JSON
	Backpointers []string   `json:"backpointers"`
	Objects      []Object   `json:"objects"`
}

// allowCORS middleware
func allowCORS(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.ServeHTTP(w, r)
	}
}

func (n *Node) StartHttpServer(port int) {
	log.Printf("Node %s HTTP server on :%d", n.ID, port)

	http.HandleFunc("/status", allowCORS(n.statusHandler))
	http.HandleFunc("/publish", allowCORS(n.publishHandler))
	http.HandleFunc("/find", allowCORS(n.findHandler))
	// Unpublish is not fully implemented in this version, but we leave the handler
	http.HandleFunc("/unpublish", allowCORS(n.unpublishHandler))

	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (n *Node) statusHandler(w http.ResponseWriter, r *http.Request) {
	// 1. Snapshot Routing Table
	n.Table.lock.RLock()
	// Create a simplified view: 40 levels, just showing the first neighbor in each slot
	// If we showed 40x16 it would be too big for the frontend probably.
	// Let's condense it. The frontend expects [][]int. We changed it to [][]string for Hex IDs.
	// We'll just dump the first 10 levels.
	rtDisplay := [][]string{}
	
	for i := 0; i < 10 && i < id.DIGITS; i++ {
		row := make([]string, id.RADIX)
		for j := 0; j < id.RADIX; j++ {
			if len(n.Table.rows[i][j]) > 0 {
				// Show ID prefix
				row[j] = n.Table.rows[i][j][0].ID.String()[:4] + "..."
			} else {
				row[j] = "."
			}
		}
		rtDisplay = append(rtDisplay, row)
	}
	n.Table.lock.RUnlock()

	// 2. Snapshot Backpointers
	n.bpLock.RLock()
	bpDisplay := []string{}
	for k := range n.Backpointers {
		bpDisplay = append(bpDisplay, k[:8]+"...")
	}
	n.bpLock.RUnlock()

	// 3. Snapshot Objects
	n.objLock.RLock()
	objDisplay := []Object{}
	for _, o := range n.LocalObjects {
		objDisplay = append(objDisplay, o)
	}
	n.objLock.RUnlock()

	status := Status{
		ID:           n.ID.String(),
		Port:         n.Port,
		RoutingTable: rtDisplay,
		Backpointers: bpDisplay,
		Objects:      objDisplay,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (n *Node) publishHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err := n.StoreAndPublish(data["key"], data["value"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (n *Node) findHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	obj, err := n.Get(data["key"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

func (n *Node) unpublishHandler(w http.ResponseWriter, r *http.Request) {
	var data map[string]string
	json.NewDecoder(r.Body).Decode(&data)
	n.Remove(data["key"])
	w.WriteHeader(http.StatusOK)
}