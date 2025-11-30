package test

import (
	"fmt"
	"testing"
	"time"

	"tapestry/internal/node"
)

const (
	START_PORT = 10000
)

// --- Test Harness Helpers ---

func createCluster(t *testing.T, count int) []*node.Node {
	var nodes []*node.Node
	bootstrapAddr := ""

	for i := 0; i < count; i++ {
		port := START_PORT + i
		n, err := node.NewNode(port)
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}

		// Start Server
		go func() {
			if err := n.Start(); err != nil {
				// Expected error on Stop()
			}
		}()
		
		// Give server a moment to bind
		time.Sleep(100 * time.Millisecond)

		// Join
		if i > 0 {
			if err := n.Join(bootstrapAddr); err != nil {
				t.Fatalf("Node %d failed to join: %v", i, err)
			}
		} else {
			bootstrapAddr = fmt.Sprintf("localhost:%d", port)
		}

		nodes = append(nodes, n)
	}
	
	// Wait for backpointer optimization and table population
	time.Sleep(2 * time.Second)
	return nodes
}

func stopCluster(nodes []*node.Node) {
	for _, n := range nodes {
		n.Stop()
	}
	time.Sleep(100 * time.Millisecond) // Cleanup wait
}

// --- Tests ---

func TestMeshConvergence(t *testing.T) {
	nodeCount := 5
	nodes := createCluster(t, nodeCount)
	defer stopCluster(nodes)

	// Check if Routing Tables are populated
	// In a small network, tables might be sparse, but shouldn't be empty for everyone.
	totalNeighbors := 0
	for i, n := range nodes {
		size := n.Table.Size()
		t.Logf("Node %d Routing Table Size: %d", i, size)
		totalNeighbors += size
	}

	if totalNeighbors == 0 {
		t.Errorf("Network failed to converge: Total routing table size is 0")
	}
}

func TestBasicDOLR(t *testing.T) {
	nodes := createCluster(t, 3)
	defer stopCluster(nodes)

	key := "my-secret"
	data := "is-secure"

	// Node 0 publishes
	t.Log("Node 0 publishing...")
	err := nodes[0].StoreAndPublish(key, data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for pointers to propagate
	time.Sleep(1 * time.Second)

	// Node 2 Lookups
	t.Log("Node 2 fetching...")
	obj, err := nodes[2].Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if obj.Data != data {
		t.Errorf("Data mismatch. Expected %s, got %s", data, obj.Data)
	}
}

func TestReplicationFailover(t *testing.T) {
	// Need enough nodes for 3 replicas (1 Primary + 2 Backups) + 1 Client
	nodes := createCluster(t, 5)
	defer stopCluster(nodes)

	key := "resilient-key"
	data := "cannot-kill-me"

	// Node 0 publishes
	nodes[0].StoreAndPublish(key, data)
	time.Sleep(2 * time.Second) // Wait for replication RPCs

	// Kill Node 0 (Primary)
	t.Log("Killing Primary Node 0...")
	nodes[0].Stop()
	
	// Node 4 tries to find it
	// It should fail to contact Node 0, but succeed via backups
	t.Log("Node 4 attempting fetch...")
	obj, err := nodes[4].Get(key)
	if err != nil {
		t.Fatalf("Failover failed. Could not retrieve object: %v", err)
	}

	if obj.Data != data {
		t.Errorf("Got wrong data: %s", obj.Data)
	}
	t.Log("Failover successful!")
}

func TestGracefulExitHandoff(t *testing.T) {
	nodes := createCluster(t, 3)
	// We handle cleanup manually to avoid double-stopping the leaver
	defer func() {
		nodes[1].Stop()
		nodes[2].Stop()
	}()

	key := "handoff-key"
	data := "take-this"

	// Node 0 has data
	nodes[0].StoreAndPublish(key, data)
	time.Sleep(1 * time.Second)

	// Node 0 leaves gracefully
	t.Log("Node 0 leaving gracefully...")
	err := nodes[0].Leave()
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	// Wait for handoff RPCs
	time.Sleep(1 * time.Second)

	// Node 1 tries to find data
	// If handoff worked, Node 0 moved data to Node 1 or 2
	t.Log("Node 1 attempting fetch...")
	obj, err := nodes[1].Get(key)
	if err != nil {
		t.Fatalf("Handoff failed. Data lost: %v", err)
	}

	if obj.Data != data {
		t.Errorf("Corrupted data: %s", obj.Data)
	}
	t.Log("Graceful handoff successful!")
}