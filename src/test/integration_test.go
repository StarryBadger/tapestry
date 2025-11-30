package test

import (
	"fmt"
	"sync/atomic" 
	"testing"
	"time"

	"tapestry/internal/node"
)

var globalPort int32 = 10000

func getNextPort() int {
	return int(atomic.AddInt32(&globalPort, 1))
}

func createCluster(t *testing.T, count int) []*node.Node {
	var nodes []*node.Node
	bootstrapAddr := ""
	

	for i := 0; i < count; i++ {
		port := getNextPort()
		n, err := node.NewNode(port)
		if err != nil {
			t.Fatalf("Failed to create node %d: %v", i, err)
		}

		go func() {
			if err := n.Start(); err != nil {
			}
		}()
		time.Sleep(50 * time.Millisecond)

		if i > 0 {
			success := false
			for attempt := 0; attempt < 3; attempt++ {
				if err := n.Join([]string{bootstrapAddr}); err == nil {
					success = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !success {
				t.Fatalf("Node %d failed to join %s", i, bootstrapAddr)
			}
		} else {
			bootstrapAddr = fmt.Sprintf("localhost:%d", port)
		}

		nodes = append(nodes, n)
	}
	
	time.Sleep(2 * time.Second)
	return nodes
}

func stopCluster(nodes []*node.Node) {
	for _, n := range nodes {
		n.Stop()
	}
	node.CloseAllConnections() 
	time.Sleep(100 * time.Millisecond)
}

func TestMeshConvergence(t *testing.T) {
	nodeCount := 5
	nodes := createCluster(t, nodeCount)
	defer stopCluster(nodes)

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

	t.Log("Node 0 publishing...")
	err := nodes[0].StoreAndPublish(key, data)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(1 * time.Second)

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
	nodes := createCluster(t, 5)
	defer stopCluster(nodes)

	key := "resilient-key"
	data := "cannot-kill-me"

	nodes[0].StoreAndPublish(key, data)
	time.Sleep(2 * time.Second)

	t.Log("Killing Primary Node 0...")
	nodes[0].Stop()
	
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
	defer func() {
		nodes[1].Stop()
		nodes[2].Stop()
	}()

	key := "handoff-key"
	data := "take-this"

	nodes[0].StoreAndPublish(key, data)
	time.Sleep(1 * time.Second)

	t.Log("Node 0 leaving gracefully...")
	err := nodes[0].Leave()
	if err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	time.Sleep(1 * time.Second)

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