package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
	"tapestry/internal/node"
)

const START_PORT = 20000

type Benchmarker struct {
	nodes []*node.Node
}

var resultFile *os.File

func main() {
	mode := flag.String("mode", "hops", "Mode: hops, load, perf, churn")
	nodeCount := flag.Int("nodes", 20, "Number of nodes in cluster")
	reqCount := flag.Int("requests", 1000, "Number of requests/objects")
	concurrency := flag.Int("workers", 10, "Number of concurrent workers (perf mode)")
	flag.Parse()

	var err error
	resultFile, err = os.Create("benchmark_results.txt")
	if err != nil {
		log.Fatalf("Failed to create result file: %v", err)
	}
	defer resultFile.Close()

	report(fmt.Sprintf("=== Tapestry Benchmark Report [%s] ===", time.Now().Format(time.RFC3339)))
	report(fmt.Sprintf("Mode: %s | Nodes: %d | Requests: %d", *mode, *nodeCount, *reqCount))

	bm := setupCluster(*nodeCount)
	defer bm.teardown()

	switch *mode {
	case "hops":
		bm.measureHops(*reqCount)
	case "load":
		bm.measureLoadBalance(*reqCount)
	case "perf":
		bm.measurePerformance(*reqCount, *concurrency)
	case "churn":
		bm.measureChurn(*reqCount, *concurrency)
	default:
		log.Fatal("Unknown mode")
	}
	
	report("=== End Report ===")
	fmt.Println("\n\nDone! Results saved to 'benchmark_results.txt'")
}

func report(msg string) {
	log.Println(msg) 
	if resultFile != nil {
		resultFile.WriteString(msg + "\n")
	}
}

// --- Cluster Management ---

func setupCluster(count int) *Benchmarker {
	bm := &Benchmarker{}
	bootstrapAddr := ""

	for i := 0; i < count; i++ {
		port := START_PORT + i
		n, err := node.NewNode(port)
		if err != nil {
			log.Fatalf("Failed to create node %d: %v", i, err)
		}

		go n.Start()
		time.Sleep(50 * time.Millisecond) 

		if i > 0 {
			if err := n.Join([]string{bootstrapAddr}); err != nil {
				log.Printf("Node %d join failed: %v", i, err)
			}
		} else {
			bootstrapAddr = fmt.Sprintf("localhost:%d", port)
		}
		bm.nodes = append(bm.nodes, n)
	}
	
	log.Println("Cluster stable. Waiting for table population...")
	time.Sleep(5 * time.Second) 
	return bm
}

func (bm *Benchmarker) teardown() {
	log.Println("Tearing down cluster...")
	for _, n := range bm.nodes {
		n.Stop()
	}
	node.CloseAllConnections()
}

// --- Set 3: Hop Count Distribution ---

func (bm *Benchmarker) measureHops(samples int) {
	report("--- Hop Count Results ---")
	
	totalHops := 0
	maxHops := 0
	monotonicityViolations := 0

	for i := 0; i < samples; i++ {
		srcIdx := rand.Intn(len(bm.nodes))
		src := bm.nodes[srcIdx]
		targetID := id.NewRandomID()

		hops, violated := traceRoute(src, targetID)
		
		totalHops += hops
		if hops > maxHops {
			maxHops = hops
		}
		if violated {
			monotonicityViolations++
		}
	}

	avg := float64(totalHops) / float64(samples)
	report(fmt.Sprintf("Average Hops: %.2f", avg))
	report(fmt.Sprintf("Max Hops:     %d", maxHops))
	report(fmt.Sprintf("Ideal Hops (Log16 N): %.2f", math.Log(float64(len(bm.nodes)))/math.Log(16)))
	report(fmt.Sprintf("Monotonicity Violations: %d", monotonicityViolations))
}

func traceRoute(startNode *node.Node, targetID id.ID) (int, bool) {
	hops := 0
	currentAddr := startNode.Address
	currentID := startNode.ID
	violated := false
	prevPrefixLen := id.SharedPrefixLength(currentID, targetID)
	prevDist := id.Distance(currentID, targetID)

	for hops < 20 { 
		client, err := node.GetClient(currentAddr)
		if err != nil { return hops, false }
		
		req := &pb.RouteRequest{TargetId: &pb.NodeID{Bytes: targetID.Bytes()}}
		resp, err := client.GetNextHop(context.Background(), req)
		client.Close()
		
		if err != nil { return hops, false }
		if resp.IsRoot { return hops, violated }

		nextAddr := resp.NextHop.Address
		if nextAddr == currentAddr { return hops, violated }

		var nextID id.ID
		copy(nextID[:], resp.NextHop.Id.Bytes)
		
		currPrefixLen := id.SharedPrefixLength(nextID, targetID)
		currDist := id.Distance(nextID, targetID)

		if currPrefixLen < prevPrefixLen {
			violated = true
		} else if currPrefixLen == prevPrefixLen {
			if currDist.Cmp(prevDist) >= 0 {
				// violated = true 
			}
		}

		prevPrefixLen = currPrefixLen
		prevDist = currDist
		currentAddr = nextAddr
		hops++
	}
	return hops, true 
}

// --- Set 3: Load Balance ---

func (bm *Benchmarker) measureLoadBalance(objects int) {
	report("--- Load Balance Results ---")
	
	for i := 0; i < objects; i++ {
		src := bm.nodes[rand.Intn(len(bm.nodes))]
		key := fmt.Sprintf("bench-obj-%d", i)
		go src.StoreAndPublish(key, "payload")
		if i % 100 == 0 { time.Sleep(10 * time.Millisecond) }
	}
	
	log.Println("Waiting for propagation...")
	time.Sleep(5 * time.Second) 

	counts := make([]int, len(bm.nodes))
	totalStored := 0
	
	for i, n := range bm.nodes {
		c := n.GetLocalObjectCount()
		counts[i] = c
		totalStored += c
	}

	mean := float64(totalStored) / float64(len(bm.nodes))
	variance := 0.0
	for _, c := range counts {
		diff := float64(c) - mean
		variance += diff * diff
	}
	stdDev := math.Sqrt(variance / float64(len(bm.nodes)))

	report(fmt.Sprintf("Total Objects Stored (inc replicas): %d", totalStored))
	report(fmt.Sprintf("Mean Objects/Node: %.2f", mean))
	report(fmt.Sprintf("Std Dev: %.2f", stdDev))
	report(fmt.Sprintf("Coefficient of Variation: %.2f", stdDev/mean))
	
	if stdDev/mean < 1.0 {
		report("RESULT: Good Load Balance")
	} else {
		report("RESULT: High Variance (Hotspots detected)")
	}
}

// --- Set 4: Performance ---

func (bm *Benchmarker) measurePerformance(requests int, workers int) {
	report("--- Performance Results ---")
	
	var wg sync.WaitGroup
	start := time.Now()
	latencies := make(chan time.Duration, requests)
	workerLoad := requests / workers
	
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < workerLoad; i++ {
				nodeIdx := rand.Intn(len(bm.nodes))
				n := bm.nodes[nodeIdx]
				key := fmt.Sprintf("perf-%d-%d", rand.Int(), i)
				
				t0 := time.Now()
				if rand.Float32() < 0.2 {
					n.StoreAndPublish(key, "data")
				} else {
					n.Get("existing-key") 
				}
				latencies <- time.Since(t0)
			}
		}()
	}
	
	wg.Wait()
	close(latencies)
	totalTime := time.Since(start)
	
	var totalLat time.Duration
	count := 0
	for l := range latencies {
		totalLat += l
		count++
	}
	
	avgLat := totalLat / time.Duration(count)
	ops := float64(requests) / totalTime.Seconds()
	
	report(fmt.Sprintf("Total Time: %v", totalTime))
	report(fmt.Sprintf("Throughput: %.2f Ops/Sec", ops))
	report(fmt.Sprintf("Avg Latency: %v", avgLat))
}

// --- Set 4: Churn Resilience ---

func (bm *Benchmarker) measureChurn(requests int, workers int) {
	report("--- Churn Resilience Results ---")
	
	stopChaos := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopChaos:
				return
			default:
				time.Sleep(3 * time.Second)
				victimIdx := 1 + rand.Intn(len(bm.nodes)-1)
				victim := bm.nodes[victimIdx]
				log.Printf("[CHAOS] Killing Node %d (%s)", victimIdx, victim.ID)
				victim.Leave() 
			}
		}
	}()

	var successCount int64
	var wg sync.WaitGroup
	
	start := time.Now()
	
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < requests/workers; i++ {
				nodeIdx := rand.Intn(len(bm.nodes))
				n := bm.nodes[nodeIdx]
				key := "resilient-data"
				if i == 0 { n.StoreAndPublish(key, "data") }

				_, err := n.Get(key)
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	
	wg.Wait()
	close(stopChaos)
	
	rate := float64(successCount) / float64(requests) * 100.0
	report(fmt.Sprintf("Success Rate: %.2f%% (%d/%d)", rate, successCount, requests))
	report(fmt.Sprintf("Time: %v", time.Since(start)))
}