package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "tapestry/api/proto"
	"tapestry/internal/id"
	"tapestry/internal/node"
)

const START_PORT = 20000

// JSONReport holds raw data for Python analysis
type JSONReport struct {
	Mode     string `json:"mode"`
	Nodes    int    `json:"nodes"`
	Requests int    `json:"requests"`
	Workers  int    `json:"workers"`

	HopCounts []int   `json:"hop_counts"`
	AvgHops   float64 `json:"avg_hops"`

	NodeStorage map[string]int `json:"node_storage"`
	CV          float64        `json:"cv"`
	Jain        float64        `json:"jain"` // NEW
	Gini        float64        `json:"gini"` // NEW

	PutLatencies []float64 `json:"put_latencies"`
	GetLatencies []float64 `json:"get_latencies"`
	Throughput   float64   `json:"throughput"`

	ChurnSuccess int `json:"churn_success"`
	ChurnTotal   int `json:"churn_total"`

	ReplicationDelays []float64 `json:"replication_delays"`
}

type Benchmarker struct {
	nodes []*node.Node
}

var resultFile *os.File

func main() {
	mode := flag.String("mode", "hops", "Mode: hops, load, perf, churn, repl")
	nodeCount := flag.Int("nodes", 20, "Number of nodes")
	reqCount := flag.Int("requests", 1000, "Number of requests")
	concurrency := flag.Int("workers", 10, "Workers")
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

	reportData := JSONReport{
		Mode: *mode, Nodes: *nodeCount, Requests: *reqCount, Workers: *concurrency,
	}

	switch *mode {
	case "hops":
		reportData.HopCounts, reportData.AvgHops = bm.measureHops(*reqCount)
	case "load":
		// Updated to return multiple metrics
		reportData.NodeStorage, reportData.CV, reportData.Jain, reportData.Gini = bm.measureLoadBalance(*reqCount)
	case "perf":
		reportData.PutLatencies, reportData.GetLatencies, reportData.Throughput = bm.measurePerformance(*reqCount, *concurrency)
	case "churn":
		reportData.ChurnSuccess, reportData.ChurnTotal = bm.measureChurn(*reqCount, *concurrency)
	case "repl":
		reportData.ReplicationDelays = bm.measureReplication(*reqCount)
	}

	file, _ := os.Create("results.json")
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.Encode(reportData)

	report("=== End Report ===")
	fmt.Println("\n\nDone! Results saved to 'benchmark_results.txt'")
}

func report(msg string) {
	log.Println(msg)
	if resultFile != nil {
		resultFile.WriteString(msg + "\n")
	}
}

// --- Cluster Setup ---
func setupCluster(count int) *Benchmarker {
	bm := &Benchmarker{}
	var activeAddrs []string

	for i := 0; i < count; i++ {
		port := START_PORT + i
		n, err := node.NewNode(port)
		if err != nil {
			log.Fatalf("Failed to create node %d: %v", i, err)
		}

		go n.Start()
		time.Sleep(20 * time.Millisecond)

		if i > 0 {
			if err := n.Join(activeAddrs); err != nil {
				log.Printf("Node %d join failed: %v", i, err)
			}
		}

		addr := fmt.Sprintf("localhost:%d", port)
		activeAddrs = append(activeAddrs, addr)
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

// --- Metrics ---

func (bm *Benchmarker) measureHops(samples int) ([]int, float64) {
	report("--- Hop Count Results ---")
	var hopsList []int
	total := 0
	for i := 0; i < samples; i++ {
		src := bm.nodes[rand.Intn(len(bm.nodes))]
		targetID := id.NewRandomID()
		h := traceRoute(src, targetID)
		hopsList = append(hopsList, h)
		total += h
	}
	avg := float64(total) / float64(samples)
	report(fmt.Sprintf("Average Hops: %.2f", avg))
	return hopsList, avg
}

func traceRoute(startNode *node.Node, targetID id.ID) int {
	hops := 0
	currentAddr := startNode.Address
	currentID := startNode.ID
	prevPrefixLen := id.SharedPrefixLength(currentID, targetID)

	for hops < 20 {
		client, err := node.GetClient(currentAddr)
		if err != nil {
			return hops
		}
		req := &pb.RouteRequest{TargetId: &pb.NodeID{Bytes: targetID.Bytes()}}
		resp, err := client.GetNextHop(context.Background(), req)
		client.Close()
		if err != nil || resp.IsRoot || resp.NextHop.Address == currentAddr {
			return hops
		}

		var nextID id.ID
		copy(nextID[:], resp.NextHop.Id.Bytes)
		currPrefixLen := id.SharedPrefixLength(nextID, targetID)

		if currPrefixLen < prevPrefixLen {
			// violation
		}
		prevPrefixLen = currPrefixLen
		currentAddr = resp.NextHop.Address
		hops++
	}
	return hops
}

// UPDATED: measureLoadBalance calculates CV, Jain's Index, and Gini Coefficient
func (bm *Benchmarker) measureLoadBalance(objects int) (map[string]int, float64, float64, float64) {
	report("--- Load Balance Results ---")
	for i := 0; i < objects; i++ {
		src := bm.nodes[rand.Intn(len(bm.nodes))]
		key := fmt.Sprintf("obj-%d", i)
		go src.StoreAndPublish(key, "data")
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	time.Sleep(5 * time.Second)

	storage := make(map[string]int)
	counts := []float64{}
	for _, n := range bm.nodes {
		c := float64(n.GetLocalObjectCount())
		storage[n.ID.String()] = int(c)
		counts = append(counts, c)
	}

	// 1. Calculate CV
	sum := 0.0
	sumSq := 0.0
	for _, c := range counts {
		sum += c
		sumSq += c * c
	}
	mean := sum / float64(len(counts))
	variance := 0.0
	for _, c := range counts {
		variance += math.Pow(c-mean, 2)
	}
	stdDev := math.Sqrt(variance / float64(len(counts)))

	cv := 0.0
	if mean > 0 {
		cv = stdDev / mean
	}

	// 2. Calculate Jain's Fairness Index
	// J = (sum(x_i))^2 / (n * sum(x_i^2))
	jain := 0.0
	if sumSq > 0 {
		jain = (sum * sum) / (float64(len(counts)) * sumSq)
	}

	// 3. Calculate Gini Coefficient
	// G = (2 * sum(i * x_i)) / (n * sum(x_i)) - (n + 1) / n
	// Requires sorted array
	sortedCounts := make([]float64, len(counts))
	copy(sortedCounts, counts)
	sort.Float64s(sortedCounts)

	giniNumerator := 0.0
	n := float64(len(sortedCounts))
	for i, x := range sortedCounts {
		// i is 0-indexed, formula uses 1-based rank
		giniNumerator += (float64(i + 1) * x)
	}

	gini := 0.0
	if sum > 0 {
		gini = (2*giniNumerator)/(n*sum) - (n+1)/n
	}

	report(fmt.Sprintf("Coefficient of Variation: %.4f", cv))
	report(fmt.Sprintf("Jain's Fairness Index:  %.4f", jain))
	report(fmt.Sprintf("Gini Coefficient:       %.4f", gini))

	return storage, cv, jain, gini
}

func (bm *Benchmarker) measurePerformance(requests int, workers int) ([]float64, []float64, float64) {
	report("--- Performance Results ---")
	var wg sync.WaitGroup
	start := time.Now()
	putLats := make(chan float64, requests)
	getLats := make(chan float64, requests)
	workerLoad := requests / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < workerLoad; i++ {
				n := bm.nodes[rand.Intn(len(bm.nodes))]
				key := fmt.Sprintf("perf-%d-%d", rand.Int(), i)
				t0 := time.Now()
				if i%5 == 0 {
					n.StoreAndPublish(key, "data")
					putLats <- float64(time.Since(t0).Microseconds()) / 1000.0
				} else {
					n.Get("some-key")
					getLats <- float64(time.Since(t0).Microseconds()) / 1000.0
				}
			}
		}()
	}
	wg.Wait()
	close(putLats)
	close(getLats)

	var puts, gets []float64
	for l := range putLats {
		puts = append(puts, l)
	}
	for l := range getLats {
		gets = append(gets, l)
	}

	ops := float64(requests) / time.Since(start).Seconds()
	report(fmt.Sprintf("Throughput: %.2f Ops/Sec", ops))
	return puts, gets, ops
}

func (bm *Benchmarker) measureChurn(requests int, workers int) (int, int) {
	report("--- Churn Resilience Results ---")

	liveStatus := make([]int32, len(bm.nodes)) // 0=alive, 1=dead

	stopChaos := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopChaos:
				return
			default:
				time.Sleep(2 * time.Second)
				aliveCount := 0
				for _, s := range liveStatus {
					if s == 0 {
						aliveCount++
					}
				}

				if aliveCount > 5 {
					var victimIdx int
					for {
						victimIdx = 1 + rand.Intn(len(bm.nodes)-1)
						if atomic.LoadInt32(&liveStatus[victimIdx]) == 0 {
							break
						}
					}

					victim := bm.nodes[victimIdx]
					log.Printf("[CHAOS] Killing Node %d (%s)", victimIdx, victim.ID)

					atomic.StoreInt32(&liveStatus[victimIdx], 1)
					victim.Leave()
				}
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
				var n *node.Node
				attempts := 0
				for {
					idx := rand.Intn(len(bm.nodes))
					if atomic.LoadInt32(&liveStatus[idx]) == 0 {
						n = bm.nodes[idx]
						break
					}
					attempts++
					if attempts > 100 {
						n = bm.nodes[0]
						break
					}
				}

				key := "resilient-data"
				if i == 0 {
					bm.nodes[0].StoreAndPublish(key, "data")
				}

				_, err := n.Get(key)
				if err == nil {
					atomic.AddInt64(&successCount, 1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(stopChaos)

	rate := float64(successCount) / float64(requests) * 100.0
	report(fmt.Sprintf("Success Rate: %.2f%% (%d/%d)", rate, successCount, requests))
	report(fmt.Sprintf("Time: %v", time.Since(start)))

	return int(successCount), requests
}

func (bm *Benchmarker) measureReplication(samples int) []float64 {
	report("--- Replication Delay Results ---")
	var delays []float64
	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("repl-%d", i)
		primary := bm.nodes[0]
		t0 := time.Now()
		primary.StoreAndPublish(key, "data")
		finder := bm.nodes[len(bm.nodes)-1]
		for {
			if _, err := finder.Get(key); err == nil {
				break
			}
			time.Sleep(1 * time.Millisecond)
			if time.Since(t0) > 1*time.Second {
				break
			}
		}
		delays = append(delays, float64(time.Since(t0).Microseconds())/1000.0)
	}
	return delays
}