package node

import (
	"log"
	"time"
)

const (
	MAINTENANCE_INTERVAL = 20 * time.Second
	POINTER_TIMEOUT      = 120 * time.Second
	REPUBLISH_INTERVAL   = 60 * time.Second
)

func (n *Node) StartMaintenanceLoop() {
	ticker := time.NewTicker(MAINTENANCE_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.runKeepAlives()
			n.runPointerGC()
		}
	}
}

func (n *Node) StartRepublishLoop() {
	ticker := time.NewTicker(REPUBLISH_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.republishObjects()
		}
	}
}

func (n *Node) runKeepAlives() {
	var neighbors []Neighbor
	n.Table.lock.RLock()
	for i := 0; i < len(n.Table.rows); i++ {
		for j := 0; j < len(n.Table.rows[i]); j++ {
			neighbors = append(neighbors, n.Table.rows[i][j]...)
		}
	}
	n.Table.lock.RUnlock()

	for _, nb := range neighbors {
		_, err := n.Probe(nb.Address)
		if err != nil {
			log.Printf("[REPAIR] Neighbor %s unreachable. Removing.", nb.Address)
			n.Table.Remove(nb.ID)
		}
	}

	var bps []Neighbor
	n.bpLock.RLock()
	for _, bp := range n.Backpointers {
		bps = append(bps, bp)
	}
	n.bpLock.RUnlock()

	for _, bp := range bps {
		_, err := n.Probe(bp.Address)
		if err != nil {
			log.Printf("[REPAIR] Backpointer %s unreachable. Removing.", bp.Address)
			n.bpLock.Lock()
			delete(n.Backpointers, bp.ID.String())
			n.bpLock.Unlock()
		}
	}
}

func (n *Node) runPointerGC() {
	n.lpLock.Lock()
	defer n.lpLock.Unlock()

	for key, entries := range n.LocationPointers {
		var active []*PointerEntry
		for _, entry := range entries {
			if time.Since(entry.LastUpdated) < POINTER_TIMEOUT {
				active = append(active, entry)
			}
		}
		
		if len(active) != len(entries) {
			n.LocationPointers[key] = active
		}
		
		if len(active) == 0 {
			delete(n.LocationPointers, key)
		}
	}
}

func (n *Node) republishObjects() {
	n.objLock.RLock()
	var keys []string
	for _, obj := range n.LocalObjects {
		keys = append(keys, obj.Key)
	}
	n.objLock.RUnlock()

	for _, key := range keys {
		go n.publishSelfSalted(key)
	}
}