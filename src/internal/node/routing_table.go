package node

import (
	"sync"
	"tapestry/internal/id"
)

const (
	K_BACKUPS = 2 // 1 Primary + 2 Backups = 3 Total
)

// RoutingTable implements the Tapestry Neighbor Map.
// It is a multi-level structure where level 'n' matches a prefix of length 'n'.
type RoutingTable struct {
	localID id.ID
	// table[level][digit] -> List of Neighbors
	// Levels: 0..39 (for 160-bit Hex)
	// Digits: 0..15 (Hex)
	rows [id.DIGITS][id.RADIX][]Neighbor
	lock sync.RWMutex
}

func NewRoutingTable(localID id.ID) *RoutingTable {
	return &RoutingTable{
		localID: localID,
	}
}

// Add attempts to add a neighbor to the routing table.
// It determines the correct level based on shared prefix length.
// It maintains the list sorted by proximity (closest first).
// Returns true if the table was updated.
func (rt *RoutingTable) Add(neighbor Neighbor) bool {
	rt.lock.Lock()
	defer rt.lock.Unlock()

	// 1. Do not add ourselves
	if neighbor.ID.Equals(rt.localID) {
		return false
	}

	// 2. Determine Level (Shared Prefix Length)
	level := id.SharedPrefixLength(rt.localID, neighbor.ID)
	
	// If matching completely, it's a collision or the same node (handled above)
	if level >= id.DIGITS {
		return false
	}

	// 3. Determine Digit at that level
	digit := neighbor.ID.GetDigit(level)

	// 4. Get current list
	currentList := rt.rows[level][digit]

	// 5. Check if already exists
	for _, n := range currentList {
		if n.ID.Equals(neighbor.ID) {
			return false // Already present
		}
	}

	// 6. Insert and Sort (Proximity)
	// In a real implementation, we would measure RTT here.
	// For now, we use XOR distance as the metric.
	updatedList := append(currentList, neighbor)
	
	// Simple Insertion Sort based on "Closeness" to localID
	// Note: The paper implies sorting by distance to *local node* for optimization,
	// though strictly the slot requirement is matching the prefix.
	// We keep the K closest nodes that satisfy the prefix constraint.
	rt.sortByProximity(updatedList)

	// 7. Trim to K size
	if len(updatedList) > (1 + K_BACKUPS) {
		updatedList = updatedList[:1+K_BACKUPS]
	}

	rt.rows[level][digit] = updatedList
	return true
}

// Remove removes a neighbor from the table (e.g., on disconnect)
func (rt *RoutingTable) Remove(neighborID id.ID) bool {
	rt.lock.Lock()
	defer rt.lock.Unlock()

	level := id.SharedPrefixLength(rt.localID, neighborID)
	if level >= id.DIGITS {
		return false
	}
	digit := neighborID.GetDigit(level)

	list := rt.rows[level][digit]
	for i, n := range list {
		if n.ID.Equals(neighborID) {
			// Delete preserving order
			rt.rows[level][digit] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

// Get returns the list of neighbors at a specific slot.
// Returns a copy to be thread-safe.
func (rt *RoutingTable) Get(level int, digit int) []Neighbor {
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	if level < 0 || level >= id.DIGITS || digit < 0 || digit >= id.RADIX {
		return nil
	}

	list := rt.rows[level][digit]
	result := make([]Neighbor, len(list))
	copy(result, list)
	return result
}

// GetLevel returns all neighbors at a specific level (for table copying)
func (rt *RoutingTable) GetLevel(level int) []Neighbor {
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	var result []Neighbor
	if level < 0 || level >= id.DIGITS {
		return result
	}

	for d := 0; d < id.RADIX; d++ {
		result = append(result, rt.rows[level][d]...)
	}
	return result
}

// Size returns the total count of neighbors
func (rt *RoutingTable) Size() int {
	rt.lock.RLock()
	defer rt.lock.RUnlock()
	count := 0
	for i := 0; i < id.DIGITS; i++ {
		for j := 0; j < id.RADIX; j++ {
			count += len(rt.rows[i][j])
		}
	}
	return count
}

// sortByProximity sorts the list such that the node "closest" to localID is first.
func (rt *RoutingTable) sortByProximity(neighbors []Neighbor) {
	// Simple bubble sort for small list (max 4 items)
	for i := 0; i < len(neighbors); i++ {
		for j := i + 1; j < len(neighbors); j++ {
			// If neighbors[j] is closer than neighbors[i], swap
			if id.Closer(rt.localID, neighbors[i].ID, neighbors[j].ID) {
				neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
			}
		}
	}
}