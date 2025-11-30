package node

import (
	"sort"
	"sync"
	"tapestry/internal/id"
)

const (
	K_BACKUPS = 2 
)

type RoutingTable struct {
	localID id.ID
	rows [id.DIGITS][id.RADIX][]Neighbor
	lock sync.RWMutex
}

func NewRoutingTable(localID id.ID) *RoutingTable {
	return &RoutingTable{
		localID: localID,
	}
}

func (rt *RoutingTable) Add(neighbor Neighbor) bool {
	rt.lock.Lock()
	defer rt.lock.Unlock()

	if neighbor.ID.Equals(rt.localID) { return false }

	level := id.SharedPrefixLength(rt.localID, neighbor.ID)
	if level >= id.DIGITS { return false }
	digit := neighbor.ID.GetDigit(level)

	currentList := rt.rows[level][digit]

	for i, n := range currentList {
		if n.ID.Equals(neighbor.ID) {
			rt.rows[level][digit][i].Latency = neighbor.Latency
			rt.sortAndTrim(level, digit)
			return true 
		}
	}

	rt.rows[level][digit] = append(currentList, neighbor)
	rt.sortAndTrim(level, digit)
	return true
}

func (rt *RoutingTable) sortAndTrim(level, digit int) {
	list := rt.rows[level][digit]

	sort.Slice(list, func(i, j int) bool {
		return list[i].Latency < list[j].Latency
	})

	if len(list) > (1 + K_BACKUPS) {
		list = list[:1+K_BACKUPS]
	}
	rt.rows[level][digit] = list
}

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
			rt.rows[level][digit] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

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

func (rt *RoutingTable) sortByProximity(neighbors []Neighbor) {
	for i := 0; i < len(neighbors); i++ {
		for j := i + 1; j < len(neighbors); j++ {
			if id.Closer(rt.localID, neighbors[i].ID, neighbors[j].ID) {
				neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
			}
		}
	}
}