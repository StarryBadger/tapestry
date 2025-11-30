package node

import (
	"time"
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

type Neighbor struct {
	ID      id.ID
	Address string
	Latency time.Duration
}

func (n Neighbor) ToProto() *pb.Neighbor {
	return &pb.Neighbor{
		Id:      &pb.NodeID{Bytes: n.ID.Bytes()},
		Address: n.Address,
	}
}

func NeighborFromProto(p *pb.Neighbor) (Neighbor, error) {
	var rawID id.ID
	if p.Id != nil {
		copy(rawID[:], p.Id.Bytes)
	}
    
	return Neighbor{
		ID:      rawID,
		Address: p.Address,
		Latency: 0,
	}, nil
}