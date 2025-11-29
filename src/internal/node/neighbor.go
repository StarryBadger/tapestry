package node

import (
	pb "tapestry/api/proto"
	"tapestry/internal/id"
)

// Neighbor represents a remote node in the overlay.
type Neighbor struct {
	ID      id.ID
	Address string // "IP:Port"
}

// ToProto converts the internal Neighbor struct to the Protobuf message.
func (n Neighbor) ToProto() *pb.Neighbor {
	return &pb.Neighbor{
		Id:      &pb.NodeID{Bytes: n.ID.Bytes()},
		Address: n.Address,
	}
}

// NeighborFromProto converts a Protobuf message to the internal struct.
func NeighborFromProto(p *pb.Neighbor) (Neighbor, error) {
	var rawID id.ID
	if p.Id != nil {
		copy(rawID[:], p.Id.Bytes)
	}
    
	return Neighbor{
		ID:      rawID,
		Address: p.Address,
	}, nil
}