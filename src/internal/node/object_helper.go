package node

import (
	"context"
	"fmt"
	"hash/fnv"

	pb "tapestry/api/proto"
)

type Object struct {
	Name    string
	Content string
}

func (n *Node) FindRoot(objectID uint64) (int, error) {
	ctx := context.Background()
	resp, err := n.Route(ctx, &pb.RouteRequest{
		ID:    objectID,
		Level: 0,
	})
	if err != nil {
		return 0, fmt.Errorf("routing failed: %v", err)
	}
	return int(resp.Port), nil
}

func StringToUint64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}