package node

import (
	"context"
	"log"

	pb "tapestry/api/proto"
	"tapestry/internal/util"
)

func (n *Node) Insert(bootstrapPort int) error {
	n.rtLock.Lock()
	for level := 0; level < util.DIGITS; level++ {
		digit := util.GetDigit(n.ID, level)
		n.RoutingTable[level][digit] = n.Port
	}
	n.rtLock.Unlock()

	if bootstrapPort == 0 {
		return nil
	}

	client, conn, err := GetNodeClient(bootstrapPort)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := client.Route(context.Background(), &pb.RouteRequest{ID: n.ID, Level: 0})
	if err != nil {
		return err
	}

	rootPort := int(resp.Port)
	rootID := resp.ID

	rootClient, rootConn, err := GetNodeClient(rootPort)
	if err != nil {
		return err
	}
	defer rootConn.Close()

	rtCopyResp, err := rootClient.RTCopy(context.Background(), &pb.Nothing{})
	if err != nil {
		return err
	}
	rtCopy := util.UnflattenMatrix(rtCopyResp.Data, int(rtCopyResp.Rows), int(rtCopyResp.Cols))

	commonLen := util.CommonPrefixLen(rootID, n.ID)
	for level := 0; level < commonLen; level++ {
		n.rtLock.Lock()
		n.RoutingTable[level] = rtCopy[level]
		n.rtLock.Unlock()
	}

	_, err = rootClient.InformHoleMulticast(context.Background(), &pb.MulticastRequest{
		NewPort:       int32(n.Port),
		NewID:         n.ID,
		OriginalLevel: int32(commonLen),
		Level:         int32(commonLen),
	})
	if err != nil {
		log.Printf("Error during multicast initiation: %v", err)
	}

	for level := 0; level < commonLen; level++ {
		for _, port := range rtCopy[level] {
			if port != -1 && port != n.Port {
				go func(p int) {
					client, conn, err := GetNodeClient(p)
					if err == nil {
						defer conn.Close()
						client.BPUpdate(context.Background(), &pb.BPUpdateRequest{ID: n.ID, Port: int32(n.Port)})
					}
				}(port)
			}
		}
	}

	return nil
}

func (n *Node) RTCopy(ctx context.Context, req *pb.Nothing) (*pb.RTCopyResponse, error) {
	n.rtLock.RLock()
	defer n.rtLock.RUnlock()

	return &pb.RTCopyResponse{
		Data: util.FlattenMatrix(n.RoutingTable),
		Rows: int32(util.DIGITS),
		Cols: int32(util.RADIX),
	}, nil
}

func (n *Node) InformHoleMulticast(ctx context.Context, req *pb.MulticastRequest) (*pb.MulticastResponse, error) {
	level := int(req.Level)
	originalLevel := int(req.OriginalLevel)
	newPort := int(req.NewPort)
	newID := req.NewID

	if level < util.DIGITS {
		n.rtLock.RLock()
		row := n.RoutingTable[level]
		n.rtLock.RUnlock()
		for _, port := range row {
			if port != -1 && port != n.Port && port != newPort {
				go func(p int) {
					client, conn, err := GetNodeClient(p)
					if err == nil {
						defer conn.Close()
						client.InformHoleMulticast(ctx, &pb.MulticastRequest{
							NewPort:       req.NewPort,
							NewID:         req.NewID,
							OriginalLevel: req.OriginalLevel,
							Level:         req.Level + 1,
						})
					}
				}(port)
			}
		}
	}

	digit := util.GetDigit(newID, originalLevel)
	n.rtLock.Lock()
	n.RoutingTable[originalLevel][digit] = newPort
	n.rtLock.Unlock()

	client, conn, err := GetNodeClient(newPort)
	if err == nil {
		defer conn.Close()
		client.BPUpdate(context.Background(), &pb.BPUpdateRequest{ID: n.ID, Port: int32(n.Port)})
	}

	return &pb.MulticastResponse{Status: 0}, nil
}