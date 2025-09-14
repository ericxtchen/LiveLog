package ingestionservice

import (
	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"

	"log"
	"net"

	"google.golang.org/grpc"
)

func Start() {
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	s := grpc.NewServer(opts...)
	pb.RegisterStreamLogsServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
