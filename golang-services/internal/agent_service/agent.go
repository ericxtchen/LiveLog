package agentservice

import (
	"context"
	"log"

	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"
	collection "github.com/ericxtchen/LiveLog/golang-services/internal/agent_service/data_collection"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func Start() {
	conn, err := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewStreamLogsClient(conn)
	for {
		t, err := collection.TailLog("/var/log/syslog") // how do we expand to more files that are being tailed within a system?
		if err != nil {
			panic(err)
		}
		stream, err := client.StreamLogEntries(context.Background())
		if err != nil {
			log.Fatalf("Error creating stream: %v", err)
		}

		go func() {
			for logEntry := range t.Lines {
				if err := stream.Send(&pb.LogEntry{Message: logEntry.Text}); err != nil {
					log.Fatalf("Error sending log entry: %v", err)
				}
			}
			stream.CloseSend()
		}()

	}
}
