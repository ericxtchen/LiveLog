package agentservice

import (
	"context"
	"log"
	"strings"
	"time"

	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"
	collection "github.com/ericxtchen/LiveLog/golang-services/internal/agent_service/data_collection"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var levels = []string{"DEBUG", "INFO", "WARN", "ERROR"}

func Start() {
	conn, err := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewStreamLogsClient(conn)
	for {
		t, err := collection.TailLog("/var/log/syslog") // expand to log multiple files, let user specify path of file(s)
		if err != nil {
			panic(err)
		}
		stream, err := client.StreamLogEntries(context.Background())
		if err != nil {
			log.Fatalf("Error creating stream: %v", err)
		}

		go func() {
			for logEntry := range t.Lines {
				var log_level string = "INFO" // default log level
				for _, level := range levels {
					if strings.Contains(logEntry.Text, level) {
						log_level = level
						break
					}
				}
				if err := stream.Send(&pb.LogEntry{Timestamp: logEntry.Time.Format(time.RFC3339), Level: log_level, Message: logEntry.Text}); err != nil {
					log.Fatalf("Error sending log entry: %v", err)
				}
			}
			stream.CloseSend()
		}()

	}
}
