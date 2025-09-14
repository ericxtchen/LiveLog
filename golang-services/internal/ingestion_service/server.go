package ingestionservice

import (
	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"
	"github.com/twmb/franz-go/pkg/kgo"

	"fmt"
	"io"
	"log"
)

type server struct {
	pb.UnimplementedStreamLogsServer
}

func (s *server) StreamLogEntries(stream pb.StreamLogs_StreamLogEntriesServer) error {
	// Intialize Kafka producer
	var seeds = []string{"localhost:9092"}
	var producerTopic = "logs"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	// Listen for incoming log entries and produce to Kafka
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("Error receiving log entry: %v", err)
		}
		fmt.Printf("[%s] %s: %s\n", entry.Timestamp, entry.Level, entry.Message)

		record := &kgo.Record{
			Topic: producerTopic,
			Value: []byte(entry.Timestamp + " " + entry.Level + " " + entry.Message),
		}

		cl.Produce(stream.Context(), record, func(r *kgo.Record, err error) {
			if err != nil {
				log.Printf("failed to produce record: %v", err)
			} else {
				log.Printf("produced record to topic %s partition %d at offset %d", r.Topic, r.Partition, r.Offset)
			}
		})
	}
}
