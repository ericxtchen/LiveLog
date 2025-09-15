package ingestionservice

import (
	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"
	"github.com/twmb/franz-go/pkg/kgo"

	"encoding/json"
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
		// Receive log entry from stream
		entry, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("Error receiving log entry: %v", err)
		}
		log.Printf("[%s] %s: %s\n", entry.Timestamp, entry.Level, entry.Message)

		// Validate log entry
		err = ValidateLogEntry(entry)
		if err != nil {
			log.Printf("Invalid log entry: %v", err)
			continue // Skip invalid entries, should notify user somehow
		}

		// Produce log entry to Kafka
		log_entry := &pb.LogEntry{Timestamp: entry.Timestamp, Level: entry.Level, Message: entry.Message}
		json_log_entry, err := json.Marshal(log_entry)
		if err != nil {
			log.Printf("failed to marshal log entry: %v", err)
			continue
		}
		// Is sending JSON through Kafka faster ot sending protobuf directly and making log_service handle unmarshalling and conversion into JSON faster?
		record := &kgo.Record{
			Topic: producerTopic,
			Value: json_log_entry,
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
