package logservice

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"

	"github.com/opensearch-project/opensearch-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type log_entry struct {
	Timestamp string
	Level     string
	Message   string
}

func Start() {
	// Initialize Kafka consumer
	var seeds = []string{"localhost:9092"}
	var consumerTopic = "logs"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(consumerTopic),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	// Initialize OpenSearch client
	opensearch_client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("Error creating OpenSearch client: %s", err)
	}

	// Verify OpenSearch connection
	_, err = opensearch_client.Info()
	if err != nil {
		log.Fatalf("Error getting OpenSearch info: %s", err)
	}

	// Continuously poll for new messages
	for {
		fetches := cl.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				// Handle errors
				log.Printf("error fetching records: %v", e)
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			// Process each record and index into OpenSearch
			// TODO: This will probably have to change
			// TODO: Add indexing into OpenSearch
			var entry log_entry
			err := json.Unmarshal(rec.Value, &entry)
			if err != nil {
				log.Printf("failed to unmarshal record value: %v", err)
				return
			}
			println(string(rec.Value))
		})
	}
}
