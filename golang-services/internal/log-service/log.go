package logservice

import (
	"bytes"
	"context"
	"crypto/tls"
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

		// Process each record and index into OpenSearch
		fetches.EachRecord(func(rec *kgo.Record) {
			// TODO: Do bulk insdexing instead of single indexing
			res, err := opensearch_client.Index(
				"logs",
				bytes.NewReader(rec.Value),
				opensearch_client.Index.WithDocumentType("_doc"),
				opensearch_client.Index.WithRefresh("true"),
				opensearch_client.Index.WithContext(context.Background()),
			)
			if err != nil {
				log.Printf("Error indexing document: %s", err)
			}
			defer res.Body.Close()
			if res.IsError() {
				log.Printf("Error indexing document: %s", res.String())
			} else {
				log.Printf("Document indexed successfully: %s", res.String())
			}
			println(string(rec.Value))
		})
	}
}
