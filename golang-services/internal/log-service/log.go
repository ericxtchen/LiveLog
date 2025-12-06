package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	BulkBatchSize = 500
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Kafka client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("log-consumer"),
		kgo.ConsumeTopics("logs"),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	// OpenSearch client
	osClient, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("failed to create opensearch client: %v", err)
	}

	var bulkBuffer bytes.Buffer
	recordCount := 0
	flushTicker := time.NewTicker(1 * time.Second)
	defer flushTicker.Stop()

	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if err := fetches.Err(); err != nil {
			if err == context.Canceled && ctx.Err() != nil { // handle shutdown and other errors
				fmt.Println("Shutting down consumer")
				flushBulk(&bulkBuffer, osClient, "logs") // flush remaining
			}
		}

		fetches.EachRecord(func(record *kgo.Record) {
			// Bulk API expects two lines per document
			// 1. action metadata
			bulkBuffer.WriteString(`{"index":{}}` + "\n")
			// 2. document itself
			bulkBuffer.Write(record.Value)
			bulkBuffer.WriteString("\n")

			recordCount++

			if recordCount >= BulkBatchSize {
				flushBulk(&bulkBuffer, osClient, "logs")
				recordCount = 0
			}
		})

		select {
		case <-flushTicker.C:
			if recordCount > 0 {
				flushBulk(&bulkBuffer, osClient, "logs")
				recordCount = 0
			}
		default:
		}
	}
}

func flushBulk(buf *bytes.Buffer, client *opensearch.Client, indexName string) {
	if buf.Len() == 0 {
		return
	}

	blk := opensearchapi.BulkRequest{
		Index: indexName,
		Body:  strings.NewReader(buf.String()),
	}

	res, err := blk.Do(context.Background(), client)
	if err != nil {
		log.Printf("Error sending bulk insert request: %s", err)
		// Try retrying to insert
	} else {
		defer res.Body.Close()
		if res.IsError() {
			log.Printf("Error inserting into opensearch: %s", res.String())
		}
	}

	buf.Reset()
}
