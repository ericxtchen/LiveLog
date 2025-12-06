package apiservice

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

type SearchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source struct {
				Timestamp string `json:"@timestamp"`
				Level     string `json:"level"`
				Message   string `json:"message"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

type LogItem struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

var osClient *opensearch.Client

func getLogs(w http.ResponseWriter, r *http.Request) {
	// Fetch latest 100 logs in OpenSearch sorted by timestamp
	// Return a JSON object contianing the logs
	content := strings.NewReader(`{
		"size": 100,
		"sort": [
			{ "@timestamp": { "order": "desc" } }
		] 
	}`)

	search := opensearchapi.SearchRequest{
		Index: []string{"logs"},
		Body:  content,
	}

	searchResponse, err := search.Do(context.Background(), osClient)
	if err != nil {
		log.Printf("Error in executing search for logs in OpenSearch: %s", err)
	}
	defer searchResponse.Body.Close()

	if searchResponse.IsError() {
		log.Printf("Error in searching: %s", searchResponse.String())
	}

	var res SearchResponse
	if err := json.NewDecoder(searchResponse.Body).Decode(&res); err != nil {
		log.Printf("Error parsing logs: %s", err)
	}

	cleanLogs := make([]LogItem, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		cleanLogs = append(cleanLogs, LogItem{
			Timestamp: hit.Source.Timestamp,
			Level:     hit.Source.Level,
			Message:   hit.Source.Message,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(cleanLogs)

}

func main() {
	var err error
	osClient, err = opensearch.NewClient(opensearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("failed to create opensearch client: %v", err)
	}

	http.HandleFunc("/api/logs", getLogs)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
