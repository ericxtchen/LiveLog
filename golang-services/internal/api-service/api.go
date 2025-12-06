package apiservice

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/twmb/franz-go/pkg/kgo"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Clients struct {
	clients map[*websocket.Conn]bool
	mu      sync.Mutex
}

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

func handleWS(clients *Clients) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Error upgrading:", err)
		}
		defer conn.Close()

		clients.mu.Lock()
		clients.clients[conn] = true
		clients.mu.Unlock()

		defer func() {
			clients.mu.Lock()
			delete(clients.clients, conn)
			clients.mu.Unlock()
			conn.Close()
			log.Println("WebSocket client disconnected")
		}()
	}

}

func (clients *Clients) streamLogsWS(client *kgo.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		fetches := client.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			msg := r.Value
			clients.mu.Lock()
			for client := range clients.clients {
				err := client.WriteMessage(websocket.TextMessage, msg)
				if err != nil {
					log.Printf("Write error: %v", err)
					client.Close()
					delete(clients.clients, client)
				}
			}
			clients.mu.Unlock()
		})
	}
}

func main() {
	var err error
	osClient, err = opensearch.NewClient(opensearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
	if err != nil {
		log.Fatalf("failed to create opensearch client: %v", err)
	}
	// Kafka client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("api-websocket"), // use a different consumer group to consume from the same topic
		kgo.ConsumeTopics("logs"),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer cl.Close()

	clients := Clients{clients: make(map[*websocket.Conn]bool)}

	go clients.streamLogsWS(cl)

	http.HandleFunc("/ws", handleWS(&clients))
	http.HandleFunc("/api/logs", getLogs)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
