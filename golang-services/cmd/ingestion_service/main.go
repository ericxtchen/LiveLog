package ingestionservice

import (
	ingestion "github.com/ericxtchen/LiveLog/golang-services/internal/ingestion_service"
)

func main() {
	// Entry point for the Ingestion service
	ingestion.Start()
}
