package ingestionservice

import (
	pb "github.com/ericxtchen/LiveLog/golang-services/api/proto"

	"fmt"
	"regexp"
	"time"
)

var controlChars = regexp.MustCompile(`[\x00-\x1F\x7F]`)

func ValidateLogEntry(entry *pb.LogEntry) error { // TODO: We are self hosting the ingestion service as a SaaS, so we need to send the error back to the dashboard to the user.
	// Basic validation: check if required fields are present
	if entry.Timestamp == "" || entry.Message == "" {
		return fmt.Errorf("invalid log entry: missing required fields")
	}
	if entry.Timestamp == "" {
		return fmt.Errorf("invalid log entry: missing timestamp")
	}
	if entry.Message == "" {
		return fmt.Errorf("invalid log entry: missing message")
	}
	if _, err := time.Parse(time.RFC3339, entry.GetTimestamp()); err != nil {
		return fmt.Errorf("invalid log entry: timestamp format is incorrect")
	}
	if controlChars.MatchString(entry.Message) {
		return fmt.Errorf("invalid log entry: message contains invalid characters")
	}
	if len(entry.Message) > 10000 { // arbitrary limit for message length
		return fmt.Errorf("invalid log entry: message is too long")
	}

	return nil
}
