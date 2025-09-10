package agentservice

import (
	agent "github.com/ericxtchen/LiveLog/golang-services/internal/agent_service"
)

func main() {
	// Entry point for the Agent service
	agent.Start()
}
