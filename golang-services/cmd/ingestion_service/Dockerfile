# ---- Build stage ----
FROM golang:1.25.1-alpine AS builder
WORKDIR /app

# Copy module files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy only the code this service needs
COPY cmd/ingestion-service ./cmd/ingestion-service
COPY internal/ingestion-service ./internal/ingestion-service
RUN go install github.com/cosmtrek/air@latest
COPY api ./api

# Build the service binary
WORKDIR /app/cmd/ingestion-service
RUN CGO_ENABLED=0 go build -o /bin/ingestion-service .

# ---- Runtime stage ----
FROM alpine:3.18
WORKDIR /app

COPY --from=builder /bin/ingestion-service .

ENTRYPOINT ["/ingestion-service"]