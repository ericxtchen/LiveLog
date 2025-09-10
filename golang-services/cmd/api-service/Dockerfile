# ---- Build stage ----
FROM golang:1.25.1-alpine AS builder
WORKDIR /app

# Copy module files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy only the code this service needs
COPY cmd/api-service ./cmd/api-service
COPY internal/api-service ./internal/api-service
RUN go install github.com/cosmtrek/air@latest
COPY api ./api

# Build the service binary
WORKDIR /app/cmd/api-service
RUN CGO_ENABLED=0 go build -o /bin/api-service .

# ---- Runtime stage ----
FROM alpine:3.18
WORKDIR /app

COPY --from=builder /bin/api-service .

ENTRYPOINT ["/api-service"]