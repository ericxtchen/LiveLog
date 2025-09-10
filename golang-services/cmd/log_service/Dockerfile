# ---- Build stage ----
FROM golang:1.25.1-alpine AS builder
WORKDIR /app

# Copy module files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy only the code this service needs
COPY cmd/log-service ./cmd/log-service
COPY internal/log-service ./internal/log-service
RUN go install github.com/cosmtrek/air@latest
COPY api ./api

# Build the service binary
WORKDIR /app/cmd/log-service
RUN CGO_ENABLED=0 go build -o /bin/log-service .

# ---- Runtime stage ----
FROM alpine:3.18
WORKDIR /app

COPY --from=builder /bin/log-service .

ENTRYPOINT ["/log-service"]