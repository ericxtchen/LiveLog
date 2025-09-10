# ---- Build stage ----
FROM golang:1.25.1-alpine AS builder
WORKDIR /app

# Copy module files first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy only the code this service needs
COPY cmd/metric-service ./cmd/metric-service
COPY internal/metric-service ./internal/metric-service
RUN go install github.com/cosmtrek/air@latest
COPY api ./api

# Build the service binary
WORKDIR /app/cmd/metric-service
RUN CGO_ENABLED=0 go build -o /bin/metric-service .

# ---- Runtime stage ----
FROM alpine:3.18
WORKDIR /app

COPY --from=builder /bin/metric-service .

ENTRYPOINT ["/metric-service"]