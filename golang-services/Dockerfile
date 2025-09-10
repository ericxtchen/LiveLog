FROM golang:1.25.1-alpine AS builder

ARG SERVICE_NAME

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
RUN go install github.com/cosmtrek/air@latest
COPY . .

RUN CGO_ENABLED=0 go build -o /app/bin/${SERVICE_NAME} ./cmd/${SERVICE_NAME}

FROM alpine:latest

ARG SERVICE_NAME

COPY --from=builder /app/bin/${SERVICE_NAME} /${SERVICE_NAME}

ENTRYPOINT [ "/" + SERVICE_NAME ]