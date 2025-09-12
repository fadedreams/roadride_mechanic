# Multi-stage build with CGO enabled for librdkafka
FROM golang:1.21-alpine AS builder

# Install build dependencies for cgo and musl (Alpine)
RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

# Copy go files
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./

# Build with CGO_ENABLED=1 and musl tags (for static linking on Alpine)
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -tags musl -ldflags="-s -w" -o kafka-app ./main.go

# Runtime stage (minimal Alpine)
FROM alpine:3.18
RUN apk add --no-cache ca-certificates  # For HTTPS/TLS in Kafka/SR
WORKDIR /app
COPY --from=builder /app/kafka-app .
EXPOSE 8087
CMD ["./kafka-app"]
