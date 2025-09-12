# Use the official Go image to build the app
FROM golang:1.25.1 AS builder

# Install librdkafka and dependencies
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code and schema
COPY main.go ./
COPY repair_event.avsc ./

# Build the Go app
RUN CGO_ENABLED=1 GOOS=linux go build -o /kafka-app

# Use a minimal base image for the final image
FROM alpine:latest
# Install runtime dependencies for librdkafka
RUN apk add --no-cache librdkafka
COPY --from=builder /kafka-app /kafka-app
COPY --from=builder /app/repair_event.avsc /repair_event.avsc
ENTRYPOINT ["/kafka-app"]
