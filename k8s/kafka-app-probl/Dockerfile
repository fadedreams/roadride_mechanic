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

# Build the Go app with verbose output for debugging
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -v -o /kafka-app

# Use a Debian-based image for the final stage to ensure compatibility
FROM debian:bookworm-slim

# Install runtime dependencies for librdkafka
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary and schema
COPY --from=builder /kafka-app /kafka-app
COPY --from=builder /app/repair_event.avsc /repair_event.avsc

# Ensure the binary is executable
RUN chmod +x /kafka-app

# Verify the binary exists
RUN ls -l /kafka-app

ENTRYPOINT ["/kafka-app"]
