# Use the official Go image to build the app
FROM golang:1.25.1 AS builder

WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY main.go ./

# Build the Go app
RUN CGO_ENABLED=0 GOOS=linux go build -o /consul-app

# Use a minimal base image for the final image
FROM alpine:latest
COPY --from=builder /consul-app /consul-app
ENTRYPOINT ["/consul-app"]
