# Stage 1: Build
FROM golang:1.22 AS builder

WORKDIR /app
COPY go.mod go.sum ./
ENV GOPROXY=https://proxy.golang.org,direct
RUN go mod download

COPY main.go .
RUN go build -v -o api-gateway .

# Stage 2: Runtime
FROM golang:1.22
WORKDIR /app
COPY --from=builder /app/api-gateway .
RUN mkdir -p /var/log/api-gateway
RUN chown nobody:nogroup /var/log/api-gateway
RUN chmod -R 755 /var/log/api-gateway
USER nobody
CMD ["./api-gateway"]
