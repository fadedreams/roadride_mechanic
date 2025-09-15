FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
ENV GOPROXY=https://proxy.golang.org,direct
RUN apk add --no-cache git
RUN go mod download

COPY main.go .
RUN CGO_ENABLED=0 go build -v -mod=readonly -o api-gateway .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/api-gateway .
RUN mkdir -p /var/log/api-gateway
RUN chown nobody:nogroup /var/log/api-gateway
RUN chmod -R 755 /var/log/api-gateway
USER nobody
CMD ["./api-gateway"]
