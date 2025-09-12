FROM golang:1.25.1 AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-app

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/kafka-app .
EXPOSE 8087
CMD ["./kafka-app"]

