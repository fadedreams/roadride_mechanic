FROM golang:1.25.1

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go .
RUN go build -o api-gateway .

RUN mkdir -p /var/log/api-gateway
RUN chmod -R 777 /var/log/api-gateway

CMD ["./api-gateway"]
