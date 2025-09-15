FROM golang:1.25.1

WORKDIR /app

COPY go.mod ./
RUN go mod init api-gateway
RUN go get github.com/elastic/go-elasticsearch/v8

COPY main.go .
RUN go build -o api-gateway .

RUN mkdir -p /var/log/api-gateway
RUN chmod -R 777 /var/log/api-gateway

CMD ["./api-gateway"]
