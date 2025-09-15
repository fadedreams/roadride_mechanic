FROM golang:1.25.1

WORKDIR /app

COPY go.mod go.sum ./
ENV GOPROXY=https://proxy.golang.org,direct
RUN go mod download

COPY main.go .
RUN go build -v -o api-gateway .

RUN mkdir -p /var/log/api-gateway
RUN chown nobody:nogroup /var/log/api-gateway
RUN chmod -R 755 /var/log/api-gateway

USER nobody
CMD ["./api-gateway"]
