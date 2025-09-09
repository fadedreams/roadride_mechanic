FROM golang:1.25.1
ENV GOPROXY=https://goproxy.cn
# ENV GOPROXY=https://proxy.golang.org,direct

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o api-gateway .

# Create log directory
RUN mkdir -p /var/log/api-gateway

EXPOSE 8085

CMD ["./api-gateway"]
