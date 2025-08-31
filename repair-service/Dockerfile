FROM golang:1.25
ENV GOPROXY=https://goproxy.cn

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o repair-service .

# Create log directory
RUN mkdir -p /var/log/repair-service

EXPOSE 8080 50051

CMD ["./repair-service"]
