FROM golang:1.25
ENV GOPROXY=https://goproxy.cn

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o mechanic-service .

EXPOSE 8082

CMD ["./mechanic-service"]
