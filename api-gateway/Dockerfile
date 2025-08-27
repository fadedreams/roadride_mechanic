FROM golang:1.25
ENV GOPROXY=https://goproxy.cn

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o api-gateway .

EXPOSE 8081

CMD ["./api-gateway"]
