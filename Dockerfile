FROM golang:1.25.4-alpine

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main ./api-go/cmd/api/main.go

CMD ["./main"]