FROM golang AS builder
LABEL authors="Alexandr-Fox"

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN go build -o server server.go

FROM ubuntu:22.04 AS release

WORKDIR /app

COPY --from=builder /app/server .
COPY --from=builder /app/config/config-example.yml ./config/config.yml

ENTRYPOINT ["/app/server"]