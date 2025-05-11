package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "subpubrpc/proto"
)

func main() {
	// Парсинг аргументов командной строки
	host := flag.String("host", "localhost:50051", "gRPC server address")
	topic := flag.String("topic", "default", "Topic to publish to")
	message := flag.String("message", "", "Message to publish")
	flag.Parse()

	if *message == "" {
		log.Fatal("Message cannot be empty")
	}

	// Установка соединения с сервером
	conn, err := grpc.NewClient(*host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	// Создаем контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Публикация сообщения
	_, err = client.Publish(ctx, &pb.PublishRequest{
		Key:  *topic,
		Data: *message,
	})
	if err != nil {
		log.Fatalf("could not publish: %v", err)
	}

	log.Printf("Message published to topic '%s': %s", *topic, *message)
}
