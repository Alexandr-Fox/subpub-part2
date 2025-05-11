package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "subpubrpc/proto"
)

func main() {
	// Парсинг аргументов командной строки
	host := flag.String("host", "localhost:50051", "gRPC server address")
	topic := flag.String("topic", "default", "Topic to subscribe to")
	flag.Parse()

	// Установка соединения с сервером
	conn, err := grpc.NewClient(*host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	// Обработка сигналов для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal...")
		cancel()
	}()

	// Подписка на сообщения
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: *topic})
	if err != nil {
		log.Fatalf("could not subscribe: %v", err)
	}

	log.Printf("Subscribed to topic '%s'. Waiting for messages...", *topic)

	for {
		event, err := stream.Recv()
		if err != nil {
			log.Printf("Subscription error: %v", err)
			return
		}
		log.Printf("Received message: %s", event.GetData())
	}
}
