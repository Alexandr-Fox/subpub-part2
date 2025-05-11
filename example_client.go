package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "subpubrpc/proto"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	// Подписка
	go func() {
		stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: "test1"})
		if err != nil {
			log.Fatalf("could not subscribe: %v", err)
		}

		for {
			event, err := stream.Recv()
			if err != nil {
				log.Printf("subscription error: %v", err)
				return
			}
			log.Printf("Received event: %s", event.GetData())
		}
	}()

	// Публикация
	for i := 0; i < 5; i++ {
		_, err := client.Publish(context.Background(), &pb.PublishRequest{
			Key:  "test",
			Data: "Hello World!",
		})
		if err != nil {
			log.Fatalf("could not publish: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}
