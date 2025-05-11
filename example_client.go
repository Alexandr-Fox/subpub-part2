package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "subpubrpc/proto"
)

const (
	gRPC_ADDRESS = "localhost:50051"
	gRPC_KEY     = "test"
)

func main() {
	// Настройка соединения с сервером
	conn, err := grpc.NewClient(gRPC_ADDRESS,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Обработка сигналов для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		subscribeToEvents(ctx, client, gRPC_KEY)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		publishMessages(ctx, client, gRPC_KEY)
	}()

	<-sigChan
	log.Println("Received shutdown signal, initiating graceful shutdown...")

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All operations completed successfully")
	case <-time.After(5 * time.Second):
		log.Println("Graceful shutdown timed out")
	}
}

func subscribeToEvents(ctx context.Context, client pb.PubSubClient, key string) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping subscription...")
			return
		default:
			stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: key})
			if err != nil {
				log.Printf("Subscription error: %v, retrying...", err)
				time.Sleep(1 * time.Second)

				continue
			}

			log.Printf("Subscribed to key: %s", key)
			for {
				event, err := stream.Recv()
				if err != nil {
					log.Printf("Stream receive error: %v", err)
					break
				}

				log.Printf("Received event: %s", event.GetData())
			}
		}
	}
}

func publishMessages(ctx context.Context, client pb.PubSubClient, key string) {
	count := 0
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping message publisher...")
			return
		case <-ticker.C:
			count++

			msg := &pb.PublishRequest{
				Key:  key,
				Data: fmt.Sprintf("Message %d", count),
			}

			_, err := client.Publish(ctx, msg)
			if err != nil {
				log.Printf("Publish error: %v", err)
				continue
			}

			log.Printf("Published message: %s", msg.Data)
		}
	}
}
