package main

import (
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Alexandr-Fox/subpub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "subpubrpc/proto"
)

type pubSubServer struct {
	pb.UnimplementedPubSubServer
	bus      subpub.SubPub
	mu       sync.Mutex
	clients  map[string]map[pb.PubSub_SubscribeServer]struct{}
	shutdown chan struct{}
}

func NewPubSubServer() *pubSubServer {
	return &pubSubServer{
		bus:      subpub.NewSubPub(),
		clients:  make(map[string]map[pb.PubSub_SubscribeServer]struct{}),
		shutdown: make(chan struct{}),
	}
}

func (s *pubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	key := req.GetKey()
	if key == "" {
		return status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Регистрируем клиента
	s.mu.Lock()
	if _, ok := s.clients[key]; !ok {
		s.clients[key] = make(map[pb.PubSub_SubscribeServer]struct{})
	}
	s.clients[key][stream] = struct{}{}
	s.mu.Unlock()

	// Создаем подписку
	sub, err := s.bus.Subscribe(key, func(msg interface{}) {
		data, ok := msg.(string)
		if !ok {
			return
		}
		if err := stream.Send(&pb.Event{Data: data}); err != nil {
			log.Printf("Failed to send event: %v", err)
		}
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer sub.Unsubscribe()

	// Ждем завершения контекста клиента или shutdown сервера
	select {
	case <-stream.Context().Done():
	case <-s.shutdown:
	}

	// Удаляем клиента при отключении
	s.mu.Lock()
	delete(s.clients[key], stream)
	if len(s.clients[key]) == 0 {
		delete(s.clients, key)
	}
	s.mu.Unlock()

	return nil
}

func (s *pubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	key := req.GetKey()
	data := req.GetData()

	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	if err := s.bus.Publish(key, data); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *pubSubServer) Shutdown(ctx context.Context) error {
	close(s.shutdown)
	return s.bus.Close(ctx)
}

func main() {
	// Создаем gRPC сервер
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pubSubServer := NewPubSubServer()
	pb.RegisterPubSubServer(server, pubSubServer)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Starting gRPC server on %s", lis.Addr())
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Ожидаем сигнал завершения
	<-quit
	log.Println("Shutting down server...")

	// Останавливаем gRPC сервер
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Сначала GracefulStop для gRPC сервера
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	// Затем shutdown для сервиса подписок
	if err := pubSubServer.Shutdown(ctx); err != nil {
		log.Printf("PubSub server shutdown error: %v", err)
	}

	// Ожидаем завершения или таймаута
	select {
	case <-stopped:
		log.Println("Server stopped gracefully")
	case <-ctx.Done():
		server.Stop()
		log.Println("Server stopped by timeout")
	}
}
