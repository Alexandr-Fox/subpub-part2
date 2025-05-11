package main

import (
	"context"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"os"
	"os/signal"
	"subpubrpc/config"
	"sync"
	"syscall"

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

func loadConfig(path string) (*config.Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg config.Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Настройка автоматического обновления конфига
	v.WatchConfig()
	v.OnConfigChange(func(e fsnotify.Event) {
		log.Println("Config file changed:", e.Name)
		if err := v.Unmarshal(&cfg); err != nil {
			log.Printf("Error reloading config: %v", err)
		}
	})

	return &cfg, nil
}

func setupLogger(cfg *config.LoggingConfig) {
	// Здесь можно настроить логгер в соответствии с конфигурацией
	// Например, для zap или logrus
	log.Printf("Logger configured with level=%s and format=%s", cfg.Level, cfg.Format)
}

func main() {
	// Загрузка конфигурации
	cfg, err := loadConfig("config/config.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Настройка логгера
	setupLogger(&cfg.Logging)

	// Создаем gRPC сервер с параметрами из конфига
	server := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      cfg.Server.GRPC.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.Server.GRPC.MaxConnectionAgeGrace,
		}),
	)

	pubSubServer := NewPubSubServer()
	pb.RegisterPubSubServer(server, pubSubServer)

	lis, err := net.Listen("tcp",
		fmt.Sprintf("%s:%d", cfg.Server.GRPC.Host, cfg.Server.GRPC.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		log.Printf("Starting gRPC server on %s:%d", cfg.Server.GRPC.Host, cfg.Server.GRPC.Port)
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(
		context.Background(),
		cfg.Server.ShutdownTimeout,
	)
	defer cancel()

	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	if err := pubSubServer.Shutdown(ctx); err != nil {
		log.Printf("PubSub server shutdown error: %v", err)
	}

	select {
	case <-stopped:
		log.Println("Server stopped gracefully")
	case <-ctx.Done():
		server.Stop()
		log.Println("Server stopped by timeout")
	}
}
