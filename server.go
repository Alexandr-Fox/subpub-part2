package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"subpubrpc/config"
	"sync"
	"syscall"
	"time"

	"github.com/Alexandr-Fox/subpub"
	"github.com/fsnotify/fsnotify"
	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_sentry "github.com/johnbellone/grpc-middleware-sentry"

	pb "subpubrpc/proto"
)

// Инициализация Sentry
func initSentry(dsn string, env string, debug bool, release string, sendDefaultPii bool) error {
	err := sentry.Init(sentry.ClientOptions{
		Dsn:              dsn,
		Environment:      env,
		TracesSampleRate: 1.0,
		AttachStacktrace: true,
		Debug:            debug,
		Release:          release,
		SendDefaultPII:   sendDefaultPii,
		EnableTracing:    true,
		SampleRate:       1.0,
	})
	if err != nil {
		return fmt.Errorf("sentry.Init: %w", err)
	}
	return nil
}

// Метрики Prometheus
var (
	subscribersGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pubsub_active_subscribers",
			Help: "Number of currently active subscribers",
		},
		[]string{"topic"},
	)

	messagesPublishedCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_messages_published_total",
			Help: "Total number of published messages",
		},
		[]string{"topic"},
	)

	messagesDeliveredCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_messages_delivered_total",
			Help: "Total number of successfully delivered messages",
		},
		[]string{"topic"},
	)

	deliveryErrorsCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pubsub_message_errors_total",
			Help: "Total number of message delivery errors",
		},
		[]string{"topic"},
	)

	deliveryLatencyHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pubsub_message_delivery_latency_seconds",
			Help:    "Message delivery latency distribution",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"topic"},
	)
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
	topic := req.GetKey()
	if topic == "" {
		sentry.CaptureMessage("Empty topic in Subscribe request")
		return status.Error(codes.InvalidArgument, "topic cannot be empty")
	}

	// Регистрируем подписчика
	s.mu.Lock()
	if _, ok := s.clients[topic]; !ok {
		s.clients[topic] = make(map[pb.PubSub_SubscribeServer]struct{})
	}
	s.clients[topic][stream] = struct{}{}
	subscribersGauge.WithLabelValues(topic).Inc()
	s.mu.Unlock()

	// Создаем подписку
	sub, err := s.bus.Subscribe(topic, func(msg interface{}) {
		start := time.Now()
		data, ok := msg.(string)
		if !ok {
			deliveryErrorsCounter.WithLabelValues(topic).Inc()
			sentry.CaptureException(fmt.Errorf("invalid message type for topic %s", topic))
			return
		}

		if err := stream.Send(&pb.Event{Data: data}); err != nil {
			deliveryErrorsCounter.WithLabelValues(topic).Inc()
			log.Printf("Failed to deliver message to topic %s: %v", topic, err)
			sentry.CaptureException(fmt.Errorf("failed to deliver message to topic %s: %w", topic, err))
			return
		}

		messagesDeliveredCounter.WithLabelValues(topic).Inc()
		deliveryLatencyHistogram.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	})
	if err != nil {
		subscribersGauge.WithLabelValues(topic).Dec()
		sentry.CaptureException(fmt.Errorf("subscribe error for topic %s: %w", topic, err))
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		sub.Unsubscribe()
		subscribersGauge.WithLabelValues(topic).Dec()
	}()

	// Ожидаем завершения
	select {
	case <-stream.Context().Done():
		log.Printf("Client disconnected from topic %s", topic)
	case <-s.shutdown:
		log.Printf("Server shutdown, unsubscribing from topic %s", topic)
	}

	// Удаляем подписчика
	s.mu.Lock()
	delete(s.clients[topic], stream)
	if len(s.clients[topic]) == 0 {
		delete(s.clients, topic)
	}
	s.mu.Unlock()

	return nil
}

func (s *pubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	topic := req.GetKey()
	data := req.GetData()

	if topic == "" {
		sentry.CaptureMessage("Empty topic in Publish request")
		return nil, status.Error(codes.InvalidArgument, "topic cannot be empty")
	}

	messagesPublishedCounter.WithLabelValues(topic).Inc()
	log.Printf("Publishing message to topic %s: %s", topic, data)

	if err := s.bus.Publish(topic, data); err != nil {
		deliveryErrorsCounter.WithLabelValues(topic).Inc()
		log.Printf("Error publishing message to topic %s: %s", topic, data)
		sentry.CaptureException(fmt.Errorf("publish error for topic %s: %w", topic, err))

		return nil, status.Error(codes.Internal, err.Error())
	}

	return &emptypb.Empty{}, nil
}

func (s *pubSubServer) Shutdown(ctx context.Context) error {
	close(s.shutdown)
	return s.bus.Close(ctx)
}

func startMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("Starting metrics server on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Metrics server error: %v", err)
			sentry.CaptureException(fmt.Errorf("metrics server error: %w", err))
		}
	}()

	return srv
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
			sentry.CaptureException(fmt.Errorf("config reload error: %w", err))
		}
	})

	return &cfg, nil
}

func main() {
	// Загрузка конфигурации
	cfg, err := loadConfig("config/config.yml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Инициализация Sentry
	if err := initSentry(cfg.Sentry.DSN, cfg.Sentry.Environment, cfg.Sentry.Debug, cfg.Sentry.Release, cfg.Sentry.SendDefaultPII); err != nil {
		log.Fatalf("Failed to initialize Sentry: %v", err)
	}
	defer sentry.Flush(2 * time.Second)
	defer sentry.Recover()

	// Запуск сервера метрик
	metricsServer := startMetricsServer(cfg.Logging.ConnectionString())

	// Создаем gRPC сервер
	lis, err := net.Listen("tcp", cfg.Server.GRPC.ConnectionString())
	if err != nil {
		sentry.CaptureException(fmt.Errorf("failed to listen: %w", err))
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_sentry.StreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_sentry.UnaryServerInterceptor(),
		)),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      cfg.Server.GRPC.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.Server.GRPC.MaxConnectionAgeGrace,
		}),
	)

	pubSubServer := NewPubSubServer()
	pb.RegisterPubSubServer(server, pubSubServer)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Starting gRPC server on %s", lis.Addr())
		if err := server.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			sentry.CaptureException(fmt.Errorf("gRPC server error: %w", err))
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")

	// Останавливаем серверы с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	// Останавливаем gRPC сервер
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	// Останавливаем сервис подписок
	if err := pubSubServer.Shutdown(ctx); err != nil {
		log.Printf("PubSub server shutdown error: %v", err)
		sentry.CaptureException(fmt.Errorf("pubsub server shutdown error: %w", err))
	}

	// Останавливаем сервер метрик
	if err := metricsServer.Shutdown(ctx); err != nil {
		log.Printf("Metrics server shutdown error: %v", err)
		sentry.CaptureException(fmt.Errorf("metrics server shutdown error: %w", err))
	}

	select {
	case <-stopped:
		log.Println("Servers stopped gracefully")
	case <-ctx.Done():
		server.Stop()
		log.Println("Servers stopped by timeout")
		sentry.CaptureMessage("Server shutdown by timeout")
	}
}
