package main

import (
	"context"
	"github.com/Alexandr-Fox/subpub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"sync"

	pb "subpubrpc/proto"
)

type pubSubServer struct {
	pb.UnimplementedPubSubServer
	bus     subpub.SubPub
	mu      sync.Mutex
	clients map[string]map[pb.PubSub_SubscribeServer]struct{}
}

func NewPubSubServer() *pubSubServer {
	return &pubSubServer{
		bus:     subpub.NewSubPub(),
		clients: make(map[string]map[pb.PubSub_SubscribeServer]struct{}),
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

	// Ждем завершения контекста клиента
	<-stream.Context().Done()

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
	return s.bus.Close(ctx)
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()

	pb.RegisterPubSubServer(server, NewPubSubServer())
	log.Printf("server listening at %v", lis.Addr())

	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
