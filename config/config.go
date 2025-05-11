package config

import (
	"time"
)

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Logging LoggingConfig `mapstructure:"logging"`
}

type ServerConfig struct {
	GRPC            GRPCConfig    `mapstructure:"grpc"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type GRPCConfig struct {
	Host                  string        `mapstructure:"host"`
	Port                  int           `mapstructure:"port"`
	MaxConnectionAge      time.Duration `mapstructure:"max_connection_age"`
	MaxConnectionAgeGrace time.Duration `mapstructure:"max_connection_age_grace"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}
