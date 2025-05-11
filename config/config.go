package config

import (
	"fmt"
	"time"
)

type Config struct {
	Server  ServerConfig     `mapstructure:"server"`
	Sentry  SentryConfig     `mapstructure:"sentry"`
	Logging PrometheusConfig `mapstructure:"logging"`
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

type PrometheusConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type SentryConfig struct {
	DSN            string `mapstructure:"dsn"`
	Environment    string `mapstructure:"environment"`
	Debug          bool   `mapstructure:"debug"`
	Release        string `mapstructure:"release"`
	SendDefaultPII bool   `mapstructure:"send_default_pii"`
}

func (cfg *GRPCConfig) ConnectionString() string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}
func (cfg *PrometheusConfig) ConnectionString() string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}
