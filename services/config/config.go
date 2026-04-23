// Package config provides configuration management for the telemetry collector system.
// It loads configuration from YAML files and environment variables using Viper.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the application configuration
type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Queue     QueueConfig     `mapstructure:"queue"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Streamer  StreamerConfig  `mapstructure:"streamer"`
	Collector CollectorConfig `mapstructure:"collector"`
	Logging   LoggingConfig   `mapstructure:"logging"`
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

// QueueConfig holds message queue configuration
type QueueConfig struct {
	DataDir              string        `mapstructure:"data_dir"`
	NumPartitions        int           `mapstructure:"num_partitions"`
	CacheDuration        time.Duration `mapstructure:"cache_duration"`
	CacheCleanupInterval time.Duration `mapstructure:"cache_cleanup_interval"`
	MaxMessageSize       int           `mapstructure:"max_message_size"`
	SegmentSize          int64         `mapstructure:"segment_size"`
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	DBName       string `mapstructure:"db_name"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

// StreamerConfig holds streamer configuration
type StreamerConfig struct {
	CSVFilePath       string        `mapstructure:"csv_file_path"`
	BatchSize         int           `mapstructure:"batch_size"`
	RetryMaxAttempts  int           `mapstructure:"retry_max_attempts"`
	RetryInitialDelay time.Duration `mapstructure:"retry_initial_delay"`
	RetryMaxDelay     time.Duration `mapstructure:"retry_max_delay"`
	RetryMultiplier   float64       `mapstructure:"retry_multiplier"`
}

// CollectorConfig holds collector configuration
type CollectorConfig struct {
	QueueEndpoint    string        `mapstructure:"queue_endpoint"`
	ConsumerGroupID  string        `mapstructure:"consumer_group_id"`
	BatchSize        int           `mapstructure:"batch_size"`
	PollInterval     time.Duration `mapstructure:"poll_interval"`
	RetryMaxAttempts int           `mapstructure:"retry_max_attempts"`
	RetryDelay       time.Duration `mapstructure:"retry_delay"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	Output string `mapstructure:"output"`
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	// Set defaults
	setDefaults()

	// Read config file
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Override with environment variables
	viper.AutomaticEnv()
	viper.SetEnvPrefix("TELEMETRY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.read_timeout", 30*time.Second)
	viper.SetDefault("server.write_timeout", 30*time.Second)

	viper.SetDefault("queue.data_dir", "./data/queue")
	viper.SetDefault("queue.num_partitions", 3)
	viper.SetDefault("queue.cache_duration", 2*time.Minute)
	viper.SetDefault("queue.cache_cleanup_interval", 1*time.Minute)
	viper.SetDefault("queue.max_message_size", 1024*1024) // 1MB
	viper.SetDefault("queue.segment_size", 100*1024*1024) // 100MB

	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.password", "postgres")
	viper.SetDefault("database.db_name", "telemetry")
	viper.SetDefault("database.max_open_conns", 25)
	viper.SetDefault("database.max_idle_conns", 5)

	viper.SetDefault("streamer.batch_size", 100)
	viper.SetDefault("streamer.retry_max_attempts", 3)
	viper.SetDefault("streamer.retry_initial_delay", 1*time.Second)
	viper.SetDefault("streamer.retry_max_delay", 30*time.Second)
	viper.SetDefault("streamer.retry_multiplier", 2.0)

	viper.SetDefault("collector.queue_endpoint", "http://localhost:8080")
	viper.SetDefault("collector.consumer_group_id", "default-collector")
	viper.SetDefault("collector.batch_size", 100)
	viper.SetDefault("collector.poll_interval", 1*time.Second)
	viper.SetDefault("collector.retry_max_attempts", 3)
	viper.SetDefault("collector.retry_delay", 1*time.Second)

	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("logging.output", "stdout")
}
