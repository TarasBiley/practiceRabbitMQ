package config

import "os"

type Config struct {
	DatabaseURL string
	RabbitURL   string
	HTTPAddr    string
}

func LoadConfig() Config {
	return Config{
		DatabaseURL: getEnv(
			"DATABASE_URL",
			"postgres://app:app@localhost:5434/notifications?sslmode=disable",
		),

		RabbitURL: getEnv(
			"RABBITMQ_URL",
			"amqp://guest:guest@localhost:5672/",
		),

		HTTPAddr: getEnv(
			"HTTP_ADDR",
			":8080",
		),
	}
}

func getEnv(key string, defaultValue string) string {
	value := os.Getenv(key)

	if value == "" {
		return defaultValue
	}

	return value
}
