package main

import (
	"log"
	"net/http"

	"github.com/async-notification-fanout-engine/internal/api"
	"github.com/async-notification-fanout-engine/internal/engine"
	"github.com/async-notification-fanout-engine/internal/kafka"
	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 2. Initialize Engines
	idempotency := engine.NewIdempotencyEngine(rdb)
	producer := kafka.NewProducer([]string{"localhost:9094"})
	defer producer.Close()

	// 3. Initialize API Server
	srv := api.NewServer(idempotency, producer)

	log.Println("Starting Ingestion API Server on :8080")
	if err := http.ListenAndServe(":8080", srv); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
