package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/async-notification-fanout-engine/internal/engine"
	"github.com/async-notification-fanout-engine/internal/kafka"
	"github.com/async-notification-fanout-engine/internal/provider"
	"github.com/async-notification-fanout-engine/internal/queue"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	kafka_go "github.com/segmentio/kafka-go"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Init Dependencies
	dbPool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/fanout_db?sslmode=disable")
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbPool.Close()

	redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}
	queueClient := queue.NewClient(redisOpt)
	defer queueClient.Close()

	templateEngine := engine.NewTemplateEngine()

	// 2. Start Fanout Worker (Kafka Consumer)
	fanout := engine.NewFanoutWorker(dbPool, queueClient, templateEngine)
	consumer := kafka.NewConsumer([]string{"localhost:9094"}, "fanout-group")
	
	go func() {
		log.Println("Starting Kafka Fanout Consumer...")
		consumer.Start(ctx, func(c context.Context, msg kafka_go.Message) error {
			return fanout.ProcessRawEvent(c, msg.Key, msg.Value)
		})
	}()

	// 3. Start Queue Delivery Processor (Asynq Workers)
	processor := queue.NewProcessor(redisOpt)
	processor.RegisterHandlers(
		provider.EmailHandler,
		provider.SMSHandler,
		provider.PushHandler,
	)

	go func() {
		log.Println("Starting Delivery Queue Processor...")
		if err := processor.Start(); err != nil {
			log.Fatalf("Processor failed: %v", err)
		}
	}()

	// Wait for interrupt
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("Shutting down...")
	
	cancel()
	processor.Stop()
	consumer.Close()
}
