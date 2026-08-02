package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    RawEventsTopic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return &Consumer{reader: r}
}

func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, msg kafka.Message) error) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("error fetching message: %v", err)
			if ctx.Err() != nil {
				return // Context cancelled
			}
			continue
		}

		if err := handler(ctx, m); err != nil {
			log.Printf("error handling message %s: %v", string(m.Key), err)
			// Decide whether to commit based on error, here we could implement a DLQ for poison pills
		}

		// Commit after successful processing or permanent failure
		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("failed to commit message: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
