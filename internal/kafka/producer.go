package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

const RawEventsTopic = "raw-events-ingest"

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    RawEventsTopic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{writer: w}
}

func (p *Producer) PublishEvent(ctx context.Context, key string, payload []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
	}
	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
