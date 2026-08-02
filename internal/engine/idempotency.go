package engine

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type IdempotencyEngine struct {
	client *redis.Client
}

func NewIdempotencyEngine(client *redis.Client) *IdempotencyEngine {
	return &IdempotencyEngine{client: client}
}

// CheckAndSet returns true if the key is new and was successfully set.
// It returns false if the key already exists (duplicate).
func (e *IdempotencyEngine) CheckAndSet(ctx context.Context, key string) (bool, error) {
	fullKey := "idempotency:" + key
	// TTL is 24 hours as per PRD
	success, err := e.client.SetNX(ctx, fullKey, "1", 24*time.Hour).Result()
	if err != nil {
		return false, err
	}
	return success, nil
}
