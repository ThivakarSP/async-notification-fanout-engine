package queue

import (
	"context"
	"encoding/json"

	"github.com/hibiken/asynq"
)

const (
	TaskEmailSend = "task:email:send"
	TaskSmsSend   = "task:sms:send"
	TaskPushSend  = "task:push:send"

	QueueEmail = "email"
	QueueSms   = "sms"
	QueuePush  = "push"
)

// DeliveryPayload represents the data needed by a channel delivery worker
type DeliveryPayload struct {
	EventID        string `json:"event_id"`
	IdempotencyKey string `json:"idempotency_key"`
	TenantID       string `json:"tenant_id"`
	UserID         string `json:"user_id"`
	TargetAddress  string `json:"target_address"` // e.g., email address, phone number, FCM token
	Subject        string `json:"subject,omitempty"`
	Body           string `json:"body"`
}

type Client struct {
	client *asynq.Client
}

func NewClient(redisOpt asynq.RedisConnOpt) *Client {
	return &Client{
		client: asynq.NewClient(redisOpt),
	}
}

func (c *Client) EnqueueDelivery(ctx context.Context, taskType string, qName string, payload DeliveryPayload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	task := asynq.NewTask(taskType, b)
	// PRD specifies Max 5 retries for transient errors.
	_, err = c.client.EnqueueContext(ctx, task, asynq.Queue(qName), asynq.MaxRetry(5))
	return err
}

func (c *Client) Close() error {
	return c.client.Close()
}
