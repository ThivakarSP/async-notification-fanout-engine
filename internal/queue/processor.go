package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

type Processor struct {
	server *asynq.Server
	mux    *asynq.ServeMux
}

func NewProcessor(redisOpt asynq.RedisConnOpt) *Processor {
	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			// Rate limits as per PRD:
			// Twilio (SMS): Max 100/sec
			// SendGrid (Email): Max 500/sec
			// FCM (Push): Max 5000/sec
			// The concurrency here defines how many goroutines are processing tasks.
			// True rate limiting is best done via external libraries or Asynq PRO, but we can approximate
			// by using concurrency limits per queue.
			Concurrency: 1000,
			Queues: map[string]int{
				QueuePush:  6, // Relative weights
				QueueEmail: 3,
				QueueSms:   1,
			},
			StrictPriority: false,
		},
	)

	mux := asynq.NewServeMux()
	return &Processor{
		server: srv,
		mux:    mux,
	}
}

// RegisterHandlers registers handlers for different task types.
func (p *Processor) RegisterHandlers(emailHandler, smsHandler, pushHandler func(context.Context, DeliveryPayload) error) {
	p.mux.HandleFunc(TaskEmailSend, p.wrapHandler(emailHandler))
	p.mux.HandleFunc(TaskSmsSend, p.wrapHandler(smsHandler))
	p.mux.HandleFunc(TaskPushSend, p.wrapHandler(pushHandler))
}

func (p *Processor) wrapHandler(handler func(context.Context, DeliveryPayload) error) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, t *asynq.Task) error {
		var payload DeliveryPayload
		if err := json.Unmarshal(t.Payload(), &payload); err != nil {
			return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
		}
		
		// If handler returns an error, asynq will automatically backoff and retry
		// up to the MaxRetry specified during enqueue.
		if err := handler(ctx, payload); err != nil {
			// In PRD: "Permanent Errors (HTTP 400, 401, Invalid Phone/Email): Move immediately to Dead Letter Queue"
			// The handler should return asynq.SkipRetry for those, so Asynq moves them to the exhausted/DLQ state immediately.
			log.Printf("Handler error for task %s (Event %s): %v", t.Type(), payload.EventID, err)
			return err
		}
		return nil
	}
}

func (p *Processor) Start() error {
	return p.server.Start(p.mux)
}

func (p *Processor) Stop() {
	// Waits for workers to finish current tasks before shutting down
	p.server.Stop()
	time.Sleep(1 * time.Second) // Give it a moment to flush
}
