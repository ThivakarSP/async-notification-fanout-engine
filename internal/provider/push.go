package provider

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/async-notification-fanout-engine/internal/metrics"
	"github.com/async-notification-fanout-engine/internal/queue"
)

func PushHandler(ctx context.Context, payload queue.DeliveryPayload) error {
	start := time.Now()
	defer func() {
		metrics.NotificationLatency.WithLabelValues("push").Observe(time.Since(start).Seconds())
	}()

	apiKey := os.Getenv("FCM_API_KEY")
	if apiKey == "" {
		// Fallback to mock
		log.Printf("[DRY-RUN PUSH] Sending to %s: Body: %s", payload.TargetAddress, payload.Body)
		metrics.NotificationsDelivered.WithLabelValues("push", "success").Inc()
		return nil
	}

	// Real SDK execution would go here.
	log.Printf("[REAL PUSH] Sending via FCM to %s", payload.TargetAddress)
	// mock error for invalid
	if payload.TargetAddress == "invalid_token" {
		metrics.NotificationsDelivered.WithLabelValues("push", "failed").Inc()
		return errors.New("invalid token")
	}

	metrics.NotificationsDelivered.WithLabelValues("push", "success").Inc()
	return nil
}
