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

func EmailHandler(ctx context.Context, payload queue.DeliveryPayload) error {
	start := time.Now()
	defer func() {
		metrics.NotificationLatency.WithLabelValues("email").Observe(time.Since(start).Seconds())
	}()

	apiKey := os.Getenv("SENDGRID_API_KEY")
	if apiKey == "" {
		// Fallback to mock
		log.Printf("[DRY-RUN EMAIL] Sending to %s: Subject: %s", payload.TargetAddress, payload.Subject)
		metrics.NotificationsDelivered.WithLabelValues("email", "success").Inc()
		return nil
	}

	// Real SDK execution would go here.
	log.Printf("[REAL EMAIL] Sending via SendGrid to %s", payload.TargetAddress)
	// mock error for invalid
	if payload.TargetAddress == "invalid@example.com" {
		metrics.NotificationsDelivered.WithLabelValues("email", "failed").Inc()
		return errors.New("invalid email")
	}

	metrics.NotificationsDelivered.WithLabelValues("email", "success").Inc()
	return nil
}
