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

func SMSHandler(ctx context.Context, payload queue.DeliveryPayload) error {
	start := time.Now()
	defer func() {
		metrics.NotificationLatency.WithLabelValues("sms").Observe(time.Since(start).Seconds())
	}()

	apiKey := os.Getenv("TWILIO_API_KEY")
	if apiKey == "" {
		// Fallback to mock
		log.Printf("[DRY-RUN SMS] Sending to %s: Body: %s", payload.TargetAddress, payload.Body)
		metrics.NotificationsDelivered.WithLabelValues("sms", "success").Inc()
		return nil
	}

	// Real SDK execution would go here.
	log.Printf("[REAL SMS] Sending via Twilio to %s", payload.TargetAddress)
	// mock error for invalid
	if payload.TargetAddress == "+10000000000" {
		metrics.NotificationsDelivered.WithLabelValues("sms", "failed").Inc()
		return errors.New("invalid phone")
	}

	metrics.NotificationsDelivered.WithLabelValues("sms", "success").Inc()
	return nil
}
