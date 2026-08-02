package delivery

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/async-notification-fanout-engine/internal/queue"
)

var ErrMockRateLimit = errors.New("rate limited")
var ErrMockInvalid = errors.New("invalid address")

// MockEmailDelivery simulates sending an email via SendGrid
func MockEmailDelivery(ctx context.Context, payload queue.DeliveryPayload) error {
	log.Printf("[EMAIL] Sending to %s: Subject: %s", payload.TargetAddress, payload.Subject)
	time.Sleep(50 * time.Millisecond) // Simulate latency
	
	if payload.TargetAddress == "invalid@example.com" {
		return ErrMockInvalid
	}
	return nil
}

// MockSMSDelivery simulates sending an SMS via Twilio
func MockSMSDelivery(ctx context.Context, payload queue.DeliveryPayload) error {
	log.Printf("[SMS] Sending to %s: Body: %s", payload.TargetAddress, payload.Body)
	time.Sleep(20 * time.Millisecond) // Simulate latency
	
	if payload.TargetAddress == "+10000000000" {
		return ErrMockInvalid
	}
	// Let's pretend some rate limits happen occasionally
	// For testing, we could make it deterministic based on payload, but we'll leave it as success for now.
	return nil
}

// MockPushDelivery simulates sending a push notification via FCM
func MockPushDelivery(ctx context.Context, payload queue.DeliveryPayload) error {
	log.Printf("[PUSH] Sending to token %s: Body: %s", payload.TargetAddress, payload.Body)
	time.Sleep(10 * time.Millisecond) // Simulate latency
	
	if payload.TargetAddress == "invalid_token" {
		return ErrMockInvalid
	}
	return nil
}
