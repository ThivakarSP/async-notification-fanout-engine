package engine

import (
	"context"
	"encoding/json"
	"log"

	"github.com/async-notification-fanout-engine/internal/db"
	"github.com/async-notification-fanout-engine/internal/metrics"
	"github.com/async-notification-fanout-engine/internal/models"
	"github.com/async-notification-fanout-engine/internal/queue"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FanoutWorker struct {
	queries     *db.Queries
	queueClient *queue.Client
	templates   *TemplateEngine
}

func NewFanoutWorker(dbPool *pgxpool.Pool, qc *queue.Client, te *TemplateEngine) *FanoutWorker {
	return &FanoutWorker{
		queries:     db.New(dbPool),
		queueClient: qc,
		templates:   te,
	}
}

// ProcessRawEvent handles messages from the Kafka raw-events topic
func (f *FanoutWorker) ProcessRawEvent(ctx context.Context, key []byte, value []byte) error {
	var event models.IngestEventRequest
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("Invalid event payload: %v", err)
		return nil // Drop invalid payloads, do not retry
	}

	// 1. Resolve Audience
	// In a real system, if "BROADCAST", we would query a user-segment service.
	// For this exercise, we will just use the DIRECT user_ids.
	userIDs := event.Recipients.UserIDs
	if event.Recipients.Type == "BROADCAST" {
		// Mock segment resolution
		userIDs = []string{"user_1", "user_2"}
	}

	for _, userID := range userIDs {
		// 2. Filter Preferences
		prefs, err := f.queries.GetUserPreferences(ctx, db.GetUserPreferencesParams{
			UserID:   userID,
			TenantID: event.TenantID,
		})
		if err != nil {
			// If no preferences found, assume default (true).
			// Let's create a default prefs struct for fallback
			prefs = db.UserPreference{
				EmailEnabled: pgtype.Bool{Bool: true, Valid: true},
				SmsEnabled:   pgtype.Bool{Bool: true, Valid: true},
				PushEnabled:  pgtype.Bool{Bool: true, Valid: true},
			}
		}

		// 3. Hydrate Templates and Queue
		f.enqueueIfEnabled(ctx, event, userID, "EMAIL", prefs.EmailEnabled.Bool)
		f.enqueueIfEnabled(ctx, event, userID, "SMS", prefs.SmsEnabled.Bool)
		f.enqueueIfEnabled(ctx, event, userID, "PUSH", prefs.PushEnabled.Bool)
	}

	return nil
}

func (f *FanoutWorker) enqueueIfEnabled(ctx context.Context, event models.IngestEventRequest, userID string, channel string, enabled bool) {
	if !enabled {
		metrics.NotificationsDelivered.WithLabelValues(channel, "filtered").Inc()
		return // Filtered out by user preference
	}

	// In a real scenario, we would also check quiet hours here.

	// 1. Fetch template for event type and channel
	tmpl, err := f.queries.GetNotificationTemplate(ctx, db.GetNotificationTemplateParams{
		EventType: event.EventType,
		Channel:   channel,
	})
	if err != nil {
		log.Printf("Template not found for %s on %s", event.EventType, channel)
		return
	}

	// 2. Render Template
	body, err := f.templates.Render(tmpl.BodyTemplate, event.Payload)
	if err != nil {
		log.Printf("Failed to render body template: %v", err)
		return
	}

	var subject string
	if tmpl.SubjectTemplate.Valid {
		subject, _ = f.templates.Render(tmpl.SubjectTemplate.String, event.Payload)
	}

	// 3. Create Delivery Payload
	payload := queue.DeliveryPayload{
		EventID:        event.IdempotencyKey,
		IdempotencyKey: event.IdempotencyKey,
		TenantID:       event.TenantID,
		UserID:         userID,
		TargetAddress:  "mock_" + channel + "_" + userID, // In reality, fetch from user profile
		Subject:        subject,
		Body:           body,
	}

	// 4. Enqueue
	var qName, taskType string
	switch channel {
	case "EMAIL":
		qName = queue.QueueEmail
		taskType = queue.TaskEmailSend
	case "SMS":
		qName = queue.QueueSms
		taskType = queue.TaskSmsSend
	case "PUSH":
		qName = queue.QueuePush
		taskType = queue.TaskPushSend
	}

	if err := f.queueClient.EnqueueDelivery(ctx, taskType, qName, payload); err != nil {
		log.Printf("Failed to enqueue %s for user %s: %v", channel, userID, err)
	}
}
