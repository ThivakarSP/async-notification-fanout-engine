-- name: GetUserPreferences :one
SELECT * FROM user_preferences
WHERE user_id = $1 AND tenant_id = $2 LIMIT 1;

-- name: UpsertUserPreferences :exec
INSERT INTO user_preferences (
    user_id, tenant_id, email_enabled, sms_enabled, push_enabled, quiet_hours_start, quiet_hours_end, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP
) ON CONFLICT (user_id) DO UPDATE SET
    email_enabled = EXCLUDED.email_enabled,
    sms_enabled = EXCLUDED.sms_enabled,
    push_enabled = EXCLUDED.push_enabled,
    quiet_hours_start = EXCLUDED.quiet_hours_start,
    quiet_hours_end = EXCLUDED.quiet_hours_end,
    updated_at = CURRENT_TIMESTAMP;

-- name: GetNotificationTemplate :one
SELECT * FROM notification_templates
WHERE event_type = $1 AND channel = $2 LIMIT 1;

-- name: CreateNotificationTemplate :one
INSERT INTO notification_templates (
    id, event_type, channel, subject_template, body_template
) VALUES (
    $1, $2, $3, $4, $5
) RETURNING *;

-- name: InsertDeliveryLog :one
INSERT INTO delivery_logs (
    event_id, user_id, channel, provider, status, error_message
) VALUES (
    $1, $2, $3, $4, $5, $6
) RETURNING *;
