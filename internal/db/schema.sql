-- User Notification Preferences
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    email_enabled BOOLEAN DEFAULT TRUE,
    sms_enabled BOOLEAN DEFAULT TRUE,
    push_enabled BOOLEAN DEFAULT TRUE,
    quiet_hours_start TIME,
    quiet_hours_end TIME,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Notification Templates
CREATE TABLE IF NOT EXISTS notification_templates (
    id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    channel VARCHAR(16) NOT NULL, -- 'EMAIL', 'SMS', 'PUSH', 'WEBHOOK'
    subject_template TEXT,
    body_template TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_event_channel UNIQUE (event_type, channel)
);

-- Audit / Delivery Logs
CREATE TABLE IF NOT EXISTS delivery_logs (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL, -- 'DELIVERED', 'FAILED', 'FILTERED'
    error_message TEXT,
    delivered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_delivery_logs_event_id ON delivery_logs(event_id);
CREATE INDEX idx_delivery_logs_user_id ON delivery_logs(user_id);
