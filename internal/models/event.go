package models

import "encoding/json"

type IngestEventRequest struct {
	IdempotencyKey   string                 `json:"idempotency_key"`
	EventType        string                 `json:"event_type"`
	TenantID         string                 `json:"tenant_id"`
	Recipients       Recipients             `json:"recipients"`
	Payload          map[string]interface{} `json:"payload"`
	ChannelsOverride []string               `json:"channels_override,omitempty"`
}

type Recipients struct {
	Type      string   `json:"type"` // "DIRECT" or "BROADCAST"
	UserIDs   []string `json:"user_ids,omitempty"`
	SegmentID string   `json:"segment_id,omitempty"`
}

type BroadcastEventRequest struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	EventType      string                 `json:"event_type"`
	TenantID       string                 `json:"tenant_id"`
	SegmentID      string                 `json:"segment_id"`
	Payload        map[string]interface{} `json:"payload"`
}

func (req *IngestEventRequest) Bytes() []byte {
	b, _ := json.Marshal(req)
	return b
}
