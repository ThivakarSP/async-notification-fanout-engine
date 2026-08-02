package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsIngested = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_ingested_total",
			Help: "Total number of events ingested",
		},
		[]string{"status"}, // accepted or duplicate
	)

	NotificationsDelivered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Total number of notifications processed by channel",
		},
		[]string{"channel", "status"}, // channel: email/sms/push, status: success/filtered/failed
	)

	NotificationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_delivery_latency_seconds",
			Help:    "Latency of notification delivery by channel",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)
)
