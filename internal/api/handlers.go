package api

import (
	"encoding/json"
	"net/http"

	"github.com/async-notification-fanout-engine/internal/engine"
	"github.com/async-notification-fanout-engine/internal/kafka"
	"github.com/async-notification-fanout-engine/internal/metrics"
	"github.com/async-notification-fanout-engine/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	router      *chi.Mux
	idempotency *engine.IdempotencyEngine
	producer    *kafka.Producer
}

func NewServer(idempotency *engine.IdempotencyEngine, producer *kafka.Producer) *Server {
	s := &Server{
		router:      chi.NewRouter(),
		idempotency: idempotency,
		producer:    producer,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.router.Post("/v1/events", s.handleIngestEvent)
	s.router.Post("/v1/broadcast", s.handleBroadcastEvent)
	
	// Metrics & Health
	s.router.Handle("/metrics", promhttp.Handler())
	s.router.Get("/healthz", s.handleHealthz)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	// In a real implementation we would ping Redis, Kafka, and PostgreSQL.
	// For this exercise, we just return a 200 OK assuming if the server is up, it's healthy.
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) handleIngestEvent(w http.ResponseWriter, r *http.Request) {
	var req models.IngestEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 1. Idempotency Check
	isNew, err := s.idempotency.CheckAndSet(r.Context(), req.IdempotencyKey)
	if err != nil {
		http.Error(w, "Idempotency engine error", http.StatusInternalServerError)
		return
	}
	if !isNew {
		// Duplicate Suppressed
		metrics.EventsIngested.WithLabelValues("duplicate").Inc()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "SUPPRESSED", "message": "Duplicate event"})
		return
	}

	// 2. Publish to Kafka
	if err := s.producer.PublishEvent(r.Context(), req.IdempotencyKey, req.Bytes()); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	metrics.EventsIngested.WithLabelValues("accepted").Inc()

	// 3. Return 202 Accepted
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":                 "ACCEPTED",
		"idempotency_key":        req.IdempotencyKey,
		"estimated_fanout_count": len(req.Recipients.UserIDs),
	})
}

func (s *Server) handleBroadcastEvent(w http.ResponseWriter, r *http.Request) {
	var req models.BroadcastEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Translate broadcast to ingest event format
	ingestReq := models.IngestEventRequest{
		IdempotencyKey: req.IdempotencyKey,
		EventType:      req.EventType,
		TenantID:       req.TenantID,
		Recipients: models.Recipients{
			Type:      "BROADCAST",
			SegmentID: req.SegmentID,
		},
		Payload: req.Payload,
	}

	isNew, err := s.idempotency.CheckAndSet(r.Context(), req.IdempotencyKey)
	if err != nil {
		http.Error(w, "Idempotency engine error", http.StatusInternalServerError)
		return
	}
	if !isNew {
		metrics.EventsIngested.WithLabelValues("duplicate").Inc()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "SUPPRESSED", "message": "Duplicate event"})
		return
	}

	if err := s.producer.PublishEvent(r.Context(), req.IdempotencyKey, ingestReq.Bytes()); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}
	
	metrics.EventsIngested.WithLabelValues("accepted").Inc()

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "ACCEPTED",
		"idempotency_key": req.IdempotencyKey,
	})
}
