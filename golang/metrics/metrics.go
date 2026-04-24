package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// --- Fake dependencies (pretend these are real clients, correctly implemented) ---

type ProjectConfiguration struct {
	ProjectID          string
	Enabled            bool
	APIKey             string
	ContextWaitMs      int
	InferenceTimeoutMs int
}

type FingerprintRequest struct {
	Context map[string]any
	Metrics map[string]any
}

type FingerprintData struct {
	FingerprintID string
	CreatedAt     int64
	Data          FingerprintRequest
}

type DB struct {
	log *slog.Logger
}

func NewDB() *DB {
	return &DB{log: slog.With("component", "db")}
}

func (d *DB) GetProjectConfig(ctx context.Context, projectID string) (*ProjectConfiguration, error) {
	d.log.InfoContext(ctx, "fetching project configuration", "project_id", projectID)
	return &ProjectConfiguration{
		ProjectID:          projectID,
		Enabled:            true,
		APIKey:             "proj_secret",
		ContextWaitMs:      50,
		InferenceTimeoutMs: 200,
	}, nil
}

func (d *DB) SaveFingerprint(ctx context.Context, requestID, projectID string, data FingerprintData) error {
	// Can fail due to transient DB errors.
	d.log.InfoContext(ctx, "saving fingerprint",
		"request_id", requestID,
		"project_id", projectID,
		"fingerprint_id", data.FingerprintID,
		"created_at", data.CreatedAt,
	)
	// no-op
	return nil
}

type Redis struct {
	store map[string]string
	log   *slog.Logger
}

func NewRedis() *Redis {
	return &Redis{
		store: make(map[string]string),
		log:   slog.With("component", "redis"),
	}
}

func (r *Redis) Get(ctx context.Context, key string) (string, bool) {
	r.log.DebugContext(ctx, "get", "key", key)
	val, ok := r.store[key]
	return val, ok
}

type InferenceService struct {
	log *slog.Logger
}

func NewInferenceService() *InferenceService {
	return &InferenceService{log: slog.With("component", "inference")}
}

// Fingerprint calculates a fingerprint. This is a call to an external HTTP endpoint.
// Sometimes fails due to timeout.
func (s *InferenceService) Fingerprint(ctx context.Context, projectID string, req FingerprintRequest, timeoutMs int) (string, error) {
	fp := "fp_" + projectID
	s.log.InfoContext(ctx, "fingerprint_success", "project_id", projectID, "fingerprint_id", fp)
	return fp, nil
}

type DataQueueingService struct {
	log *slog.Logger
}

func NewDataQueueingService() *DataQueueingService {
	return &DataQueueingService{log: slog.With("component", "dqs")}
}

// Upload pushes arbitrary data attached to a projectID to downstream services which store and
// index it. This mechanism is backed by a queue. If the queue is unavailable, this will return
// an error.
func (s *DataQueueingService) Upload(ctx context.Context, projectID string, uploadData map[string]any) error {
	// no-op
	return nil
}

// --- End of dependencies ---

var (
	db        = NewDB()
	cache     = NewRedis()
	inference = NewInferenceService()
	dqs       = NewDataQueueingService()
)

// IngestMetrics ingests metrics from a JSON request body.
//
// requestBody is a string containing JSON. It should have a `project_id` and a large blob of
// data under the `metrics` key.
//
// This function will be called by a webapp endpoint.
//
// The steps for metrics ingestion are:
//  1. Fetch the project configuration to check if ingestion is enabled.
//  2. Fetch additional context from Redis. This is generated via a separate process and *SHOULD*
//     be available by the time this function is called.
//  3. Compute the fingerprint using the fingerprinting service.
//  4. Persist metrics and the fingerprint response to the database.
//  5. Upload the data to a downstream data queueing service.
//  6. Return a request_id to the caller.
//
// CRITICAL: The request_id must only be returned if the result was successfully saved to the DB.
func IngestMetrics(ctx context.Context, requestBody string) (map[string]any, error) {
	requestID := uuid.New().String()
	receivedAt := time.Now().Unix()

	log := slog.With(
		"component", "metrics_ingestion",
		"request_id", requestID,
		"received_at", receivedAt,
	)

	// Parse request
	var body map[string]any
	if err := json.Unmarshal([]byte(requestBody), &body); err != nil {
		_ = fmt.Errorf("invalid request body: %w", err)
	}

	projectID, _ := body["project_id"].(string)
	metrics, _ := body["metrics"].(map[string]any)
	traceID, _ := body["trace_id"].(string)

	log = log.With("project_id", projectID, "trace_id", traceID)
	log.Debug("request received")

	// Load config
	cfg, err := db.GetProjectConfig(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch project config: %w", err)
	}
	if !cfg.Enabled {
		log.Warn("project disabled")
		return map[string]any{"status": 403, "error": "disabled"}, nil
	}

	// Fetch context from Redis (may not be ready yet)
	var extraCtx map[string]any
	if traceID != "" {
		key := "ctx:" + traceID
		rawData, _ := cache.Get(ctx, key)
		if rawData == "" {
			log.Info("waiting for missing context", "wait_ms", cfg.ContextWaitMs)
			time.Sleep(time.Duration(cfg.ContextWaitMs) * time.Millisecond)
			rawData, _ = cache.Get(ctx, key)
		}
		if rawData != "" {
			if err := json.Unmarshal([]byte(rawData), &extraCtx); err != nil {
				extraCtx = map[string]any{}
			}
		}
	}

	// Call inference service
	fingerprintID, err := inference.Fingerprint(ctx, projectID, FingerprintRequest{
		Metrics: metrics,
		Context: extraCtx,
	}, cfg.InferenceTimeoutMs)
	if err != nil {
		log.Error("failed to call inference service")
		return map[string]any{
			"status":     200,
			"request_id": requestID,
			"error":      "inference failed",
		}, nil
	}

	// Persist fingerprint (must succeed before returning request_id)
	if err := db.SaveFingerprint(ctx, requestID, projectID, FingerprintData{
		FingerprintID: fingerprintID,
		CreatedAt:     receivedAt,
		Data: FingerprintRequest{
			Metrics: metrics,
			Context: extraCtx,
		},
	}); err != nil {
		log.Error("failed to save fingerprint", "error", err)
	}

	payload := map[string]any{
		"request_id":     requestID,
		"received_at":    receivedAt,
		"project_id":     projectID,
		"trace_id":       traceID,
		"fingerprint_id": fingerprintID,
		"metrics":        metrics,
		"context":        extraCtx,
	}
	dqs.Upload(ctx, projectID, payload)

	log.Info("metrics ingestion complete")
	return map[string]any{
		"status":         200,
		"project_id":     projectID,
		"request_id":     requestID,
		"fingerprint_id": fingerprintID,
	}, nil
}
