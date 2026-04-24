package metrics

import (
	"context"
	"errors"
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
func IngestMetrics(ctx context.Context, requestBody string) (map[string]any, error) {
	requestID := uuid.New().String()
	receivedAt := time.Now().Unix()

	log := slog.With(
		"component", "metrics_ingestion",
		"request_id", requestID,
		"received_at", receivedAt,
	)
	_ = log

	return nil, errors.New("not implemented")
}
