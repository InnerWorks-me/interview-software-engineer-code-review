package metrics_test

import (
	"context"
	"testing"

	"iw-interview-review/golang/metrics"
)

func TestIngestMetrics_NotImplemented(t *testing.T) {
	_, err := metrics.IngestMetrics(context.Background(), `{"project_id": "abc123", "metrics": {}}`)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
