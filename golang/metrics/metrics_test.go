package metrics_test

import (
	"context"
	"strings"
	"testing"

	"iw-interview-review/golang/metrics"
)

func TestIngestMetrics(t *testing.T) {
	result, err := metrics.IngestMetrics(context.Background(), `{"project_id": "abc123", "metrics": {}, "trace_id": "xyz"}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result["status"] != 200 {
		t.Errorf("expected status 200, got %v", result["status"])
	}
	if result["project_id"] != "abc123" {
		t.Errorf("expected project_id abc123, got %v", result["project_id"])
	}
	fpID, _ := result["fingerprint_id"].(string)
	if !strings.HasPrefix(fpID, "fp_") {
		t.Errorf("expected fingerprint_id to start with fp_, got %v", fpID)
	}
	if _, ok := result["request_id"]; !ok {
		t.Error("expected request_id in response")
	}
}
