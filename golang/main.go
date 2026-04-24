package main

import (
	"context"
	"fmt"

	"iw-interview-review/golang/metrics"
)

func main() {
	result, err := metrics.IngestMetrics(context.Background(), `{"project_id": "foobar", "metrics": {}}`)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(result)
}
