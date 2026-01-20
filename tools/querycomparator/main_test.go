package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanpruningprocessor"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestDebugCmd(t *testing.T) {
	//t.Skip("Test for debugging purposes only")

	exporter, err := New()
	require.NoError(t, err)
	pruner := spanpruningprocessor.NewBatchSpanProcessor(exporter, []string{})
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(pruner))
	otel.SetTracerProvider(tp)

	storageBucket := MustGCSDataobjBucket("dev-us-central-0-loki-dev-005-data") // TODO: set bucket name for local engine support
	indexStoragePrefix = ""                                                     // TODO: set index storage prefix for local engine support
	orgID = "29"                                                                // TODO: set org ID for querying remote instances

	start := time.Date(2026, 4, 12, 6, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Minute)
	//level := "error"
	query := fmt.Sprintf(`{namespace="cortex-dev-01", pod="ingester-zone-a-1"}`) //  != "while ingesting write request"

	params, err := logql.NewLiteralParams(query, start, end, 0, 0, logproto.BACKWARD, 1000, nil, nil)
	require.NoError(t, err)

	// Run subcommands as necessary
	// require.NoError(t, doComparison(params, "localhost:3101", "localhost:3102"))
	//require.NoError(t, queryMetastore(params))
	require.NoError(t, doExecuteLocallyV2Scheduler(params, storageBucket))
	pruner.ForceFlush(t.Context())
	tp.Shutdown(t.Context())
}

func TestQueryComparatorCmd(t *testing.T) {
	t.Skip("Test for debugging purposes only")

	bucket := MustGCSDataobjBucket("")
	orgID = "" // TODO: set org ID for querying remote instances

	start := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Minute)
	query := `{container="distributor"}`

	params, err := logql.NewLiteralParams(query, start, end, 0, 0, logproto.BACKWARD, 1000, nil, nil)
	require.NoError(t, err)

	// Run subcommands as necessary
	require.NoError(t, doComparison(params, "localhost:3101", "localhost:3102"))
	require.NoError(t, queryMetastore(params, bucket))
	require.NoError(t, doExecuteLocallyV2SchedulerRemote(params, bucket))
}
