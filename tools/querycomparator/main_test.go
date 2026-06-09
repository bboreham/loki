package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/storage/bucket/diskcache"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type bucketReaderAsBucket struct {
	objstore.BucketReader
}

func (b bucketReaderAsBucket) Close() error { return nil }

func (b bucketReaderAsBucket) Provider() objstore.ObjProvider { panic("not implemented") }
func (b bucketReaderAsBucket) Upload(_ context.Context, _ string, _ io.Reader) error {
	panic("not implemented")
}
func (b bucketReaderAsBucket) GetAndReplace(_ context.Context, _ string, _ func(io.ReadCloser) (io.ReadCloser, error)) error {
	panic("not implemented")
}
func (b bucketReaderAsBucket) Delete(_ context.Context, _ string) error { panic("not implemented") }
func (b bucketReaderAsBucket) Name() string                             { panic("not implemented") }

func TestDebugCmd(t *testing.T) {
	//t.Skip("Test for debugging purposes only")

	rawBucket := MustS3DataobjBucket("ops-eu-south-0-loki-ops-002-data", "s3.eu-south-2.amazonaws.com")
	cachedBucket := diskcache.New(rawBucket, "/tmp/lokicache")
	storageBucket := bucketReaderAsBucket{BucketReader: cachedBucket}

	indexStoragePrefix = "" // TODO: set index storage prefix for local engine support
	orgID = "29"            // TODO: set org ID for querying remote instances

	start := time.Date(2026, 4, 12, 6, 0, 0, 0, time.UTC)
	end := start.Add(1 * time.Minute)
	//level := "error"
	//query := `sum(avg_over_time({cluster="ops-eu-south-0", container="parallel-querier", job="loki-ops-002/parallel-querier-burst", name="parallel-querier-burst", namespace="loki-ops-002", service="parallel-querier", service_name="loki/parallel-querier"} | logfmt | duration != "" | unwrap duration(duration) [5m]))`
	query := "sum(count_over_time({cluster=\"ops-eu-south-0\", container=\"ruler\", gossip_ring_member=\"false\", insight=\"true\", job=\"mimir-ops-03/ruler-zone-a\", name=\"ruler-zone-a\", namespace=\"mimir-ops-03\", pod=\"ruler-zone-a-688f4fbd4f-rk24s\", service_name=\"mimir/ruler\"}[5m]))"

	params, err := logql.NewLiteralParams(query, start, end, 0, 0, logproto.BACKWARD, 1000, nil, nil)
	require.NoError(t, err)

	// Run subcommands as necessary
	// require.NoError(t, doComparison(params, "localhost:3101", "localhost:3102"))
	//require.NoError(t, queryMetastore(params))
	require.NoError(t, doExecuteLocallyV2Scheduler(params, storageBucket))
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
