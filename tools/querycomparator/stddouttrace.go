// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/semconv/v1.37.0/otelconv"
)

var zeroTime time.Time

var _ trace.SpanExporter = &Exporter{}

// New creates an Exporter with the passed options.
func New(options ...Option) (*Exporter, error) {
	cfg := newConfig(options...)

	exporter := &Exporter{
		writer:     cfg.Writer,
		timestamps: cfg.Timestamps,
	}

	return exporter, nil
}

// Exporter is an implementation of trace.SpanSyncer that writes spans to stdout.
type Exporter struct {
	writer     io.Writer
	encoderMu  sync.Mutex
	timestamps bool

	stoppedMu sync.RWMutex
	stopped   bool

	selfObservabilityEnabled bool
	selfObservabilityAttrs   []attribute.KeyValue // selfObservability common attributes
	selfObservabilitySetOpt  metric.MeasurementOption
	spanInflightMetric       otelconv.SDKExporterSpanInflight
	spanExportedMetric       otelconv.SDKExporterSpanExported
	operationDurationMetric  otelconv.SDKExporterOperationDuration
}

var (
	measureAttrsPool = sync.Pool{
		New: func() any {
			// "component.name" + "component.type" + "error.type"
			const n = 1 + 1 + 1
			s := make([]attribute.KeyValue, 0, n)
			// Return a pointer to a slice instead of a slice itself
			// to avoid allocations on every call.
			return &s
		},
	}

	addOptPool = &sync.Pool{
		New: func() any {
			const n = 1 // WithAttributeSet
			o := make([]metric.AddOption, 0, n)
			return &o
		},
	}

	recordOptPool = &sync.Pool{
		New: func() any {
			const n = 1 // WithAttributeSet
			o := make([]metric.RecordOption, 0, n)
			return &o
		},
	}
)

// ExportSpans writes spans in json format to stdout.
func (e *Exporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.stoppedMu.RLock()
	stopped := e.stopped
	e.stoppedMu.RUnlock()
	if stopped {
		return nil
	}

	if len(spans) == 0 {
		return nil
	}

	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)

	e.encoderMu.Lock()
	defer e.encoderMu.Unlock()
	// Encode span stubs, one by one
	for i := range stubs {
		stub := &stubs[i]
		// Remove timestamps
		if !e.timestamps {
			stub.StartTime = zeroTime
			stub.EndTime = zeroTime
			for j := range stub.Events {
				ev := &stub.Events[j]
				ev.Time = zeroTime
			}
		}

		// {"Name":"cloud.google.com/go/storage.Object.Attrs","SpanContext":{"TraceID":"bb4bdeb0cf744d2b2ead82bbe088b892","SpanID":"09ec67566d654a48","TraceFlags":"01","TraceState":"","Remote":false},"Parent":{"TraceID":"00000000000000000000000000000000","SpanID":"0000000000000000","TraceFlags":"00","TraceState":"","Remote":false},"SpanKind":1,"StartTime":"2026-01-16T11:25:45.566660027Z","EndTime":"2026-01-16T11:25:45.70266542Z","Attributes":null,"Events":null,"Links":null,"Status":{"Code":"Unset","Description":""},"DroppedAttributes":0,"DroppedEvents":0,"DroppedLinks":0,"ChildSpanCount":1,"Resource":[{"Key":"service.name","Value":{"Type":"STRING","Value":"unknown_service:querycomparator.test"}},{"Key":"telemetry.sdk.language","Value":{"Type":"STRING","Value":"go"}},{"Key":"telemetry.sdk.name","Value":{"Type":"STRING","Value":"opentelemetry"}},{"Key":"telemetry.sdk.version","Value":{"Type":"STRING","Value":"1.39.0"}}],"InstrumentationScope":{"Name":"cloud.google.com/go","Version":"","SchemaURL":"","Attributes":null},"InstrumentationLibrary":{"Name":"cloud.google.com/go","Version":"","SchemaURL":"","Attributes":null}}
		// {name: "ReadRanges", id: "a817ad58651d3f8a", parentID: "b30cea768446e771", startTime: t("2026-01-13T14:03:42.424323699Z"), endTime: t("2026-01-13T14:03:42.568654718Z"), ended: true},
		fmt.Fprintf(e.writer, "{name: %q, traceId: %q, spanId: %q, parentID: %q, startTime: %q, endTime: %q, resource: %v, attributes: %v, events: %v}\n", stub.Name, stub.SpanContext.TraceID(), stub.SpanContext.SpanID(), stub.Parent.SpanID(), stub.StartTime.Format(time.RFC3339), stub.EndTime.Format(time.RFC3339), stub.Resource.String(), kvsToString(stub.Attributes), stub.Events)
	}
	return err
}

// Shutdown is called to stop the exporter, it performs no action.
func (e *Exporter) Shutdown(context.Context) error {
	e.stoppedMu.Lock()
	e.stopped = true
	e.stoppedMu.Unlock()

	return nil
}

// MarshalLog is the marshaling function used by the logging system to represent this Exporter.
func (e *Exporter) MarshalLog() any {
	return struct {
		Type           string
		WithTimestamps bool
	}{
		Type:           "stdout",
		WithTimestamps: e.timestamps,
	}
}

func kvsToString(kvs []attribute.KeyValue) string {
	if len(kvs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("{")
	for i, kv := range kvs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprint(kv.Key))
		b.WriteString(": ")
		b.WriteString(fmt.Sprint(kv.Value.AsInterface()))
	}
	b.WriteString("}")
	return b.String()
}
