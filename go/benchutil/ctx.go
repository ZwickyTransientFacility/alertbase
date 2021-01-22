package benchutil

import (
	"context"
	"time"
)

type loggerkeyType struct{}

var loggerkey loggerkeyType

// Logger returns the logger inserted into a given context. It will panic if the
// logger has not been set into the context.
func Logger(ctx context.Context) *BenchmarkLogger {
	return ctx.Value(loggerkey).(*BenchmarkLogger)
}

// WithLog returns a new context with the given logger inserted.
func WithBenchmarker(ctx context.Context, log *BenchmarkLogger) context.Context {
	return context.WithValue(ctx, loggerkey, log)
}

func ObserveInt(ctx context.Context, qty int, label string) error {
	return Logger(ctx).ObserveInt(qty, label)
}

func ObserveDuration(ctx context.Context, dur time.Duration, label string) error {
	return Logger(ctx).ObserveDuration(dur, label)
}

func StartObservation(ctx context.Context, obsLabel, label string) (stopper func() error, err error) {
	return Logger(ctx).StartObservation(obsLabel, label)
}
