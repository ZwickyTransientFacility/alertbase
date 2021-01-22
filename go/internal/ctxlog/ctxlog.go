// package ctxlog provides utilities for passing a Zap logger around through
// contexts.
package ctxlog

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerkeyType struct{}

var loggerkey loggerkeyType

var skip1 = zap.AddCallerSkip(1)

// Logger returns the logger inserted into a given context. It will panic if the
// logger has not been set into the context.
func Logger(ctx context.Context) *zap.Logger {
	return ctx.Value(loggerkey).(*zap.Logger)
}

// WithLog returns a new context with the given logger inserted.
func WithLog(ctx context.Context, log *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerkey, log)
}

// WithFields returns a new context, augmenting the given context's logger to
// attach more fields.
func WithFields(ctx context.Context, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, loggerkey, Logger(ctx).With(fields...))
}

// Info logs a message using the context's logger at the INFO level.
func Info(ctx context.Context, msg string, fields ...zapcore.Field) {
	Logger(ctx).WithOptions(skip1).Info(msg, fields...)
}

// Debug logs a message using the context's logger at the DEBUG level.
func Debug(ctx context.Context, msg string, fields ...zapcore.Field) {
	Logger(ctx).WithOptions(skip1).Debug(msg, fields...)
}

// Warn logs a message using the context's logger at the WARN level.
func Warn(ctx context.Context, msg string, fields ...zapcore.Field) {
	Logger(ctx).WithOptions(skip1).Warn(msg, fields...)
}

// Error logs a message using the context's logger at the ERROR level.
func Error(ctx context.Context, msg string, fields ...zapcore.Field) {
	Logger(ctx).WithOptions(skip1).Error(msg, fields...)
}
