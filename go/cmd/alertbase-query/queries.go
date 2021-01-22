package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"go.uber.org/zap"
)

func queryCandidate(ctx context.Context, db *alertdb.Database, candidateID uint64) error {
	ctxlog.Info(ctx, "querying candidate", zap.Uint64("candidateID", candidateID))
	alert, err := db.GetByCandidateID(ctx, candidateID)
	if err != nil {
		return err
	}
	printAlert(alert)
	return nil
}

func queryObject(ctx context.Context, db *alertdb.Database, object string) error {
	ctxlog.Info(ctx, "querying object", zap.String("object", object))
	alerts, err := db.GetByObjectID(ctx, object)
	if err != nil {
		return err
	}
	ctxlog.Debug(ctx, "received alerts", zap.Int("n-alerts", len(alerts)))
	printAlerts(alerts)
	return nil
}

func queryTimerange(ctx context.Context, db *alertdb.Database, start, end, format string) error {
	ctxlog.Info(ctx, "querying time range",
		zap.String("start", start),
		zap.String("end", end),
		zap.String("format", format),
	)
	if format != "jd" {
		return fmt.Errorf("format not implemented: %q", format)
	}
	startFloat, err := strconv.ParseFloat(start, 64)
	if err != nil {
		return err
	}
	endFloat, err := strconv.ParseFloat(end, 64)
	if err != nil {
		return err
	}

	results := make(chan *schema.Alert, 100)
	go func() {
		err = db.StreamByTimerange(ctx, startFloat, endFloat, results)
	}()
	for alert := range results {
		printAlert(alert)
	}
	if err != nil {
		return err
	}
	return nil
}
