package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

var (
	objectID    = flag.String("object", "", "query by object ID")
	candidateID = flag.Uint64("candidate", 0, "query by candidate ID")

	timeStart  = flag.String("time-start", "", "query by time range: start time")
	timeEnd    = flag.String("time-end", "", "query by time range: end time")
	timeFormat = flag.String("time-format", "jd", "format of time inputs (can be 'jd', 'unixnano', or 'rfc3339'")

	dbPath = flag.String("db", "alerts.db", "path to alerts database")
	bucket = flag.String("bucket", "ztf-alert-archive-prototyping", "s3 bucket containing alert data")

	platform = flag.String("platform", "aws", "platform (can be 'aws' or 'google'")
)

func main() {
	flag.Parse()

	ctx := context.Background()
	log, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	ctx = ctxlog.WithLog(ctx, log)
	log.Debug("starting up",
		zap.Stringp("-object", objectID),
		zap.Uint64p("-candidate", candidateID),
		zap.Stringp("-time-start", timeStart),
		zap.Stringp("-time-end", timeEnd),
		zap.Stringp("-time-format", timeFormat),
		zap.Stringp("-db", dbPath),
		zap.Stringp("-bucket", bucket),
		zap.Stringp("-platform", platform),
	)

	query, err := parseQueryType()
	if err != nil {
		fatal(log, err)
	}

	session, err := session.NewSession()
	if err != nil {
		fatal(log, err)
	}
	var db *alertdb.Database
	switch *platform {
	case "aws":
		s3 := s3.New(session, aws.NewConfig().WithRegion("us-west-2"))
		db, err = alertdb.NewS3Database(*dbPath, *bucket, s3)
		if err != nil {
			fatal(log, err)
		}
	// case "google":
	// 	gcs, err := storage.NewClient(ctx)
	// 	if err != nil {
	// 		fatal(log, err)
	// 	}
	// 	db, err = alertdb.NewGoogleCloudDatabase(*dbPath, *bucket, gcs)
	// 	if err != nil {
	// 		fatal(log, err)
	// 	}
	default:
		fatal(log, errors.New("invalid platform"))
	}
	defer db.Close()

	switch query {
	case candidate:
		err = queryCandidate(ctx, db, *candidateID)
	case object:
		err = queryObject(ctx, db, *objectID)
	case timerange:
		err = queryTimerange(ctx, db, *timeStart, *timeEnd, *timeFormat)
	}
	if err != nil {
		fatal(log, err)
	}
}

func fatal(log *zap.Logger, err error) {
	log.Fatal("Fatal error", zap.Error(err))
}

type queryType int

const (
	unknown queryType = iota
	candidate
	object
	timerange
)

func parseQueryType() (queryType, error) {
	var queried queryType = unknown
	if *objectID != "" {
		queried = object
	}

	if *candidateID != 0 {
		if queried != unknown {
			return unknown, fmt.Errorf("exactly one query filter must be specified")
		}
		queried = candidate
	}

	if *timeStart != "" || *timeEnd != "" {
		if *timeStart == "" || *timeEnd == "" {
			return unknown, fmt.Errorf("both -time-start and -time-end must be specified")
		}
		if queried != unknown {
			return unknown, fmt.Errorf("exactly one query filter must be specified")
		}
		queried = timerange
	}

	if queried == unknown {
		return unknown, fmt.Errorf("exactly one query filter must be specified")
	}
	return queried, nil
}
