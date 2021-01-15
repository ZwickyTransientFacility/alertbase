package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.uber.org/zap"
)

const (
	bucket    = "ztf-alert-archive-prototyping"
	awsRegion = "us-west-2"
)

func printusage() {
	fmt.Println(`usage: alertbase-ingest DB-DIR ALERT-FILE-GLOB

DB-DIR should be a leveldb database directory
ALERT-FILE-GLOB should match avro-encoded files containing one or more alerts`)
}

func main() {
	if len(os.Args) != 3 {
		printusage()
		os.Exit(1)
	}

	log, err := zap.NewDevelopment()
	defer log.Sync()

	db := os.Args[1]
	glob := os.Args[2]

	log.Info("ingesting",
		zap.String("db", db),
		zap.String("glob", glob),
	)

	alertDB, err := initDB(db)
	if err != nil {
		fatal(log, err)
	}
	defer alertDB.Close()

	ctx := context.Background()
	ctx = ctxlog.WithLog(ctx, log)
	err = ingestFiles(ctx, glob, alertDB)
	if err != nil {
		fatal(log, err)
	}
}

func initDB(db string) (*alertdb.Database, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	s3 := s3.New(session, aws.NewConfig().WithRegion(awsRegion))

	alertDB, err := alertdb.NewS3Database(db, bucket, s3)
	if err != nil {
		return nil, err
	}
	return alertDB, nil

}

func ingestFiles(ctx context.Context, glob string, db *alertdb.Database) error {
	files, err := filepath.Glob(glob)
	if err != nil {
		return err
	}
	ctxlog.Info(ctx, "found files", zap.Int("n-files", len(files)))
	for _, f := range files {
		ctxlog.Info(ctx, "reading file", zap.String("filename", f))
		alerts, err := alertsFromFile(f)
		if err != nil {
			return err
		}
		ctxlog.Info(ctx, "found alerts", zap.Int("n-alerts", len(alerts)))
		for _, a := range alerts {
			err = db.Add(ctx, a)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func alertsFromFile(filepath string) ([]*schema.Alert, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("unable to open file: %w", err)
	}
	ar, err := schema.NewAlertReader(f)
	if err != nil {
		return nil, fmt.Errorf("unable to read alert file: %w", err)
	}
	alerts := make([]*schema.Alert, 0)
	for {
		a, err := ar.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		alerts = append(alerts, a)
	}
	return alerts, nil
}

func fatal(log *zap.Logger, err error) {
	log.Fatal("fatal error", zap.Error(err))
}
