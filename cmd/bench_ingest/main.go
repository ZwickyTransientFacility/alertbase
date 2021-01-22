package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/benchutil"
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

var (
	srcDir   = flag.String("src-dir", "testdata/*.avro", "directory with tarballs to load from")
	benchDir = flag.String("bench-dir", "bench", "directory to store benchmarks in")
	db       = flag.String("db", "alerts.db", "base directory to work from")
	clean    = flag.Bool("clean", false, "delete database before starting")
)

func main() {
	flag.Parse()
	log, err := zap.NewDevelopment()
	defer log.Sync()

	if *clean {
		err = os.RemoveAll(*db)
		if err != nil {
			fatal(log, err)
		}
	}

	alertDB, err := initDB(*db)
	if err != nil {
		fatal(log, err)
	}
	defer alertDB.Close()
	benchEnv := benchutil.BenchmarkEnvironment{
		DataVolumeDays:   int64(len(alertDB.Meta.Days)),
		DataVolumeAlerts: int64(alertDB.Meta.NAlerts),
		DataVolumeBytes:  int64(alertDB.Meta.NBytes),
		Scheme:           benchutil.NESTED,
		IndexDB:          benchutil.LevelDB,
		Blobstore:        benchutil.S3,
		Env:              benchutil.Laptop,
		Indexes:          benchutil.AllIndexTypes,
		QueryType:        benchutil.Ingest,
	}

	benchlog, err := benchutil.NewBenchmarkLogger(benchEnv, *benchDir)
	if err != nil {
		fatal(log, err)
	}
	defer benchlog.Close()

	ctx := context.Background()
	ctx = ctxlog.WithLog(ctx, log)
	ctx = benchutil.WithBenchmarker(ctx, benchlog)
	err = ingestFiles(ctx, *srcDir, alertDB, benchlog)
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

func ingestFiles(ctx context.Context, glob string, db *alertdb.Database, bl *benchutil.BenchmarkLogger) error {
	files, err := filepath.Glob(glob)
	if err != nil {
		return err
	}
	bl.ObserveInt(len(files), "n-files")
	ctxlog.Info(ctx, "found files", zap.Int("n-files", len(files)))
	for _, f := range files {
		ctxlog.Info(ctx, "reading file", zap.String("filename", f))
		alerts, err := alertsFromFile(f)
		if err != nil {
			return err
		}
		bl.ObserveInt(len(alerts), "n-alerts")
		ctxlog.Info(ctx, "found alerts", zap.Int("n-alerts", len(alerts)))
		for _, a := range alerts {
			start := time.Now()
			err = db.Add(ctx, a)
			if err != nil {
				return err
			}
			bl.ObserveDuration(time.Since(start), "add-alert")
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
	log.WithOptions(zap.AddCallerSkip(1)).Fatal("fatal error", zap.Error(err))
}
