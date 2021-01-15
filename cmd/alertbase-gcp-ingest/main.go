package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/schema"
)

const (
	bucket    = "ztf-alert-archive-prototyping"
	projectID = "ztf-alert-archive-prototyping"
)

func printusage() {
	fmt.Println(`usage: alertbase-gcp-ingest DB-DIR ALERT-FILE-GLOB

DB-DIR should be a leveldb database directory
ALERT-FILE-GLOB should match avro-encoded files containing one or more alerts`)
}

func main() {
	if len(os.Args) != 3 {
		printusage()
		os.Exit(1)
	}
	db := os.Args[1]
	glob := os.Args[2]
	alertDB, err := initDB(db)
	if err != nil {
		fatal(err)
	}
	defer alertDB.Close()
	err = ingestFiles(glob, alertDB)
	if err != nil {
		fatal(err)
	}
}

func initDB(db string) (*alertdb.Database, error) {
	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	if err := ensureBucketExists(ctx, gcs, bucket); err != nil {
		return nil, err
	}

	alertDB, err := alertdb.NewGoogleCloudDatabase(db, bucket, gcs)
	if err != nil {
		return nil, err
	}
	return alertDB, nil

}

func ensureBucketExists(ctx context.Context, client *storage.Client, bucket string) error {
	// Create bucket if it doesn't exist
	bucketHandle := client.Bucket(bucket)
	_, err := bucketHandle.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrBucketNotExist) {
			err := bucketHandle.Create(ctx, projectID, nil)
			if err != nil {
				return fmt.Errorf("unable to create bucket: %w", err)
			}
		} else {
			return fmt.Errorf("unable to verify existence of bucket: %w", err)
		}
	}
	return nil
}

func ingestFiles(glob string, db *alertdb.Database) error {
	files, err := filepath.Glob(glob)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, f := range files {
		alerts, err := alertsFromFile(f)
		if err != nil {
			return err
		}

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

func fatal(err error) {
	log.Fatalf("FATAL: %v", err)
}
