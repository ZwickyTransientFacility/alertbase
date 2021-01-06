package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

func ingestFiles(glob string, db *alertdb.Database) error {
	files, err := filepath.Glob(glob)
	if err != nil {
		return err
	}
	for _, f := range files {
		alerts, err := alertsFromFile(f)
		if err != nil {
			return err
		}

		for _, a := range alerts {
			err = db.Add(a)
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
