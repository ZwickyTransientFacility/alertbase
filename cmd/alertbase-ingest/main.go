package main

import (
	"fmt"
	"io"
	"log"
	"os"

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
	fmt.Println(`usage: alertbase-ingest DB-DIR ALERT-FILE

DB-DIR should be a leveldb database directory
ALERT-FILE should be an avro-encoded file containing one or more alerts`)
}

func main() {
	if len(os.Args) != 3 {
		printusage()
		os.Exit(1)
	}
	db := os.Args[1]
	file := os.Args[2]
	err := ingestFile(file, db)
	if err != nil {
		fatal(err)
	}
}

func ingestFile(filepath, db string) error {
	alerts, err := alertsFromFile(filepath)
	if err != nil {
		return err
	}

	session, err := session.NewSession()
	if err != nil {
		return err
	}
	s3 := s3.New(session, aws.NewConfig().WithRegion(awsRegion))

	alertDB, err := alertdb.NewDatabase(db, bucket, s3)
	if err != nil {
		return err
	}
	defer alertDB.Close()

	for _, a := range alerts {
		err = alertDB.Add(a)
		if err != nil {
			return err
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
