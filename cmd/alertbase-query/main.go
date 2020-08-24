package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ZwickyTransientFacility/alertbase/alertdb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	objectID    = flag.String("object", "", "query by object ID")
	candidateID = flag.Uint64("candidate", 0, "query by candidate ID")

	timeStart  = flag.String("time-start", "", "query by time range: start time")
	timeEnd    = flag.String("time-end", "", "query by time range: end time")
	timeFormat = flag.String("time-format", "jd", "format of time inputs (can be 'jd', 'unixnano', or 'rfc3339'")

	db     = flag.String("db", "alerts.db", "path to alerts database")
	bucket = flag.String("bucket", "ztf-alert-archive-prototyping", "s3 bucket containing alert data")
)

func main() {
	flag.Parse()
	query, err := parseQueryType()
	if err != nil {
		fatal(err)
	}

	session, err := session.NewSession()
	if err != nil {
		fatal(err)
	}
	s3 := s3.New(session, aws.NewConfig().WithRegion("us-west-2"))
	db, err := alertdb.NewDatabase(*db, *bucket, s3)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	switch query {
	case candidate:
		err = queryCandidate(db, *candidateID)
	case object:
		err = queryObject(db, *objectID)
	case timerange:
		err = queryTimerange(db, *timeStart, *timeEnd, *timeFormat)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	log.Fatalf("FATAL: %v", err)
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
