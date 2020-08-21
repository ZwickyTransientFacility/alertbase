package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	bucket    = "ztf-alert-achive-prototyping"
	awsRegion = "us-west-2"
)

func printusage() {
	fmt.Println(`usage: alertbase-ingest DB-FILE ALERT-FILE

DB-FILE should be a leveldb database file
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

	ingester, err := newIngester(bucket, db)
	if err != nil {
		return err
	}
	return ingester.ingest(alerts)
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

type ingester struct {
	blobs blobstore
	db    keyvalDB
}

func newIngester(bucket, dbPath string) (ingester, error) {
	blobs, err := newS3Blobstore(bucket)
	if err != nil {
		return ingester{}, err
	}
	db, err := newLevelDB(dbPath)
	if err != nil {
		return ingester{}, err
	}
	return ingester{
		blobs: blobs,
		db:    db,
	}, nil
}

func (i ingester) ingest(alerts []*schema.Alert) error {
	for _, a := range alerts {
		contents := bytes.NewBuffer(nil)
		err := a.Serialize(contents)
		if err != nil {
			return fmt.Errorf("unable to serialize alert id=%v: %v", a.ObjectId, err)
		}
		url, err := i.blobs.store(a.ObjectId, bytes.NewReader(contents.Bytes()))
		if err != nil {
			return err
		}
		err = i.db.store([]byte(a.ObjectId), []byte(url))
		if err != nil {
			return err
		}
	}
	return nil
}

// blobstore stores large piles of bytes
type blobstore interface {
	// Store pile of bytes with a key. Return a URL indicating its location.
	store(key string, value io.ReadSeeker) (string, error)
}

type s3Blobstore struct {
	bucket string
	s3     s3iface.S3API
}

func newS3Blobstore(bucket string) (s3Blobstore, error) {
	session, err := session.NewSession()
	if err != nil {
		return s3Blobstore{}, err
	}
	s3 := s3.New(session, aws.NewConfig().WithRegion(awsRegion))
	return s3Blobstore{
		bucket: bucket,
		s3:     s3,
	}, nil
}

func (bs s3Blobstore) store(key string, body io.ReadSeeker) (string, error) {
	fullKey := "alerts/v1/" + key
	url := fmt.Sprintf("s3://%s/%s", bs.bucket, fullKey)
	_, err := bs.s3.PutObject(&s3.PutObjectInput{
		Body:   body,
		Bucket: aws.String(bs.bucket),
		Key:    aws.String(fullKey),
	})
	return url, err
}

// keyvalDB indexes key-value pairs
type keyvalDB interface {
	store(key, value []byte) error
}

type levelDB struct {
	ldb *leveldb.DB
}

func newLevelDB(filepath string) (levelDB, error) {
	db, err := leveldb.OpenFile(filepath, nil)
	if err != nil {
		return levelDB{}, err
	}
	return levelDB{ldb: db}, nil
}

func (db levelDB) store(key, value []byte) error {
	return db.ldb.Put(key, value, nil)
}
