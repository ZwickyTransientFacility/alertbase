package alertdb

import (
	"fmt"
	"log"

	"github.com/ZwickyTransientFacility/alertbase/blobstore"
	"github.com/ZwickyTransientFacility/alertbase/indexdb"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type Database struct {
	index *indexdb.IndexDB
	blobs *blobstore.S3Blobstore
}

func NewDatabase(indexDBPath string, s3Bucket string, s3Client s3iface.S3API) (*Database, error) {
	blobs := blobstore.NewS3Blobstore(s3Client, s3Bucket)
	indexDB, err := indexdb.NewIndexDB(indexDBPath)
	if err != nil {
		return nil, err
	}
	return &Database{index: indexDB, blobs: blobs}, nil
}

func (db *Database) Add(a *schema.Alert) error {
	log.Printf("adding alert id=%v", a.Candid)
	url, err := db.blobs.Write(a)
	if err != nil {
		return fmt.Errorf("unable to add alert to blobstore: %w", err)
	}
	log.Printf("adding alert id=%v  url=%v", a.Candid, url)
	err = db.index.Add(a, url)
	if err != nil {
		return fmt.Errorf("unable to add alert to indexDB: %w", err)
	}
	log.Printf("added")
	return nil
}

func (db *Database) GetByCandidateID(id uint64) (*schema.Alert, error) {
	url, err := db.index.GetByCandidateID(id)
	if err != nil {
		return nil, err
	}
	return db.blobs.Read(url)
}

func (db *Database) GetByObjectID(id string) ([]*schema.Alert, error) {
	urls, err := db.index.GetByObjectID(id)
	if err != nil {
		return nil, err
	}
	alerts := make([]*schema.Alert, len(urls))
	for i, u := range urls {
		alerts[i], err = db.blobs.Read(u)
		if err != nil {
			return nil, err
		}
	}
	return alerts, nil
}

func (db *Database) GetByTimerange(start, end float64) ([]*schema.Alert, error) {
	urls, err := db.index.GetByTimerange(start, end)
	if err != nil {
		return nil, err
	}
	alerts := make([]*schema.Alert, len(urls))
	for i, u := range urls {
		alerts[i], err = db.blobs.Read(u)
		if err != nil {
			return nil, err
		}
	}
	return alerts, nil
}

func (db *Database) Close() error {
	return db.index.Close()
}
