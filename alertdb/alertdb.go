package alertdb

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/ZwickyTransientFacility/alertbase/blobstore"
	"github.com/ZwickyTransientFacility/alertbase/indexdb"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type Database struct {
	index *indexdb.IndexDB
	blobs Blobstore
}

type Blobstore interface {
	Read(ctx context.Context, url string) (*schema.Alert, error)
	ReadMany(ctx context.Context, urls []string) *blobstore.AlertIterator
	Write(context.Context, *schema.Alert) (url string, err error)
}

func NewS3Database(indexDBPath string, s3Bucket string, s3Client s3iface.S3API) (*Database, error) {
	blobs := blobstore.NewS3Blobstore(s3Client, s3Bucket)
	indexDB, err := indexdb.NewIndexDB(indexDBPath)
	if err != nil {
		return nil, err
	}
	return &Database{index: indexDB, blobs: blobs}, nil
}

func NewGoogleCloudDatabase(indexDBPath string, gcsBucket string, gcsClient *storage.Client) (*Database, error) {
	blobs := blobstore.NewCloudStorageBlobstore(gcsClient, gcsBucket)
	indexDB, err := indexdb.NewIndexDB(indexDBPath)
	if err != nil {
		return nil, err
	}
	return &Database{index: indexDB, blobs: blobs}, nil
}

func (db *Database) Add(ctx context.Context, a *schema.Alert) error {
	log.Printf("adding alert id=%v", a.Candid)
	url, err := db.blobs.Write(ctx, a)
	if err != nil {
		return fmt.Errorf("unable to add alert to blobstore: %w", err)
	}
	log.Printf("adding alert id=%v  url=%v", a.Candid, url)
	err = db.index.Add(ctx, a, url)
	if err != nil {
		return fmt.Errorf("unable to add alert to indexDB: %w", err)
	}
	log.Printf("added")
	return nil
}

func (db *Database) GetByCandidateID(ctx context.Context, id uint64) (*schema.Alert, error) {
	url, err := db.index.GetByCandidateID(ctx, id)
	if err != nil {
		return nil, err
	}
	return db.blobs.Read(ctx, url)
}

func (db *Database) GetByObjectID(ctx context.Context, id string) ([]*schema.Alert, error) {
	urls, err := db.index.GetByObjectID(ctx, id)
	if err != nil {
		return nil, err
	}
	alerts := make([]*schema.Alert, len(urls))
	for i, u := range urls {
		alerts[i], err = db.blobs.Read(ctx, u)
		if err != nil {
			return nil, err
		}
	}
	return alerts, nil
}

func (db *Database) GetByTimerange(ctx context.Context, start, end float64) ([]*schema.Alert, error) {
	urls, err := db.index.GetByTimerange(ctx, start, end)
	if err != nil {
		return nil, err
	}
	alerts := make([]*schema.Alert, len(urls))
	iterator := db.blobs.ReadMany(ctx, urls)
	i := 0
	for iterator.Next() {
		alerts[i] = iterator.Value()
		i += 1
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	return alerts, nil
}

func (db *Database) StreamByTimerange(ctx context.Context, start, end float64, ch chan *schema.Alert) error {
	defer close(ch)
	urls, err := db.index.GetByTimerange(ctx, start, end)
	if err != nil {
		return err
	}
	iterator := db.blobs.ReadMany(ctx, urls)
	for iterator.Next() {
		ch <- iterator.Value()
	}
	return iterator.Error()
}

func (db *Database) Close() error {
	return db.index.Close()
}
