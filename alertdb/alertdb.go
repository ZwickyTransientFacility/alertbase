package alertdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ZwickyTransientFacility/alertbase/blobstore"
	"github.com/ZwickyTransientFacility/alertbase/indexdb"
	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.uber.org/zap"
)

const healpixOrder = 10

type Database struct {
	dbPath string
	index  *indexdb.IndexDB
	blobs  Blobstore

	Meta DBMeta
}

type Blobstore interface {
	Read(ctx context.Context, url string) (*schema.Alert, error)
	ReadMany(ctx context.Context, urls []string) *blobstore.AlertIterator
	Write(context.Context, *schema.Alert) (size int, url string, err error)
}

func NewS3Database(dbPath string, s3Bucket string, s3Client s3iface.S3API) (*Database, error) {
	blobs := blobstore.NewS3Blobstore(s3Client, s3Bucket)
	indexDB, err := indexdb.NewIndexDB(dbPath, healpixOrder)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, err
	}

	meta := NewDBMeta()
	f, err := os.Open(filepath.Join(dbPath, "meta.json"))
	if err == nil {
		defer f.Close()
		err = meta.ReadFrom(f)
		if err != nil {
			return nil, err
		}
	}

	return &Database{
		dbPath: dbPath,
		index:  indexDB,
		blobs:  blobs,
		Meta:   *meta,
	}, nil
}

func (db *Database) Add(ctx context.Context, a *schema.Alert) error {
	ctx = ctxlog.WithFields(ctx, zap.Int64("CandID", a.Candid))

	ctxlog.Debug(ctx, "adding alert to blobstore")
	size, url, err := db.blobs.Write(ctx, a)
	if err != nil {
		return fmt.Errorf("unable to add alert to blobstore: %w", err)
	}
	db.Meta.NBytes += size

	ctxlog.Debug(ctx, "adding alert to index", zap.String("url", url))
	err = db.index.Add(ctx, a, url)
	if err != nil {
		return fmt.Errorf("unable to add alert to indexDB: %w", err)
	}

	db.Meta.NAlerts += 1

	db.Meta.markTimestamps(a)

	ctxlog.Debug(ctx, "alert added")
	return nil
}

func (db *Database) GetByCandidateID(ctx context.Context, id uint64) (*schema.Alert, error) {
	ctx = ctxlog.WithFields(ctx, zap.Uint64("CandID", id))
	ctxlog.Debug(ctx, "getting alert from index")
	url, err := db.index.GetByCandidateID(ctx, id)
	if err != nil {
		return nil, err
	}
	ctxlog.Debug(ctx, "alert received, fetching from blobstore", zap.String("url", url))
	return db.blobs.Read(ctx, url)
}

func (db *Database) GetByObjectID(ctx context.Context, id string) ([]*schema.Alert, error) {
	ctx = ctxlog.WithFields(ctx, zap.String("ObjectID", id))
	ctxlog.Debug(ctx, "getting alert URLs from index")

	urls, err := db.index.GetByObjectID(ctx, id)
	if err != nil {
		return nil, err
	}

	ctxlog.Debug(ctx, "found alert URLs, fetching from blobstore", zap.Int("n-urls", len(urls)))
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
	ctx = ctxlog.WithFields(ctx, zap.Float64("start", start), zap.Float64("end", end))
	ctxlog.Debug(ctx, "getting alert URLs from index")
	urls, err := db.index.GetByTimerange(ctx, start, end)
	if err != nil {
		return nil, err
	}

	ctxlog.Debug(ctx, "found alert URLs, fetching from blobstore", zap.Int("n-urls", len(urls)))
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
	ctx = ctxlog.WithFields(ctx, zap.Float64("start", start), zap.Float64("end", end))
	ctxlog.Debug(ctx, "getting alert URLs from index")

	defer close(ch)
	urls, err := db.index.GetByTimerange(ctx, start, end)
	if err != nil {
		return err
	}

	ctxlog.Debug(ctx, "found alert URLs, fetching from blobstore", zap.Int("n-urls", len(urls)))
	iterator := db.blobs.ReadMany(ctx, urls)
	for iterator.Next() {
		ch <- iterator.Value()
	}
	return iterator.Error()
}

func (db *Database) Close() error {
	f, err := os.Create(filepath.Join(db.dbPath, "meta.json"))
	if err != nil {
		return err
	}
	defer f.Close()

	err = db.Meta.WriteTo(f)
	if err != nil {
		return err
	}
	return db.index.Close()
}
