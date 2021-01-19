package indexdb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/ZwickyTransientFacility/alertbase/schema"
	"github.com/spenczar/healpix"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
)

type IndexDB struct {
	// maps Candidate ID -> URL
	byCandidateID *leveldb.DB

	// maps Object ID -> sequence of 8-byte candidate IDs
	byObjectID *leveldb.DB

	// maps Timestamp -> sequence of 8-byte candidate IDs
	byTimestamp *leveldb.DB

	// maps HEALPix pixel -> sequence of 8-byte candidate IDs
	byHealpix     *leveldb.DB
	healpixMapper *healpix.HEALPixMapper
}

func NewIndexDB(dbPath string, healpixOrder int) (*IndexDB, error) {
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, err
	}

	db := &IndexDB{}
	db.healpixMapper, err = healpix.NewHEALPixMapper(healpixOrder, healpix.Nest)
	if err != nil {
		return nil, err
	}

	// Keep track of all DBs we open as we go so they can be closed safely in case
	// of error.
	var openDBs []io.Closer
	openDB := func(name string, prevErr error) (*leveldb.DB, error) {
		// Use a sticky previous error to reduce noise.
		if prevErr != nil {
			return nil, prevErr
		}

		db, err := leveldb.OpenFile(filepath.Join(dbPath, name), nil)
		if err != nil {
			return nil, fmt.Errorf("unable to open %s database: %w", name, err)
		}

		openDBs = append(openDBs, db)
		return db, nil
	}

	db.byCandidateID, err = openDB("candidates", err)
	db.byObjectID, err = openDB("objects", err)
	db.byTimestamp, err = openDB("timestamps", err)
	db.byHealpix, err = openDB("healpixels", err)

	if err != nil {
		for _, c := range openDBs {
			c.Close()
		}
		return nil, err
	}

	return db, nil
}

func (db *IndexDB) Add(ctx context.Context, a *schema.Alert, url string) error {
	id := byteID(a)
	ctxlog.Debug(ctx, "storing candidate URL",
		zap.String("db", "candidates"),
		zap.Binary("key", id),
		zap.Binary("value", []byte(url)),
	)
	err := db.byCandidateID.Put(id, []byte(url), nil)
	if err != nil {
		return fmt.Errorf("unable to write into candidate ID DB: %w", err)
	}

	err = ldbAppend(
		ctxlog.WithFields(ctx, zap.String("db", "objects")),
		db.byObjectID, byteObjectID(a), id,
	)
	if err != nil {
		return fmt.Errorf("unable to write into object ID DB: %w", err)
	}

	err = ldbAppend(
		ctxlog.WithFields(ctx, zap.String("db", "timestamps")),
		db.byTimestamp, byteTimestamp(a), id,
	)
	if err != nil {
		return fmt.Errorf("unable to write into timestamp DB: %w", err)
	}

	err = ldbAppend(
		ctxlog.WithFields(ctx, zap.String("db", "healpix")),
		db.byHealpix, byteHEALPixel(ctx, a, db.healpixMapper), id,
	)
	if err != nil {
		return fmt.Errorf("unable to write into healpix DB: %w", err)
	}

	return nil
}

// GetByCandidateID gets the URL holding data for a particular alert by ID.
func (db *IndexDB) GetByCandidateID(ctx context.Context, id uint64) (url string, err error) {
	ctxlog.Debug(ctx, "looking up candidate URL", zap.Uint64("id", id))
	have, err := db.byCandidateID.Get(uint64ToBytes(id), nil)
	if err != nil {
		return "", err
	}
	ctxlog.Debug(ctx, "retrieved candidate URL", zap.Uint64("id", id), zap.String("url", string(have)))
	return string(have), nil
}

// GetByObjectID gets all URLs for alerts associated with a particular Object by
// ID.
func (db *IndexDB) GetByObjectID(ctx context.Context, id string) (urls []string, err error) {
	candidates, err := db.byObjectID.Get([]byte(id), nil)
	if err != nil {
		return nil, err
	}
	ctxlog.Debug(ctx, "retrieved packed candidate data", zap.Binary("candidates", candidates))
	candidateIDs := packedUint64s(candidates)
	urls = make([]string, candidateIDs.Len())
	ctxlog.Debug(ctx, "unpacked into IDs", zap.Int("n-candidates", candidateIDs.Len()))
	for i, id := range candidateIDs.Values() {
		urls[i], err = db.GetByCandidateID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve candidate ID %d to a URL: %w", id, err)
		}
	}
	return urls, nil
}

// GetByTimerange returns the URLs for alerts between two julian dates.
func (db *IndexDB) GetByTimerange(ctx context.Context, start, end float64) (urls []string, err error) {
	startUnix := jd2unix(start)
	endUnix := jd2unix(end)
	byterange := &util.Range{
		Start: uint64ToBytes(startUnix),
		Limit: uint64ToBytes(endUnix),
	}
	ctxlog.Debug(ctx, "searching by timestamp in byte range",
		zap.Float64("start-float", start),
		zap.Float64("end-float", end),
		zap.Uint64("start-unix", startUnix),
		zap.Uint64("end-unix", endUnix),
		zap.Binary("start-binary", byterange.Start),
		zap.Binary("end-binary", byterange.Limit),
	)
	iterator := db.byTimestamp.NewIterator(byterange, nil)
	ctxlog.Debug(ctx, "iterator created")
	defer iterator.Release()

	err = iterator.Error()
	if err != nil {
		return nil, err
	}

	urls = make([]string, 0)
	for iterator.Next() {
		ids := packedUint64s(iterator.Value())
		ctxlog.Debug(ctx, "iterator step",
			zap.Binary("key", iterator.Key()),
			zap.Binary("value", iterator.Value()),
			zap.Int("size", ids.Len()),
		)
		for _, id := range ids.Values() {
			url, err := db.GetByCandidateID(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("unable to resolve candidate ID %d to a URL: %w", id, err)
			}
			urls = append(urls, url)
		}
	}
	ctxlog.Debug(ctx, "iteration complete")

	err = iterator.Error()
	if err != nil {
		return nil, err
	}
	return urls, nil
}

func (db *IndexDB) GetByConeSearch(ctx context.Context, ra, dec, radius float64) (urls []string, err error) {
	pointing := healpix.RADec(ra, dec)
	pixranges := db.healpixMapper.QueryDiscInclusive(pointing, radius, 4)
	for _, pixrange := range pixranges {
		pixrangeUrls, err := db.queryHealpixRange(ctx, pixrange)
		if err != nil {
			return nil, err
		}
		urls = append(urls, pixrangeUrls...)
	}
	return urls, nil
}

func (db *IndexDB) queryHealpixRange(ctx context.Context, pixrange healpix.PixelRange) (urls []string, err error) {
	dbrange := &util.Range{
		Start: uint64ToBytes(uint64(pixrange.Start)),
		Limit: uint64ToBytes(uint64(pixrange.Stop)),
	}
	ctxlog.Debug(ctx, "searching by healpix id in byte range",
		zap.Int("start-int", pixrange.Start),
		zap.Int("end-int", pixrange.Stop),
		zap.Binary("start-binary", dbrange.Start),
		zap.Binary("end-binary", dbrange.Limit),
	)
	ctxlog.Debug(ctx, "creating iterator")
	iterator := db.byHealpix.NewIterator(dbrange, nil)
	defer iterator.Release()
	err = iterator.Error()
	if err != nil {
		return nil, err
	}

	urls = make([]string, 0)
	for iterator.Next() {
		ids := packedUint64s(iterator.Value())
		ctxlog.Debug(ctx, "iterator step",
			zap.Binary("key", iterator.Key()),
			zap.Binary("value", iterator.Value()),
			zap.Int("size", ids.Len()),
		)
		for _, id := range ids.Values() {
			url, err := db.GetByCandidateID(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("unable to resolve candidate ID %d to a URL: %w", id, err)
			}
			urls = append(urls, url)
		}
	}
	ctxlog.Debug(ctx, "iteration complete")

	err = iterator.Error()
	if err != nil {
		return nil, err
	}
	return urls, nil
}

func (db *IndexDB) Close() error {
	err := db.byCandidateID.Close()
	if err != nil {
		return fmt.Errorf("unable to close candidate DB: %w", err)
	}
	err = db.byObjectID.Close()
	if err != nil {
		return fmt.Errorf("unable to close object DB: %w", err)
	}
	err = db.byTimestamp.Close()
	if err != nil {
		return fmt.Errorf("unable to close timestamp DB: %w", err)
	}
	err = db.byHealpix.Close()
	if err != nil {
		return fmt.Errorf("unable to close healpix DB: %w", err)
	}
	return nil
}

func ldbAppend(ctx context.Context, db *leveldb.DB, key, val []byte) error {
	have, err := db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			ctxlog.Debug(ctx, "creating new levelDB entry",
				zap.Binary("key", key),
				zap.Binary("val", val),
			)
			return db.Put(key, val, nil)
		}
		return err
	}
	ctxlog.Debug(ctx, "appending to levelDB entry",
		zap.Binary("key", key),
		zap.Binary("val", val),
		zap.Int("existing-size", len(have)),
	)
	return db.Put(key, append(have, val...), nil)
}
