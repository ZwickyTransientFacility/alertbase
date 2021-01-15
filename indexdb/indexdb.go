package indexdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ZwickyTransientFacility/alertbase/internal/ctxlog"
	"github.com/ZwickyTransientFacility/alertbase/schema"
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
}

func NewIndexDB(dbPath string) (*IndexDB, error) {
	err := os.MkdirAll(dbPath, 0755)
	if err != nil {
		return nil, err
	}

	candidateDB, err := leveldb.OpenFile(filepath.Join(dbPath, "candidates"), nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open candidates database: %w", err)
	}
	objectDB, err := leveldb.OpenFile(filepath.Join(dbPath, "objects"), nil)
	if err != nil {
		candidateDB.Close()
		return nil, fmt.Errorf("unable to open object database: %w", err)
	}
	timestampDB, err := leveldb.OpenFile(filepath.Join(dbPath, "timestamps"), nil)
	if err != nil {
		candidateDB.Close()
		objectDB.Close()
		return nil, fmt.Errorf("unable to open timestamp database: %w", err)
	}
	return &IndexDB{
		byCandidateID: candidateDB,
		byObjectID:    objectDB,
		byTimestamp:   timestampDB,
	}, nil
}

func (db *IndexDB) Add(ctx context.Context, a *schema.Alert, url string) error {
	id := byteID(a)
	err := db.byCandidateID.Put(id, []byte(url), nil)
	if err != nil {
		return fmt.Errorf("unable to write into candidate ID DB: %w", err)
	}

	err = ldbAppend(db.byObjectID, byteObjectID(a), id)
	if err != nil {
		return fmt.Errorf("unable to write into object ID DB: %w", err)
	}

	err = ldbAppend(db.byTimestamp, byteTimestamp(a), id)
	if err != nil {
		return fmt.Errorf("unable to write into timestamp DB: %w", err)
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
	ctxlog.Debug(ctx, "searching in byte range",
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
	return nil
}

func ldbAppend(db *leveldb.DB, key, val []byte) error {
	have, err := db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return db.Put(key, val, nil)
		}
		return err
	}
	return db.Put(key, append(have, val...), nil)
}
