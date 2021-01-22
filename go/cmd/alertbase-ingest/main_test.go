package main

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/ZwickyTransientFacility/alertbase/schema"
)

func TestIngester(t *testing.T) {
	blobs := inMemoryBlobstore{blobs: make(map[string][]byte)}
	db := inMemoryKeyval{data: make(map[string][]byte)}
	ingester := ingester{
		blobs: blobs,
		db:    db,
	}
	alerts := []*schema.Alert{
		mockAlert("k1", "v1"),
		mockAlert("k2", "v2"),
		mockAlert("k3", "v3"),
	}
	err := ingester.ingest(alerts)
	if err != nil {
		t.Fatalf("ingest error: %v", err)
	}
	if len(blobs.blobs) != 3 {
		t.Errorf("unexpected amount of data in blob store - expected 3 values, have %d", len(blobs.blobs))
	}
	if len(db.data) != 3 {
		t.Errorf("unexpected amount of data in keyval store - expected 3 values, have %d", len(db.data))
	}
}

func mockAlert(id, data string) *schema.Alert {
	return &schema.Alert{
		ObjectId:  id,
		Candidate: &schema.Candidate{},
		CutoutScience: &schema.UnionNullCutout{
			Cutout: &schema.Cutout{
				StampData: []byte(data),
			},
			UnionType: schema.UnionNullCutoutTypeEnumCutout,
		},
	}
}

type inMemoryBlobstore struct {
	blobs map[string][]byte
}

func (imb inMemoryBlobstore) store(key string, value io.ReadSeeker) (string, error) {
	body, err := ioutil.ReadAll(value)
	if err != nil {
		return "", err
	}
	imb.blobs[key] = body
	return "imb://" + key, nil
}

type inMemoryKeyval struct {
	data map[string][]byte
}

func (imk inMemoryKeyval) store(key, value []byte) error {
	imk.data[string(key)] = value
	return nil
}

func TestAlertsFromFileBatch(t *testing.T) {
	have, err := alertsFromFile("../../testdata/batch.avro")
	if err != nil {
		t.Fatal(err)
	}
	if len(have) != 182 {
		t.Errorf("expected 182 alerts in the batch file, but got %d", len(have))
	}
}

func TestAlertsFromFileSingle(t *testing.T) {
	have, err := alertsFromFile("../../testdata/single.avro")
	if err != nil {
		t.Fatal(err)
	}
	if len(have) != 1 {
		t.Errorf("expected 1 alert in the single file, but got %d", len(have))
	}
}
