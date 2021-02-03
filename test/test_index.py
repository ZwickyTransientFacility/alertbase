import pytest
import shutil
import tempfile
import pathlib

import alertbase
import astropy.time


@pytest.fixture(scope="function")
def leveldb_5k():
    """Copy the testdata/leveldbs/alerts.db.5k database to a temporary directory,
    scoped to a single test invocation.

    """
    db_path = "testdata/leveldbs/alerts.db.5k"
    with tempfile.TemporaryDirectory(prefix="test-alerts-5k-") as tmp_dir:
        tmp_db = pathlib.Path(tmp_dir) / "alerts.db.5k"
        shutil.copytree(db_path, tmp_db)
        yield tmp_db


class TestIndexDB:
    def test_open_database(self, leveldb_5k):
        alertbase.IndexDB(leveldb_5k)

    def test_count_candidates(self, leveldb_5k):
        db = alertbase.IndexDB(leveldb_5k)
        n = db.count_candidates()
        assert n == 5000

    def test_count_objects(self, leveldb_5k):
        db = alertbase.IndexDB(leveldb_5k)
        n = db.count_objects()
        assert n == 4848

    def test_count_timestamps(self, leveldb_5k):
        db = alertbase.IndexDB(leveldb_5k)
        n = db.count_timestamps()
        assert n == 11

    def test_count_healpixels(self, leveldb_5k):
        db = alertbase.IndexDB(leveldb_5k)
        n = db.count_healpixels()
        assert n == 4216

    def test_open_missing_db(self):
        with pytest.raises(Exception):
            alertbase.IndexDB("bogus")

    def test_write_roundtrip(self, tmpdir):
        db = alertbase.IndexDB(tmpdir, create_if_missing=True)
        alert_url = "url"
        candidate_id = 1
        object_id = "obj"
        timestamp = astropy.time.Time("2020-01-01T00:00:00")
        healpixel = 1
        db._write(alert_url, candidate_id, object_id, timestamp, healpixel)

        have_url = db.get_url(candidate_id)
        assert have_url == alert_url

        candidates = list(db.object_search(object_id))
        assert len(candidates) == 1
        assert candidates[0] == candidate_id

        candidates = list(db.timerange_search(start=timestamp, end=timestamp + 1))
        assert len(candidates) == 1
        assert candidates[0] == candidate_id
