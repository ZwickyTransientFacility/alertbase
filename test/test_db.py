import pytest
import shutil
import tempfile
import pathlib

import alertbase


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


class TestDatabase:
    def test_open_database(self, leveldb_5k):
        alertbase.open_db(leveldb_5k)

    def test_count_candidates(self, leveldb_5k):
        db = alertbase.open_db(leveldb_5k)
        n = db.count_candidates()
        assert n == 5000

    def test_count_objects(self, leveldb_5k):
        db = alertbase.open_db(leveldb_5k)
        n = db.count_objects()
        assert n == 4848

    def test_count_timestamps(self, leveldb_5k):
        db = alertbase.open_db(leveldb_5k)
        n = db.count_timestamps()
        assert n == 11

    def test_count_healpixels(self, leveldb_5k):
        db = alertbase.open_db(leveldb_5k)
        n = db.count_healpixels()
        assert n == 4216

    def test_open_missing_db(self):
        with pytest.raises(Exception):
            alertbase.open_db("bogus")
