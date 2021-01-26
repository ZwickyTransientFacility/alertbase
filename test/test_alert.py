import shutil
import tempfile
from alertbase.alert import AlertRecord
import pytest


@pytest.fixture(scope="function")
def alert_file():
    src = "testdata/alertfiles/1311156250015010003.avro"
    with tempfile.TemporaryDirectory(prefix="test-alertfile") as tempdir:
        dst = tempdir + "/testfile.avro"
        shutil.copyfile(src, dst)
        yield dst


def test_load_alertrecord_from_file_safely(alert_file):
    ar = AlertRecord.from_file_safe(open(alert_file, "rb"))
    assert ar.candidate_id == 1311156250015010003
    assert ar.object_id == "ZTF18aaylcqb"
    assert ar.position.ra.value == 234.1362886
    assert ar.position.dec.value == 16.6055949
    assert ar.timestamp.jd == 2459065.65625


def test_load_alertrecord_from_file_unsafely(alert_file):
    ar = AlertRecord.from_file_unsafe(open(alert_file, "rb"))
    assert ar.candidate_id == 1311156250015010003
    assert ar.object_id == "ZTF18aaylcqb"
    assert ar.position.ra.value == 234.1362886
    assert ar.position.dec.value == 16.6055949
    assert ar.timestamp.jd == 2459065.65625
