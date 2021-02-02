import shutil
import tempfile
from alertbase.alert import AlertRecord
import pytest
from astropy.coordinates import SkyCoord


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


def test_calculate_healpixel():
    north_pole = AlertRecord(
        candidate_id=None,
        object_id=None,
        timestamp=None,
        raw_data=None,
        raw_dict=None,
        position=SkyCoord(
            ra=0,
            dec=90,
            unit="deg",
        ),
    )
    assert north_pole.healpixel(1) == 3
    assert north_pole.healpixel(2) == 15
    assert north_pole.healpixel(3) == 63

    zeros = north_pole
    zeros.position = SkyCoord(
        ra=0,
        dec=0,
        unit="deg",
    )
    assert zeros.healpixel(1) == 17
    assert zeros.healpixel(2) == 70
    assert zeros.healpixel(3) == 282
