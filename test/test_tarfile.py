import pytest
import tempfile
import shutil
from alertbase.alert_tar import iterate_tarfile


@pytest.fixture(scope="function")
def alert_tarball():
    src = "testdata/alertfiles/ztf_public_20210120.tar.gz"
    with tempfile.TemporaryDirectory(prefix="test-alertfile") as tempdir:
        dst = tempdir + "/testfile.avro"
        shutil.copyfile(src, dst)
        yield dst


def test_iterate_tarfile(alert_tarball):
    iterator = iterate_tarfile(alert_tarball)
    i = 0
    for a in iterator:
        i += 1

    assert i == 2567
