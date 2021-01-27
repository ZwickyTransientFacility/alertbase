from typing import Iterator
import tarfile
import pathlib
from alertbase.alert import AlertRecord


def iterate_tarfile(tarfile_path: pathlib.Path) -> Iterator[AlertRecord]:
    """
    Iterate over the alerts found in a standard ZTF alert archive tarball. The
    tarball should be gzipped, and contain individual alert files.
    """
    with tarfile.open(tarfile_path, mode="r:gz") as tf:
        for member in tf.getmembers():
            buf = tf.extractfile(member)
            assert buf is not None
            ar = AlertRecord.from_file_unsafe(buf)
            yield ar
            buf.close()
