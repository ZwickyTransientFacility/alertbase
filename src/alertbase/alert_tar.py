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
        member = tf.next()
        while member is not None:
            buffer = tf.extractfile(member)
            assert buffer is not None
            ar = AlertRecord.from_file_unsafe(buffer)
            yield ar
            member = tf.next()
