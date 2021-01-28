from typing import Iterator, Callable, IO
import tarfile
import pathlib
from alertbase.alert import AlertRecord


def iterate_tarfile(
    tarfile_path: pathlib.Path,
    deserializer: Callable[[IO[bytes]], AlertRecord] = AlertRecord.from_file_unsafe,
) -> Iterator[AlertRecord]:
    """
    Iterate over the alerts found in a standard ZTF alert archive tarball. The
    tarball should be gzipped, and contain individual alert files.
    """
    with tarfile.open(tarfile_path, mode="r:gz") as tf:
        for member in tf.getmembers():
            buf = tf.extractfile(member)
            assert buf is not None
            ar = deserializer(buf)
            yield ar
            buf.close()
