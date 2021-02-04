from __future__ import annotations
from typing import Iterator, Optional, Generic, TypeVar, Tuple, Union

import pathlib
import plyvel
from astropy.coordinates import SkyCoord, Angle, CartesianRepresentation
from astropy.time import Time
import healpy
import numpy as np

from alertbase.alert import AlertRecord

from alertbase.encoding import (
    Codec,
    uint64_codec,
    time_codec,
    str_codec,
    varint_codec,
    varint_iterator_codec,
)
import logging

logger = logging.getLogger(__name__)


class IndexDB:
    db_root: pathlib.Path
    candidates: _TypedLevelDB[int, str]
    objects: _TypedLevelDB[str, Iterator[int]]
    healpixels: _TypedLevelDB[int, Iterator[int]]
    timestamps: _TypedLevelDB[Time, Iterator[int]]

    order: int

    def __init__(
        self, db_path: Union[str, pathlib.Path], create_if_missing: bool = False
    ):
        self.db_root = pathlib.Path(db_path)

        if create_if_missing:
            self.db_root.mkdir(parents=True, exist_ok=True)

        self.objects = _TypedLevelDB(
            db=plyvel.DB(
                str(self.db_root / "objects"),
                create_if_missing=create_if_missing,
            ),
            key_codec=str_codec,
            val_codec=varint_iterator_codec,
        )
        self.candidates = _TypedLevelDB(
            db=plyvel.DB(
                str(self.db_root / "candidates"),
                create_if_missing=create_if_missing,
            ),
            key_codec=varint_codec,
            val_codec=str_codec,
        )
        self.healpixels = _TypedLevelDB(
            db=plyvel.DB(
                str(self.db_root / "healpixels"),
                create_if_missing=create_if_missing,
            ),
            key_codec=uint64_codec,
            val_codec=varint_iterator_codec,
        )
        self.timestamps = _TypedLevelDB(
            db=plyvel.DB(
                str(self.db_root / "timestamps"),
                create_if_missing=create_if_missing,
            ),
            key_codec=time_codec,
            val_codec=varint_iterator_codec,
        )

        self.order = 12

    def _append(self, db: plyvel.DB, key: bytes, value: bytes) -> None:
        """ Add key-value pair to DB, appending if a value already exists."""
        prev = db.get(key, default=b"", fill_cache=False)
        db.put(key, prev + value)

    def _write(
        self,
        alert_url: str,
        candidate_id: int,
        object_id: str,
        time: Time,
        healpixel: int,
    ) -> None:
        """
        Add a record to all levelDB databases.
        """
        self.candidates.put(candidate_id, alert_url)
        self.objects.append(object_id, iter([candidate_id]))
        self.healpixels.append(healpixel, iter([candidate_id]))
        self.timestamps.append(time, iter([candidate_id]))

    def insert(self, url: str, alert: AlertRecord) -> None:
        self._write(
            alert_url=url,
            candidate_id=alert.candidate_id,
            object_id=alert.object_id,
            time=alert.timestamp,
            healpixel=alert.healpixel(self.order),
        )

    def get_url(self, candidate_id: int) -> Optional[str]:
        """
        Get the URL where a full alert packet is stored for the given
        alert candidate ID.
        """
        return self.candidates.get(candidate_id)

    def object_search(self, object_id: str) -> Iterator[int]:
        """
        Retrieve the candidate IDs for a given ZTF object
        """
        it = self.objects.get(object_id)
        if it is None:
            raise StopIteration
        return it

    def timerange_search(self, start: Time, end: Time) -> Iterator[int]:
        """
        Retrieve the candidate IDs for all alerts that were recorded between
        start and end time range (values should be julian dates).
        """
        iterator = self.timestamps.iterate(start, end)
        for timestamp in iterator:
            for candidate_id in timestamp:
                yield candidate_id

    def cone_search(self, center: SkyCoord, radius: Angle) -> Iterator[int]:
        """
        cone_search retrieves the Candidate IDs for alerts that can be found in
        a region of the sky.
        """
        if center.representation_type != CartesianRepresentation:
            center = center.replicate(representation_type=CartesianRepresentation)
        pixels = healpy.query_disc(
            nside=healpy.order2nside(self.order),
            vec=(center.x.value, center.y.value, center.z.value),
            radius=radius.degree,
            inclusive=True,
            nest=True,
        )
        logger.info("found %d pixels which might match", len(pixels))
        ranges = self._compact_pixel_ranges(pixels)
        logger.info("compacted range into %d elements", len(ranges))
        for start, stop in ranges:
            logger.info("checking range %d to %d", start, stop)
            for pixel in self.healpixels.iterate(start, stop):
                for candidate_id in pixel:
                    yield candidate_id

    @staticmethod
    def _compact_pixel_ranges(pixelseq: np.ndarray) -> np.ndarray:
        """healpy gives us an enormous sequence of pixels. Compact it into (start, stop)
        pairs describing contiguous runs.
        """
        # Compute differences of sequential elements
        deltas = pixelseq[1:] - pixelseq[:-1]
        # Find elements which are more than 1 away from the previous elemeent
        starts = pixelseq[1:][deltas != 1]
        # Include first element
        starts = np.concatenate((pixelseq[:1], starts))
        # Find elements which are more than 1 away from the next element
        ends = pixelseq[:-1][deltas != 1]
        # Include last element
        ends = np.concatenate((ends, pixelseq[-1:]))
        # Offset ends by 1 for exclusive ranges
        ends += 1
        result: np.ndarray = np.dstack((starts, ends))[0]
        return result

    def count_objects(self) -> int:
        """count_objects iterates over all the objects in the database to count how
        many there are.

        """
        return self.objects.count()

    def count_candidates(self) -> int:
        """count_candidates iterates over all the candidates in the database to count
        how many there are.

        """
        return self.candidates.count()

    def count_healpixels(self) -> int:
        """count_candidates iterates over all the HEALPix pixels in the database to
        count how many have data.

        """
        return self.healpixels.count()

    def count_timestamps(self) -> int:
        """count_timestamps iterates over all the HEALPix pixels in the database to
        count how many unique timestamps have data.

        """
        return self.timestamps.count()

    def close(self) -> None:
        self.candidates.close()
        self.objects.close()
        self.healpixels.close()
        self.timestamps.close()


K = TypeVar("K")
V = TypeVar("V")


class _TypedLevelDB(Generic[K, V]):
    db: plyvel.DB
    key_codec: Codec[K]
    val_codec: Codec[V]

    def __init__(self, db: plyvel.DB, key_codec: Codec[K], val_codec: Codec[V]):
        self.db = db
        self.key_codec = key_codec
        self.val_codec = val_codec

    def close(self) -> None:
        self.db.close()

    def get(self, key: K) -> Optional[V]:
        val = self.db.get(self.key_codec.pack(key))
        if val is None:
            return None
        return self.val_codec.unpack(val)

    def iterate(self, start: K, stop: K) -> Iterator[V]:
        with self.db.iterator(
            start=self.key_codec.pack(start),
            stop=self.key_codec.pack(stop),
            include_key=False,
        ) as it:
            for v in it:
                yield self.val_codec.unpack(v)

    def put(self, key: K, val: V) -> None:
        self.db.put(self.key_codec.pack(key), self.val_codec.pack(val))

    def append(self, key: K, val: V) -> None:
        encoded_key = self.key_codec.pack(key)
        prev = self.db.get(encoded_key, default=b"", fill_cache=False)
        self.db.put(encoded_key, prev + self.val_codec.pack(val))

    def count(self) -> int:
        with self.db.iterator(include_value=False) as it:
            return sum(1 for _ in it)

    def key_range_stats(self) -> Tuple[int, K, K]:
        """ Return the count, min, and max of the key space. """
        n = 0
        min_val = None
        max_val = None
        with self.db.iterator(include_value=False) as it:
            for key_raw in it:
                n += 1
                key = self.key_codec.unpack(key_raw)
                if min_val is None or key < min_val:
                    min_val = key
                if max_val is None or key > max_val:
                    max_val = key

        if min_val is None or max_val is None:
            raise ValueError("no values in the database")

        return n, min_val, max_val
