from __future__ import annotations
from typing import Iterator, Optional

import pathlib
import plyvel
from astropy.coordinates import SkyCoord, Angle, CartesianRepresentation
from astropy.time import Time
import healpy
import numpy as np

from alertbase.alert import AlertRecord

from .encoding import (
    pack_uint64,
    pack_varint,
    pack_str,
    pack_time,
    unpack_str,
    iter_varints,
)
import logging

logger = logging.getLogger(__name__)


class IndexDB:
    db_root: pathlib.Path
    objects: plyvel.DB
    candidates: plyvel.DB
    healpixels: plyvel.DB
    timestamps: plyvel.DB

    order: int

    def __init__(self, db_path: str, create_if_missing: bool = False):
        self.db_root = pathlib.Path(db_path)

        if create_if_missing:
            self.db_root.mkdir(parents=True, exist_ok=True)

        self.objects = plyvel.DB(
            str(self.db_root / "objects"),
            create_if_missing=create_if_missing,
        )
        self.candidates = plyvel.DB(
            str(self.db_root / "candidates"),
            create_if_missing=create_if_missing,
        )
        self.healpixels = plyvel.DB(
            str(self.db_root / "healpixels"),
            create_if_missing=create_if_missing,
        )
        self.timestamps = plyvel.DB(
            str(self.db_root / "timestamps"),
            create_if_missing=create_if_missing,
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
        id_bytes = pack_varint(candidate_id)
        self.candidates.put(id_bytes, pack_str(alert_url))
        self._append(self.objects, pack_str(object_id), id_bytes)
        self._append(self.healpixels, pack_uint64(healpixel), id_bytes)
        self._append(self.timestamps, pack_time(time), id_bytes)

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
        val = self.candidates.get(pack_varint(candidate_id))
        if val is None:
            return None
        return unpack_str(val)

    def object_search(self, object_id: str) -> Iterator[int]:
        """
        Retrieve the candidate IDs for a given ZTF object
        """
        raw = self.objects.get(pack_str(object_id))
        if raw is None:
            raise StopIteration
        return iter_varints(raw)

    def timerange_search(self, start: Time, end: Time) -> Iterator[int]:
        """
        Retrieve the candidate IDs for all alerts that were recorded between
        start and end time range (values should be julian dates).
        """
        iterator = self.timestamps.iterator(
            start=pack_time(start),
            stop=pack_time(end),
        )
        for _, val in iterator:
            for id in iter_varints(val):
                yield id

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
            start_bytes = pack_uint64(start)
            stop_bytes = pack_uint64(stop)
            for _, value in self.healpixels.iterator(
                start=start_bytes, stop=stop_bytes
            ):
                candidates = iter_varints(value)
                for c in candidates:
                    yield c

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
        return sum(1 for _ in self.objects.iterator())

    def count_candidates(self) -> int:
        """count_candidates iterates over all the candidates in the database to count
        how many there are.

        """
        return sum(1 for _ in self.candidates.iterator())

    def count_healpixels(self) -> int:
        """count_candidates iterates over all the HEALPix pixels in the database to
        count how many have data.

        """
        return sum(1 for _ in self.healpixels.iterator())

    def count_timestamps(self) -> int:
        """count_timestamps iterates over all the HEALPix pixels in the database to
        count how many unique timestamps have data.

        """
        return sum(1 for _ in self.timestamps.iterator())
