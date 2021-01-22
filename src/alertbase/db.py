from __future__ import annotations
from typing import Iterator

import pathlib
import plyvel
from astropy.coordinates import SkyCoord, Angle, CartesianRepresentation
import healpy

from .encoding import pack_uint64


def open_db(path: str) -> Database:
    return Database(path)


class Database:
    db_root: pathlib.Path
    objects: plyvel.DB
    candidates: plyvel.DB
    healpixels: plyvel.DB
    timestamps: plyvel.DB

    order: int

    def __init__(self, db_path: str):
        self.db_root = pathlib.Path(db_path)
        self.objects = plyvel.DB(str(self.db_root / "objects"))
        self.candidates = plyvel.DB(str(self.db_root / "candidates"))
        self.healpixels = plyvel.DB(str(self.db_root / "healpixels"))
        self.timestamps = plyvel.DB(str(self.db_root / "timestamps"))

        self.order = 12

    def cone_search(self, center: SkyCoord, radius: Angle) -> Iterator[str]:
        """

        cone_search retrieves the URLs for alerts that can be found in a region of the
        sky.

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
        for p in pixels:
            url = self.healpixels.get(pack_uint64(p))
            if url is not None:
                yield url.decode("utf-8")

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
