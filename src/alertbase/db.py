from __future__ import annotations
from typing import Iterator

import pathlib
import plyvel
import astropy.coordinates


def open_db(path: str) -> Database:
    return Database(path)


class Database:
    db_root: pathlib.Path
    objects: plyvel.DB
    candidates: plyvel.DB
    healpixels: plyvel.DB
    timestamps: plyvel.DB

    def __init__(self, db_path: str):
        self.db_root = pathlib.Path(db_path)
        self.objects = plyvel.DB(str(self.db_root / "objects"))
        self.candidates = plyvel.DB(str(self.db_root / "candidates"))
        self.healpixels = plyvel.DB(str(self.db_root / "healpixels"))
        self.timestamps = plyvel.DB(str(self.db_root / "timestamps"))

    def cone_search(self, center: astropy.coordinates.SkyCoord) -> Iterator[str]:
        pass

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
