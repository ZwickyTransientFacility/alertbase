from __future__ import annotations

from typing import Generic, TypeVar, Any, Optional, IO, Dict

from astropy.time import Time
import dataclasses
import json
from alertbase.index import IndexDB, _TypedLevelDB


@dataclasses.dataclass
class DBMeta:
    s3_bucket: str
    s3_region: str

    candidates: DBMetaKeyStats[int]
    objects: DBMetaKeyStats[str]
    healpixels: DBMetaKeyStats[int]
    timestamps: DBMetaKeyStats[Time]

    def __init__(
        self,
        bucket: str,
        region: str,
        candidates: Optional[DBMetaKeyStats[int]] = None,
        objects: Optional[DBMetaKeyStats[str]] = None,
        healpixels: Optional[DBMetaKeyStats[int]] = None,
        timestamps: Optional[DBMetaKeyStats[Time]] = None,
    ):
        self.s3_bucket = bucket
        self.s3_region = region
        self.candidates = (
            candidates if candidates is not None else DBMetaKeyStats(0, 0, 0)
        )
        self.objects = objects if objects is not None else DBMetaKeyStats(0, "", "")
        self.healpixels = (
            healpixels if healpixels is not None else DBMetaKeyStats(0, 0, 0)
        )
        self.timestamps = (
            timestamps
            if timestamps is not None
            else DBMetaKeyStats(0, Time(0, format="unix"), Time(0, format="unix"))
        )

    def compute_keyranges(self, idx: IndexDB) -> None:
        self.candidates = DBMetaKeyStats.from_db(idx.candidates)
        self.objects = DBMetaKeyStats.from_db(idx.objects)
        self.healpixels = DBMetaKeyStats.from_db(idx.healpixels)
        self.timestamps = DBMetaKeyStats.from_db(idx.timestamps)

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def write_to_file(self, fp: IO[str]) -> None:
        """Serialize the metadata into JSON and store it in a file."""
        data = self.to_dict()
        # Convert times to unix so that they serialize into JSON easily.
        data["timestamps"]["min"] = data["timestamps"]["min"].unix
        data["timestamps"]["max"] = data["timestamps"]["max"].unix
        json.dump(data, fp)

    @classmethod
    def read_from_file(cls, fp: IO[str]) -> DBMeta:
        """
        Load metadata from a file that was previously written with
        write_to_file.
        """
        data = json.load(fp)
        return DBMeta(
            bucket=data["s3_bucket"],
            region=data["s3_region"],
            candidates=DBMetaKeyStats(
                count=data["candidates"]["count"],
                min=data["candidates"]["min"],
                max=data["candidates"]["max"],
            ),
            objects=DBMetaKeyStats(
                count=data["objects"]["count"],
                min=data["objects"]["min"],
                max=data["objects"]["max"],
            ),
            healpixels=DBMetaKeyStats(
                count=data["healpixels"]["count"],
                min=data["healpixels"]["min"],
                max=data["healpixels"]["max"],
            ),
            timestamps=DBMetaKeyStats(
                count=data["timestamps"]["count"],
                min=Time(data["timestamps"]["min"], format="unix"),
                max=Time(data["timestamps"]["max"], format="unix"),
            ),
        )


T = TypeVar("T")


@dataclasses.dataclass
class DBMetaKeyStats(Generic[T]):
    count: int
    min: T
    max: T

    @classmethod
    def from_db(cls, db: _TypedLevelDB[T, Any]) -> DBMetaKeyStats[T]:
        count, min, max = db.key_range_stats()
        return cls(
            count=count,
            min=min,
            max=max,
        )
