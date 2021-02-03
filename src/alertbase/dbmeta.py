from __future__ import annotations

from typing import Generic, TypeVar, Any

from astropy.time import Time
from dataclasses import dataclass

from alertbase.index import IndexDB, _TypedLevelDB


@dataclass
class DBMeta:
    s3_bucket: str
    s3_region: str

    candidates: DBMetaKeyRange[int]
    objects: DBMetaKeyRange[str]
    healpixels: DBMetaKeyRange[int]
    timestamps: DBMetaKeyRange[Time]

    def __init__(self, bucket: str, region: str, idx: IndexDB):
        self.s3_bucket = bucket
        self.s3_region = region
        self.candidates = DBMetaKeyRange.from_db(idx.candidates)
        self.objects = DBMetaKeyRange.from_db(idx.objects)
        self.healpixels = DBMetaKeyRange.from_db(idx.healpixels)
        self.timestamps = DBMetaKeyRange.from_db(idx.timestamps)


T = TypeVar("T")


@dataclass
class DBMetaKeyRange(Generic[T]):
    count: int
    min: T
    max: T

    @classmethod
    def from_db(cls, db: _TypedLevelDB[T, Any]) -> DBMetaKeyRange[T]:
        count, min, max = db.key_range_stats()
        return cls(
            count=count,
            min=min,
            max=max,
        )
