from __future__ import annotations

from types import TracebackType
from typing import AsyncGenerator, Iterator, Optional, List, Union, Type

import pathlib
import logging
import time
from astropy.time import Time
from astropy.coordinates import SkyCoord, Angle

from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore, BlobstoreSession
from alertbase.index import IndexDB
from alertbase.dbmeta import DBMeta

import asyncio

logger = logging.getLogger(__name__)


class Database:
    blobstore: Blobstore
    index: IndexDB
    meta: DBMeta
    db_path: pathlib.Path

    any_writes: bool = False

    def __init__(
        self,
        s3_region: str,
        bucket: str,
        db_path: Union[pathlib.Path, str],
        create_if_missing: bool = False,
    ):
        """ Legacy constructor."""
        self.db_path = pathlib.Path(db_path)
        self.index = IndexDB(db_path, create_if_missing)
        self.blobstore = Blobstore(s3_region, bucket)

        meta_path = Database._meta_path(db_path)
        if meta_path.exists():
            with open(meta_path, "r") as f:
                self.meta = DBMeta.read_from_file(f)
        else:
            self.meta = DBMeta(bucket, s3_region)
            self.meta.compute_keyranges(self.index)
        with open(meta_path, "w") as f:
            self.meta.write_to_file(f)

    @classmethod
    def create(
        cls, region: str, bucket: str, db_path: Union[str, pathlib.Path]
    ) -> Database:
        return Database(region, bucket, db_path, True)

    @classmethod
    def open(cls, db_path: Union[str, pathlib.Path]) -> Database:
        meta_path = Database._meta_path(db_path)
        with open(meta_path, "r") as f:
            meta = DBMeta.read_from_file(f)
        return Database(
            s3_region=meta.s3_region,
            bucket=meta.s3_bucket,
            db_path=db_path,
            create_if_missing=False,
        )

    def __enter__(self) -> Database:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        self.close()
        return None

    @staticmethod
    def _meta_path(db_path: Union[str, pathlib.Path]) -> pathlib.Path:
        return db_path / pathlib.Path("meta.json")

    def close(self) -> None:
        if self.any_writes:
            logger.info("since writes took place, computing meta.json key ranges")
            self.meta.compute_keyranges(self.index)
        else:
            logger.debug("skipping keyrange computation because no writes occurred")
        with open(Database._meta_path(self.db_path), "w") as f:
            self.meta.write_to_file(f)
        self.index.close()

    async def _write(self, alert: AlertRecord, session: BlobstoreSession) -> None:
        self.any_writes = True
        start = time.monotonic()
        logger.debug("writing alert id=%s", alert.candidate_id)
        url = await session.upload(alert)
        self.index.insert(url, alert)
        logger.info(
            "wrote alert id=%s\ttiming=%.3fs",
            alert.candidate_id,
            time.monotonic() - start,
        )

    def get_by_candidate_id(self, candidate_id: int) -> Optional[AlertRecord]:
        url = self.index.get_url(candidate_id)
        if url is None:
            return None

        async def fetch(url: str) -> AlertRecord:
            async with await self.blobstore.session() as session:
                return await session.download(url)

        return asyncio.run(fetch(url))

    def get_by_object_id(self, object_id: str) -> List[AlertRecord]:
        candidates = self.index.object_search(object_id)
        return self._download_alerts(candidates)

    def get_by_time_range(self, start: Time, end: Time) -> List[AlertRecord]:
        candidates = self.index.timerange_search(start, end)
        return self._download_alerts(candidates)

    def get_by_cone_search(self, center: SkyCoord, radius: Angle) -> List[AlertRecord]:
        candidates = self.index.cone_search(center, radius)
        return self._download_alerts(candidates)

    async def get_by_object_id_async(
        self, object_id: str
    ) -> AsyncGenerator[AlertRecord, None]:
        candidates = self.index.object_search(object_id)
        return self._stream_alerts(candidates)

    async def get_by_time_range_async(
        self, start: Time, end: Time
    ) -> AsyncGenerator[AlertRecord, None]:
        candidates = self.index.timerange_search(start, end)
        return self._stream_alerts(candidates)

    async def get_by_cone_search_async(
        self, center: SkyCoord, radius: Angle
    ) -> AsyncGenerator[AlertRecord, None]:
        candidates = self.index.cone_search(center, radius)
        return self._stream_alerts(candidates)

    def _download_alerts(self, candidates: Iterator[int]) -> List[AlertRecord]:
        """Run an async loop to get all the candidates' associated alert data. Block
        until it's complete, returning a complete list.
        """

        async def fetch(candidates: Iterator[int]) -> List[AlertRecord]:
            result = []
            async for alert in self._stream_alerts(candidates):
                result.append(alert)
            return result

        return asyncio.run(fetch(candidates))

    async def _stream_alerts(
        self,
        candidate_ids: Iterator[int],
    ) -> AsyncGenerator[AlertRecord, None]:
        """Asynchronously fetch all the candidates' associated alert data. Returns an
        asynchronous generator over the alerts.
        """
        url_queue: asyncio.Queue[str] = asyncio.Queue()
        result_queue: asyncio.Queue[AlertRecord] = asyncio.Queue()

        n = 0
        for id in candidate_ids:
            n += 1
            url = self.index.get_url(id)
            if url is None:
                raise ValueError(f"no known URL for candidate: {id}")
            await url_queue.put(url)

        if n < 10:
            n_worker = 1
        elif n < 20:
            n_worker = 2
        elif n < 40:
            n_worker = 3
        elif n < 60:
            n_worker = 4
        elif n < 80:
            n_worker = 5
        else:
            n_worker = 8

        async def process_queue() -> None:
            async with await self.blobstore.session() as session:
                while True:
                    url = await url_queue.get()
                    alert = await session.download(url)
                    await result_queue.put(alert)

        tasks = []
        for i in range(n_worker):
            task = asyncio.create_task(process_queue())
            tasks.append(task)

        for i in range(n):
            result = await result_queue.get()
            yield result

        for t in tasks:
            t.cancel()

    async def upload_tarfile(
        self,
        tarfile_path: pathlib.Path,
        n_worker: int = 8,
        limit: Optional[int] = None,
        skip_existing: bool = False,
    ) -> None:
        """Upload a ZTF-style tarfile of alert data using a pool of workers to
        concurrently upload alerts.

        tarfile_path: a local path on disk to a gzipped tarfile containing
        individual avro-serialized alert files.

        n_worker: the number of concurrent S3 sessions to open for uploading.

        limit: maximum numbr of alerts to upload.

        skip_existing: if true, don't upload alerts which are already present in
        the local index

        """

        # Putting a limit on the queue size ensures that we don't slurp
        # _everything_ into memory at once.
        upload_queue: asyncio.Queue[AlertRecord] = asyncio.Queue(100)
        tarfile_read_done = asyncio.Event()

        async def tarfile_to_queue() -> None:
            n = 0
            for alert in iterate_tarfile(tarfile_path):
                logger.info("scanned alert %s", alert.candidate_id)
                if skip_existing:
                    if self.index.get_url(alert.candidate_id) is not None:
                        logger.info("alert is already stored, skipping it")
                        continue
                n += 1
                if limit is not None and n > limit:
                    logger.info("tarfile limit reached")
                    break
                await upload_queue.put(alert)
                await asyncio.sleep(0)  # Yield to the scheduler.
            tarfile_read_done.set()
            logger.info("done processing tarfile")

        async def process_queue() -> None:
            logger.debug("process queue online")
            async with await self.blobstore.session() as session:
                while True:
                    if tarfile_read_done.is_set() and upload_queue.empty():
                        # All input is done, so exit
                        break
                    # More to go
                    alert = await upload_queue.get()
                    await self._write(alert, session)
                    upload_queue.task_done()
                    await asyncio.sleep(0)  # Yield to the scheduler.
            logger.debug("uploader task done")

        uploaders = []

        asyncio.create_task(tarfile_to_queue())
        for i in range(n_worker):
            logger.info("spinning up uploader task id=%d", i)
            task = asyncio.create_task(
                process_queue(),
            )
            uploaders.append(task)

        try:
            await asyncio.gather(*uploaders)
        finally:
            for u in uploaders:
                u.cancel()
