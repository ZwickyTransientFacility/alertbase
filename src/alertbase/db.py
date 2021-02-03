from typing import AsyncGenerator, Iterator, Optional, List

import pathlib
import logging
import time
from astropy.coordinates import SkyCoord, Angle

from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore
from alertbase.index import IndexDB

import asyncio

logger = logging.getLogger(__name__)


class Database:
    blobstore: Blobstore
    index: IndexDB

    def __init__(
        self,
        s3_region: str,
        bucket: str,
        indexdb_path: str,
        create_if_missing: bool = False,
    ):
        self.blobstore = Blobstore(s3_region, bucket)
        self.index = IndexDB(indexdb_path, create_if_missing)

    async def cone_search(
        self, center: SkyCoord, radius: Angle
    ) -> AsyncGenerator[AlertRecord, None]:
        candidates = self.index.cone_search(center, radius)

        async for alert in self.stream_alerts(candidates):
            yield alert

    def cone_search_synchronous(
        self, center: SkyCoord, radius: Angle
    ) -> List[AlertRecord]:
        async def gather_alerts() -> List[AlertRecord]:
            result = []
            async for alert in self.cone_search(center, radius):
                result.append(alert)
            return result

        return asyncio.run(gather_alerts())

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

        upload_queue: asyncio.Queue[AlertRecord] = asyncio.Queue()
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
                    start = time.monotonic()
                    logger.debug("uploading alert id=%s", alert.candidate_id)
                    url = await session.upload(alert)
                    self.index.insert(url, alert)
                    logger.info(
                        "uploaded alert id=%s\ttiming=%.3fs",
                        alert.candidate_id,
                        time.monotonic() - start,
                    )
                    upload_queue.task_done()
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

    async def stream_alerts(
        self, candidate_ids: Iterator[int], n_worker: int = 8
    ) -> AsyncGenerator[AlertRecord, None]:
        url_queue: asyncio.Queue[str] = asyncio.Queue()
        result_queue: asyncio.Queue[AlertRecord] = asyncio.Queue()

        n = 0
        for id in candidate_ids:
            n += 1
            url = self.index.get_url(id)
            if url is None:
                raise ValueError(f"no known URL for candidate: {id}")
            await url_queue.put(url)

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
