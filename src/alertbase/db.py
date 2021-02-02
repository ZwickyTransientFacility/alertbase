from typing import AsyncGenerator, Iterator, Optional, List, Any

import pathlib
import logging
import time

from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore
from alertbase.index import IndexDB

import asyncio

logger = logging.getLogger(__name__)


class Database:
    blobstore: Blobstore
    index: IndexDB

    def __init__(self, s3_region: str, bucket: str, indexdb_path: str, create_if_missing: bool = False):
        self.blobstore = Blobstore(s3_region, bucket)
        self.index = IndexDB(indexdb_path, create_if_missing)

    async def upload_tarfile(
            self, tarfile_path: pathlib.Path, n_worker: int = 8, limit: Optional[int] = None,
    ) -> None:
        upload_queue: asyncio.Queue[AlertRecord] = asyncio.Queue()
        tarfile_read_done = asyncio.Event()

        async def tarfile_to_queue() -> None:
            n = 0
            for alert in iterate_tarfile(tarfile_path):
                logger.info("scanned alert %s", alert.candidate_id)
                await upload_queue.put(alert)
                n += 1
                if limit is not None and n >= limit:
                    break
            tarfile_read_done.set()
            logger.info("done processing tarfile")

        async def process_queue(bs) -> None:
            logger.info("process queue online")
            while True:
                alert = await upload_queue.get()
                start = time.monotonic()
                logger.debug("uploading alert id=%s", alert.candidate_id)
                url = await bs.upload_alert_async(alert)
                self.index.insert(url, alert)
                logger.info("uploaded alert id=%s\ttiming=%.3fs",
                             alert.candidate_id, time.monotonic() - start)
                upload_queue.task_done()
            logger.info("uploader task done")

        asyncio.create_task(tarfile_to_queue())
        async with self.blobstore:
            tasks = []
            for i in range(n_worker):
                logger.info("spinning up uploader task id=%d", i)
                task = asyncio.create_task(process_queue(self.blobstore))
                tasks.append(task)

            async def shutdown_workers() -> None:
                # Wait for the scan of the file to complete
                await tarfile_read_done.wait()
                # Wait for everything in the queue to get handled, _or_ for a task to die on its own.
                try:
                    logger.info("waiting...")
                    wait_tasks = (
                        upload_queue.join(),
                        asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION),
                    )

                    done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
                    await asyncio.gather(*list(done))

                except Exception as e:
                    raise e
                finally:
                    for t in tasks:
                        t.cancel()

            await shutdown_workers()

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
            while True:
                url = await url_queue.get()
                alert = await self.blobstore.download_alert_async(url)
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
