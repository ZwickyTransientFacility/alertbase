from typing import AsyncGenerator, Iterator

import pathlib

from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore
from alertbase.index import IndexDB

import asyncio


class Database:
    blobstore: Blobstore
    index: IndexDB

    def __init__(self, bucket: str, indexdb_path: str, create_if_missing: bool = False):
        self.blobstore = Blobstore(bucket)
        self.index = IndexDB(indexdb_path, create_if_missing)

    async def upload_tarfile(
        self, tarfile_path: pathlib.Path, n_worker: int = 8
    ) -> None:
        upload_queue: asyncio.Queue[AlertRecord] = asyncio.Queue()

        async def tarfile_to_queue() -> None:
            for alert in iterate_tarfile(tarfile_path):
                await upload_queue.put(alert)

        async def process_queue() -> None:
            while True:
                alert = await upload_queue.get()
                url = await self.blobstore.upload_alert_async(alert)
                self.index.insert(url, alert)
                upload_queue.task_done()

        asyncio.create_task(tarfile_to_queue())
        tasks = []
        for i in range(n_worker):
            task = asyncio.create_task(process_queue())
            tasks.append(task)

        await upload_queue.join()
        for t in tasks:
            t.cancel()

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
