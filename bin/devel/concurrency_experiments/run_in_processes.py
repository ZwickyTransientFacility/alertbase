from typing import Any

import os
import time
import astropy.time
import queue
import threading
import multiprocessing as mp
import logging

from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore


def upload_and_download():
    filepath = "testdata/alertfiles/ztf_public_20210120.tar.gz"
    bucket = "ztf-alert-archive-prototyping-tmp"

    n_alerts = 500
    n_uploaders = 6
    n_downloaders = 6
    max_s3_concurrency = 20

    bs = Blobstore(bucket, max_concurrency=max_s3_concurrency)
    with mp.Manager() as manager:
        alerts_from_disk = CloseableQueue(manager.Event(), mp.Queue(100))
        uploaded_alert_urls = CloseableQueue(manager.Event(), mp.Queue(100))

        # Read files from the tarball
        reader_proc = mp.Process(
            target=read_alerts,
            name="DiskRead",
            args=(filepath, alerts_from_disk, n_alerts),
        )

        # Upload them to S3
        uploader_procs = []
        for i in range(n_uploaders):
            upload_proc = mp.Process(
                target=upload_alerts,
                name=f"Upload-{i}",
                args=(bs, alerts_from_disk, uploaded_alert_urls),
            )
            upload_proc.start()
            uploader_procs.append(upload_proc)

        # Download them back down
        downloader_procs = []
        for i in range(n_downloaders):
            download_proc = mp.Process(
                target=redownload_alerts,
                name=f"Download-{i}",
                args=(bs, uploaded_alert_urls),
            )
            download_proc.start()
            downloader_procs.append(download_proc)

        reader_proc.start()

        # Wait for everything to be read
        reader_proc.join()
        logging.debug("done reading, waiting for uploads")
        logging.debug("waiting for upload processes to exit")
        for p in uploader_procs:
            p.join()
        logging.debug("waiting for download processes to exit")
        for p in downloader_procs:
            p.join()
        logging.debug("done with shutdown")

def read_alerts(filepath: str, q: queue.Queue, read_max:int=-1) -> None:
    iterator = iterate_tarfile(filepath)
    n_read = 0
    for alert in iterator:
        q.put(alert)
        logging.debug(f"putting into q: {alert.candidate_id}")
        n_read += 1
        if read_max > 0 and n_read >= read_max:
            break
    logging.debug(f"file queue complete, exiting")
    q.close()

def upload_alerts(bs: Blobstore, alerts: queue.Queue, urls: queue.Queue):
    while True:
        try:
            alert = alerts.get()
        except QueueClosed as e:
            logging.debug(f"upload queue complete, exiting: {e}")
            break
            # Is it empty because we're fast, or because there's nothing left?
        start = time.monotonic()
        url = bs.upload_alert(alert)
        logging.debug(f"upload {url} done - took {time.monotonic() - start}")
        urls.put(url)
    urls.close()

def redownload_alerts(bs: Blobstore, urls: queue.Queue):
    while True:
        try:
            url = urls.get()
        except QueueClosed:
            logging.debug(f"download queue complete, exiting")
            return
        logging.debug(f"download {url} start")
        start = time.monotonic()
        alert = bs.download_alert(url)
        logging.debug(f"got alert back out: {alert.candidate_id} - took {time.monotonic() - start}")


class CloseableQueue:
    _closed: threading.Event
    _queue: mp.Queue

    def __init__(self, event, queue):
        self._closed = event
        self._queue = queue

    def close(self):
        self._closed.set()

    def closed(self) -> bool:
        return self._closed.is_set()

    def wait_for_close(self):
        self._closed.wait()

    def put(self, obj):
        self._queue.put(obj)

    def get(self, block=True, timeout=None) -> Any:
        if self._queue.empty() and self.closed():
            raise QueueClosed("Queue is closed")
        if block and timeout is None:
            return self._get_until_closed()
        return self._queue.get(block, timeout)

    def _get_until_closed(self, poll_interval=0.1) -> Any:
        while True:
            if self._queue.empty() and self.closed():
                raise QueueClosed("Queue is closed")
            try:
                return self._queue.get(True, poll_interval)
            except queue.Empty:
                pass


class QueueClosed(Exception):
    pass

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(relativeCreated)6d %(name)s %(processName)s %(message)s')
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("asyncio").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    upload_and_download()

if __name__ == "__main__":
    main()
