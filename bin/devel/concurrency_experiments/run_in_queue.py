import os
import time
import astropy.time
import asyncio
from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore


async def upload_and_download():
    filepath = "testdata/alertfiles/ztf_public_20210120.tar.gz"
    bucket = "ztf-alert-archive-prototyping-tmp"

    n_alerts = 500
    n_uploaders = 8
    n_downloaders = 8
    max_s3_concurrency = 16

    bs = Blobstore(bucket, max_concurrency=max_s3_concurrency)

    alerts_from_disk = asyncio.Queue(1000)
    uploaded_alert_urls = asyncio.Queue(1000)
    all_alerts_read = asyncio.Event()

    # Read files from the tarball
    reader_task = asyncio.create_task(read_alerts(filepath, alerts_from_disk, all_alerts_read, n_alerts))

    # Upload them to S3
    uploader_tasks = []
    for i in range(n_uploaders):
        upload_coroutine = upload_alerts(bs, alerts_from_disk, uploaded_alert_urls)
        uploader_tasks.append(asyncio.create_task(upload_coroutine))

    # Download them back down
    downloader_tasks = []
    for i in range(n_downloaders):
        download_coroutine = redownload_alerts(bs, uploaded_alert_urls)
        downloader_tasks.append(asyncio.create_task(download_coroutine))

    # Wait for everything to be read
    await all_alerts_read.wait()
    print("done reading, waiting for uploads")
    # Wait for everything to be uploaded
    await alerts_from_disk.join()
    print ("done uploading, waiting for downloads")
    # Wait for everything to be redownloaded
    await uploaded_alert_urls.join()
    print("done downloading")
    # Shut down running tasks
    print("canceling uploads")
    for task in uploader_tasks:
        task.cancel()
    print("canceling downloads")
    for task in downloader_tasks:
        task.cancel()
    print("done with shutdown")

async def read_alerts(filepath: str, q: asyncio.Queue, waiter: asyncio.Event, read_max:int=-1) -> None:
    iterator = iterate_tarfile(filepath)
    n_read = 0
    for alert in iterator:
        await q.put(alert)
        n_read += 1
        if read_max > 0 and n_read >= read_max:
            break
    waiter.set()

async def upload_alerts(bs: Blobstore, alerts: asyncio.Queue, urls: asyncio.Queue):
    while True:
        alert = await alerts.get()
        start = time.monotonic()
        url = await bs.upload_alert_async(alert)
        print(f"upload {url} done - took {time.monotonic() - start}")
        await urls.put(url)
        alerts.task_done()

async def redownload_alerts(bs: Blobstore, urls: asyncio.Queue):
    while True:
        url = await urls.get()
        print(f"download {url} start")
        start = time.monotonic()
        alert = await bs.download_alert_async(url)
        urls.task_done()
        print(f"got alert back out: {alert.candidate_id} - took {time.monotonic() - start}")


def main():
    asyncio.run(upload_and_download())

if __name__ == "__main__":
    main()
