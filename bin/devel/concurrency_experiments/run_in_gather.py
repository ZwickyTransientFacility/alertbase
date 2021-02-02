import asyncio
import asyncio.queues
from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
from alertbase.blobstore import Blobstore


async def upload_and_download():
    filepath = "testdata/alertfiles/ztf_public_20210120.tar.gz"
    bucket = "ztf-alert-archive-prototyping-tmp"
    bs = Blobstore(bucket)
    iterator = iterate_tarfile(filepath)

    tasks = []
    for alert in iterator:
        print(f"file read: {alert.candidate_id}")
        tasks.append(bs.upload_alert_async(alert))

    urls = await asyncio.gather(*tasks)
    tasks = []
    for url in urls:
        print(f"uploaded to: {url}")
        tasks.append(bs.download_alert_async(url))

    downloads = await asyncio.gather(*tasks)
    for download in downloads:
        print(f"downloaded: {download.candidate_id}")


def main():
    asyncio.run(upload_and_download_tar())

if __name__ == "__main__":
    main()
