import boto3
import aioboto3
import io
import asyncio

from alertbase.alert import AlertRecord

# TODO: Single Object Encoding with cached schemas
#
#   We upload alerts including their full schema document. That's very wasteful
#   we could store fingerprint the thing and load the fingerprinted schema just
#   once.
#
#   For example, something like this:
#
#   alerts/v2/<obj>/<candidate> is an alert file. It is encoded with Avro Single
#   Object Encoding, so the first 2 bytes are magic, followed by an 8 byte
#   schema fingerprint, followed by the contents.
#
#   schemas/<fingerprint> store the schema documents
#
#   This Blobstore would cache fingerprints it has seen, under the presumption
#   that there are never going to be so many unique schemas that we'd ever run
#   into trouble.
#
#   When uploading, we'd need to encode with the Single Object Encoding format,
#   check whether the current schema is already uploaded (cached check, that),
#   and proceed.
#
#
# TODO: Parallelize uploads and downloads.
#
#   This seems like it should be doable just with asyncio concurrency
#   primitives.
#
#
# TODO: Write tests for blobstore.
#
#
# TODO: Write a full database unifying blobstore and indexdb.


class Blobstore:
    bucket: str
    semaphore: asyncio.Semaphore

    def __init__(self, bucket: str, max_concurrency: int = 50):
        self.bucket = bucket
        self.semaphore = asyncio.Semaphore(max_concurrency)

    def url_for(self, alert: AlertRecord) -> str:
        return f"s3://{self.bucket}/{self._key_for(alert)}"

    def _key_for(self, alert: AlertRecord) -> str:
        return f"alerts/v2/{alert.object_id}/{alert.candidate_id}"

    def _upload(self, alert_bytes: bytes, key: str) -> None:
        boto3.client("s3").put_object(
            Bucket=self.bucket,
            Key=key,
            Body=alert_bytes,
        )

    async def _upload_async(self, alert_bytes: bytes, key: str) -> None:
        async with aioboto3.client("s3") as s3:
            await s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=alert_bytes,
            )

    async def upload_alert_async(self, alert: AlertRecord) -> str:
        async with self.semaphore:
            url = self.url_for(alert)
            key = self._key_for(alert)
            print(f"doing an async upload to {url}")
            if alert.raw_data is None:
                raise ValueError("alert has no raw data associated with it")
            await self._upload_async(alert.raw_data, key)
            return url

    def upload_alert(self, alert: AlertRecord) -> str:
        url = self.url_for(alert)
        key = self._key_for(alert)
        if alert.raw_data is None:
            raise ValueError("alert has no raw data associated with it")
        self._upload(alert.raw_data, key)
        return url

    def download_alert(self, url: str) -> AlertRecord:
        if not url.startswith("s3://"):
            raise ValueError("invalid scheme, url should start with 's3://'")
        url = url[5:]
        bucket, key = url.split("/", 1)
        resp = boto3.client("s3").get_object(
            Bucket=bucket,
            Key=key,
        )
        return AlertRecord.from_file_safe(resp["Body"])

    async def download_alert_async(self, url: str) -> AlertRecord:
        async with self.semaphore:
            if not url.startswith("s3://"):
                raise ValueError("invalid scheme, url should start with 's3://'")
            url = url[5:]
            bucket, key = url.split("/", 1)
            async with aioboto3.client("s3") as s3:
                resp = await s3.get_object(
                    Bucket=bucket,
                    Key=key,
                )
                body = await resp["Body"].read()
                import functools

                f = functools.partial(AlertRecord.from_file_unsafe, io.BytesIO(body))
                return await asyncio.get_running_loop().run_in_executor(None, f)
