from __future__ import annotations
from typing import Optional

import io
import asyncio
import aiobotocore.session
import aiobotocore.client
from aiobotocore.config import AioConfig

import contextlib

import logging
import functools

from alertbase.alert import AlertRecord

logger = logging.getLogger(__name__)

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
# TODO: Write tests for blobstore.
#
# TODO: Keep a pool of active sessions
_aio_boto_config = AioConfig(
    connector_args=dict(
        # These get passed in to the aiobotocore AioEndpointCreator, and from
        # there they get passed in to the aiohttp TCPConnector constructore.
        use_dns_cache=True,
        keepalive_timeout=15,
    ),
    retries=dict(max_attempts=10, mode="standard"),
)


class Blobstore:
    region: str
    bucket: str
    semaphore: asyncio.Semaphore  # Limits active number of BlobstoreSessions

    # S3 endpoint; overwritten in tests
    _endpoint: Optional[str]

    def __init__(self, s3_region: str, bucket: str, max_concurrency: int = 50):
        """
        Construct a new Blobstore.

        max_concurrency sets the maximum number of concurrent sessions.
        """
        self.region = s3_region
        self.bucket = bucket
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._endpoint = None

    async def session(self) -> BlobstoreSession:
        return BlobstoreSession(
            region=self.region,
            bucket=self.bucket,
            semaphore=self.semaphore,
            endpoint=self._endpoint,
        )


class BlobstoreSession:
    def __init__(
        self,
        region: str,
        bucket: str,
        semaphore: asyncio.Semaphore,
        endpoint: Optional[str] = None,
    ):
        self._region = region
        self._bucket = bucket
        self._sem = semaphore
        self._endpoint = endpoint
        self._s3_client: Optional[aiobotocore.client.AioBaseClient] = None
        self._exit_stack = contextlib.AsyncExitStack()

    async def __aenter__(self) -> BlobstoreSession:
        await self._sem.acquire()
        session = aiobotocore.session.AioSession()
        self._s3_client = await self._exit_stack.enter_async_context(
            session.create_client(
                "s3",
                region_name=self._region,
                config=_aio_boto_config,
                endpoint_url=self._endpoint,
            )
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        self._s3_client = None
        self._sem.release()

    def url_for(self, alert: AlertRecord) -> str:
        return f"s3://{self._bucket}/{self._key_for(alert)}"

    def _key_for(self, alert: AlertRecord) -> str:
        return f"alerts/v2/{alert.object_id}/{alert.candidate_id}"

    async def upload(self, alert: AlertRecord) -> str:
        url = self.url_for(alert)
        key = self._key_for(alert)
        logging.debug("doing an async upload to %s", url)
        if alert.raw_data is None:
            raise ValueError("alert has no raw data associated with it")
        assert self._s3_client is not None
        await self._s3_client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=alert.raw_data,
        )
        return url

    async def download(self, url: str) -> AlertRecord:
        if not url.startswith("s3://"):
            raise ValueError("invalid scheme, url should start with 's3://'")
        url = url[5:]
        bucket, key = url.split("/", 1)
        assert self._s3_client is not None
        resp = await self._s3_client.get_object(
            Bucket=self._bucket,
            Key=key,
        )
        body = await resp["Body"].read()

        f = functools.partial(AlertRecord.from_file_safe, io.BytesIO(body))
        return await asyncio.get_running_loop().run_in_executor(None, f)
