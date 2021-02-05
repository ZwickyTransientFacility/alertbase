import pytest
import os
import asyncio
import astropy.time
import astropy.coordinates
import boto3
import moto.server
import werkzeug.serving
import threading
from alertbase.alert import AlertRecord
from alertbase.blobstore import Blobstore


@pytest.fixture
def aws_credentials():
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    if "AWS_PROFILE" in os.environ:
        del os.environ["AWS_PROFILE"]


@pytest.fixture
def s3_server(aws_credentials):
    # Starts a mock S3 server in a separate thread. Yields the server, and then
    # turns it off after the test finishes.
    ip = "127.0.0.1"
    port = "5287"
    app = moto.server.DomainDispatcherApplication(
        moto.server.create_backend_app,
        service="s3",
    )
    app.debug = True

    server = werkzeug.serving.make_server(ip, port, app, True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://{ip}:{port}"
    server.shutdown()
    thread.join()


@pytest.fixture
def s3_bucket(s3_server):
    # Create a bucket in a mock S3 server, and return the bucket name.
    boto3.client("s3", endpoint_url=s3_server).create_bucket(
        Bucket="bucket",
    )
    return "bucket"


@pytest.fixture
def blobstore(s3_server, s3_bucket):
    # Create a blobstore backed by bucket and server.
    bs = Blobstore("us-west-2", s3_bucket, 1)
    bs._endpoint = s3_server
    return bs


@pytest.mark.asyncio
async def test_upload(blobstore, alert_from_file):
    async with await blobstore.session() as session:
        await session.upload(alert_from_file)
        redownload = await session.download(session.url_for(alert_from_file))
    assert alert_from_file.candidate_id == redownload.candidate_id


def test_create_blobstore():
    Blobstore("region", "bucket", 2)


@pytest.mark.asyncio
async def test_session_urls(alert_record):
    bs = Blobstore("region", "bucket", 2)
    async with await bs.session() as session:
        url = session.url_for(alert_record)
        assert url == "s3://bucket/alerts/v2/1/cid"


@pytest.mark.asyncio
async def test_concurrency_limit_exceeded(alert_record):
    """
    Try to create 3 sessions with a concurrency limit of 2.
    """
    bs = Blobstore("region", "bucket", 2)
    session1 = await bs.session()
    await session1.__aenter__()

    session2 = await bs.session()
    await session2.__aenter__()

    session3 = await bs.session()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(session3.__aenter__(), 0.1)

    await session2.__aexit__(None, None, None)
    await session3.__aenter__()
    await session3.__aexit__(None, None, None)
    await session1.__aexit__(None, None, None)


@pytest.fixture
def alert_record():
    return AlertRecord(
        candidate_id="cid",
        object_id=1,
        timestamp=astropy.time.Time("2010-01-01T00:00:00"),
        raw_data=b"123",
        raw_dict={},
        position=astropy.coordinates.SkyCoord(
            ra=0,
            dec=90,
            unit="deg",
        ),
    )


@pytest.fixture
def alert_from_file():
    alert_file = "testdata/alertfiles/1311156250015010003.avro"
    ar = AlertRecord.from_file_safe(open(alert_file, "rb"))
    return ar
