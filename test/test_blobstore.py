import pytest
import astropy.time
import astropy.coordinates
from alertbase.alert import AlertRecord
from alertbase.blobstore import Blobstore


def test_create_blobstore():
    Blobstore("region", "bucket", 2)


@pytest.mark.asyncio
async def test_session_urls(alert_record):
    bs = Blobstore("region", "bucket", 2)
    async with await bs.session() as session:
        url = session.url_for(alert_record)
        assert url == "s3://bucket/alerts/v2/1/cid"


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
