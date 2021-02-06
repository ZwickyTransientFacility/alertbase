from __future__ import annotations
from typing import Dict, Optional, Any, IO
import io

from avro.io import BinaryDecoder, DatumReader
from avro.datafile import META_SCHEMA, DataFileReader
from avro import schema

from dataclasses import dataclass
from astropy.coordinates import SkyCoord, CartesianRepresentation
from astropy.time import Time
import healpy

_optional_float = schema.parse('["null", "float"]')
_optional_string = schema.parse('["null", "string"]')
_optional_long = schema.parse('["null", "long"]')
_optional_int = schema.parse('["null", "int"]')


@dataclass
class AlertRecord:
    #: The ZTF Candidate ID for this alert.
    candidate_id: int

    #: The ZTF Object ID for this alert.
    object_id: str
    position: SkyCoord  #: The RA-Dec position of this alert.
    timestamp: Time  #: The time of the exposure that sourced this alert.

    raw_data: Optional[bytes]  #: The Avro serialization of this dalert's data.
    raw_dict: Optional[Dict[str, Any]]  #: The full alert payload.

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertRecord:
        """
        Constructs an AlertRecord from an avro-style dictionary representing a ZTF
        alert.

        :param d: The dictionary to load from.
        :returns: The AlertRecord with all fields filled out.
        """
        pos = SkyCoord(
            ra=d["candidate"]["ra"],
            dec=d["candidate"]["dec"],
            unit="deg",
        )
        timestamp = Time(d["candidate"]["jd"], format="jd")

        return AlertRecord(
            object_id=d["objectId"],
            candidate_id=d["candid"],
            position=pos,
            timestamp=timestamp,
            raw_dict=d,
            raw_data=None,
        )

    @classmethod
    def from_file_safe(cls, fp: IO[bytes]) -> AlertRecord:
        """
        Read from an Avro alert file stored on disk.

        :param fp: A file-like object to read raw bytes from for
                   deserialization.
        :returns: The fully deserialized AlertRecord.
        """
        # Save a copy of the raw bytes
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)
        with DataFileReader(buf, DatumReader()) as df:
            alert_dict = next(df)
            alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert

    @classmethod
    def from_file_unsafe(cls, fp: IO[bytes]) -> AlertRecord:
        """
        Read from an alert file stored on disk, recklessly making assumptions about
        its schema. This is *much* faster (~20-50x) than the safe call, but can
        yield errors, or severely incorrect data in the worst case.

        The returned AlertRecord will have its :py:attr:`raw_data` field filled
        out, but not :py:attr:`raw_dict`.

        :param fp: A file-like object to ead raw bytes from for deserialization.
        :returns: The deserialized AlertRecord.
        """
        # Save a copy of the raw bytes
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)

        decoder = BinaryDecoder(buf)
        dr = DatumReader()
        # Skip the file header
        dr.skip_record(META_SCHEMA, decoder)

        # Num objects in the block
        block_count = decoder.read_long()
        assert block_count == 1
        # Size in bytes of the serialized objects in the block
        _ = decoder.read_long()

        # Skip schemavsn, publisher
        decoder.skip_utf8()
        decoder.skip_utf8()

        # Read candidate ID, object ID
        object_id = decoder.read_utf8()
        candidate_id = decoder.read_long()

        # Read jd, the julian date of the observation
        jd = decoder.read_double()
        timestamp = Time(jd, format="jd")

        # Skip many fields:
        decoder.skip_int()  # fid
        decoder.skip_long()  # pid
        dr.skip_union(_optional_float, decoder)  # diffmaglim
        dr.skip_union(_optional_string, decoder)  # pdiffimfilename
        dr.skip_union(_optional_string, decoder)  # programpi
        decoder.skip_int()  # programid
        decoder.skip_long()  # candid
        decoder.skip_utf8()  # isdiffpos
        dr.skip_union(_optional_long, decoder)  # tblid
        dr.skip_union(_optional_int, decoder)  # nid
        dr.skip_union(_optional_int, decoder)  # rcid
        dr.skip_union(_optional_int, decoder)  # field
        dr.skip_union(_optional_float, decoder)  # xpos
        dr.skip_union(_optional_float, decoder)  # ypos

        ra = decoder.read_double()
        dec = decoder.read_double()
        pos = SkyCoord(ra=ra, dec=dec, unit="deg")

        return AlertRecord(
            object_id=object_id,
            candidate_id=candidate_id,
            position=pos,
            timestamp=timestamp,
            raw_data=raw_data,
            raw_dict=None,
        )

    def healpixel(self, map_order: int) -> int:
        pos = self.position.replicate(representation_type=CartesianRepresentation)
        return healpy.vec2pix(
            nside=healpy.order2nside(map_order),
            x=pos.x.value,
            y=pos.y.value,
            z=pos.z.value,
            nest=True,
        )
