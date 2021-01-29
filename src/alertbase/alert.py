from __future__ import annotations
from typing import Dict, Optional, Any, IO
import io

from avro.io import BinaryDecoder, DatumReader
from avro.datafile import META_SCHEMA, DataFileReader
from avro import schema

import fastavro
import json

from dataclasses import dataclass
from astropy.coordinates import SkyCoord
from astropy.time import Time

from alertbase import alert_schemas

_optional_float_schema = ["null", "float"]
_optional_float_avro = schema.parse(json.dumps(_optional_float_schema))
_optional_float_fastavro = fastavro.parse_schema(_optional_float_schema)
_optional_string_schema = ["null", "string"]
_optional_string_avro = schema.parse(json.dumps(_optional_string_schema))
_optional_string_fastavro = fastavro.parse_schema(_optional_string_schema)
_optional_long_schema = ["null", "long"]
_optional_long_avro = schema.parse(json.dumps(_optional_long_schema))
_optional_long_fastavro = fastavro.parse_schema(_optional_long_schema)
_optional_int_schema = ["null", "int"]
_optional_int_avro = schema.parse(json.dumps(_optional_int_schema))
_optional_int_fastavro = fastavro.parse_schema(_optional_int_schema)

_fastavro_META_SCHEMA = fastavro.parse_schema(META_SCHEMA.to_json())


@dataclass
class AlertRecord:
    candidate_id: int
    object_id: str
    position: SkyCoord
    timestamp: Time

    raw_data: Optional[bytes]
    raw_dict: Optional[Dict[str, Any]]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlertRecord:
        """
        Constructs an AlertRecord from an avro-style dictionary representing a ZTF
        alert.

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
    def from_file_unsafe(cls, fp: IO[bytes]) -> AlertRecord:
        """Read from an alert file stored on disk, recklessly making assumptions about
        its schema. This is *much* faster (~20-50x) than the safe call, but can
        yield errors, or severely incorrect data in the worst case.

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
        dr.skip_union(_optional_float_avro, decoder)  # diffmaglim
        dr.skip_union(_optional_string_avro, decoder)  # pdiffimfilename
        dr.skip_union(_optional_string_avro, decoder)  # programpi
        decoder.skip_int()  # programid
        decoder.skip_long()  # candid
        decoder.skip_utf8()  # isdiffpos
        dr.skip_union(_optional_long_avro, decoder)  # tblid
        dr.skip_union(_optional_int_avro, decoder)  # nid
        dr.skip_union(_optional_int_avro, decoder)  # rcid
        dr.skip_union(_optional_int_avro, decoder)  # field
        dr.skip_union(_optional_float_avro, decoder)  # xpos
        dr.skip_union(_optional_float_avro, decoder)  # ypos

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

    @classmethod
    def from_file_safe(cls, fp: IO[bytes]) -> AlertRecord:
        """ Read from an alert file stored on disk. """
        # Save a copy of the raw bytes
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)
        with DataFileReader(buf, DatumReader()) as df:
            alert_dict = next(df)
            alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert

    @classmethod
    def from_file_subschema(cls, fp: IO[bytes]) -> AlertRecord:
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)
        with DataFileReader(
            buf,
            DatumReader(
                writers_schema=alert_schemas.avro_writer_schema,
                readers_schema=alert_schemas.avro_reader_schema,
            ),
        ) as df:
            alert_dict = next(df)
            alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert

    @classmethod
    def from_file_fastavro_safe(cls, fp: IO[bytes]) -> AlertRecord:
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)
        alert_dict = next(fastavro.reader(buf))
        alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert

    @classmethod
    def from_file_fastavro_unsafe(cls, fp: IO[bytes]) -> AlertRecord:
        import fastavro._read

        # Save a copy of the raw bytes
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)

        # Skip the file header
        fastavro._read.skip_record(buf, _fastavro_META_SCHEMA, {})

        # Num objects in the block
        block_count = fastavro._read.read_long(buf)
        assert block_count == 1
        # Size in bytes of the serialized objects in the block
        _ = fastavro._read.skip_long(buf)

        # Skip schemavsn, publisher
        fastavro._read.skip_utf8(buf)
        fastavro._read.skip_utf8(buf)

        # Read candidate ID, object ID
        object_id = fastavro._read.read_utf8(buf)
        candidate_id = fastavro._read.read_long(buf)

        # Read jd, the julian date of the observation
        jd = fastavro._read.read_double(buf)
        timestamp = Time(jd, format="jd")

        # Skip many fields:
        fastavro._read.skip_int(buf)  # fid
        fastavro._read.skip_long(buf)  # pid
        fastavro._read.skip_union(buf, _optional_float_fastavro, {})  # diffmaglim
        fastavro._read.skip_union(buf, _optional_string_fastavro, {})  # pdiffimfilename
        fastavro._read.skip_union(buf, _optional_string_fastavro, {})  # programpi
        fastavro._read.skip_int(buf)  # programid
        fastavro._read.skip_long(buf)  # candid
        fastavro._read.skip_utf8(buf)  # isdiffpos
        fastavro._read.skip_union(buf, _optional_long_fastavro, {})  # tblid
        fastavro._read.skip_union(buf, _optional_int_fastavro, {})  # nid
        fastavro._read.skip_union(buf, _optional_int_fastavro, {})  # rcid
        fastavro._read.skip_union(buf, _optional_int_fastavro, {})  # field
        fastavro._read.skip_union(buf, _optional_float_fastavro, {})  # xpos
        fastavro._read.skip_union(buf, _optional_float_fastavro, {})  # ypos

        ra = fastavro._read.read_double(buf)
        dec = fastavro._read.read_double(buf)
        pos = SkyCoord(ra=ra, dec=dec, unit="deg")

        return AlertRecord(
            object_id=object_id,
            candidate_id=candidate_id,
            position=pos,
            timestamp=timestamp,
            raw_data=raw_data,
            raw_dict=None,
        )

    @classmethod
    def from_file_fastavro_subschema(cls, fp: IO[bytes]) -> AlertRecord:
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)
        alert_dict = next(
            fastavro.reader(buf, reader_schema=alert_schemas.fastavro_reader_schema)
        )
        alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert

    @classmethod
    def from_file_precompile(cls, fp: IO[bytes]) -> AlertRecord:
        # Save a copy of the raw bytes
        raw_data = fp.read()
        buf = io.BytesIO(raw_data)

        # Skip the file header
        fastavro._read.skip_record(buf, _fastavro_META_SCHEMA, {})

        # Num objects in the block
        block_count = fastavro._read.read_long(buf)
        assert block_count == 1
        # Size in bytes of the serialized objects in the block
        _ = fastavro._read.skip_long(buf)

        alert_dict = alert_schemas.precompiled_writer_parser(buf)
        alert = cls.from_dict(alert_dict)
        alert.raw_data = raw_data
        return alert
