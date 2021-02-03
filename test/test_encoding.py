import alertbase.encoding
import pytest
import astropy.time
import struct


class TestVarintIteratorCodec:
    cases = [
        (
            [],
            bytearray.fromhex(""),
        ),
        (
            [0],
            bytearray.fromhex("00"),
        ),
        (
            [1, 2, 3, 4],
            bytearray.fromhex("02040608"),
        ),
        (
            [128, 256, 512],
            bytearray.fromhex("800280048008"),
        ),
        (
            [1 << 32, 1 << 60, (1 << 63) - 1],
            bytearray.fromhex(
                "8080 8080 2080 8080" + "8080 8080 8020 feff" + "ffff ffff ffff ff01"
            ),
        ),
    ]

    @pytest.mark.parametrize("values,encoded", cases)
    def test_packing(self, values, encoded):
        have = alertbase.encoding.varint_iterator_codec.pack(iter(values))
        assert have == encoded

    @pytest.mark.parametrize("values,encoded", cases)
    def test_unpacking(self, values, encoded):
        have_iter = alertbase.encoding.varint_iterator_codec.unpack(encoded)
        have = list(have_iter)
        assert have == values


class TestTimePacking:
    def test_pack_time_roundtrip(self):
        time = astropy.time.Time("2010-01-01T00:00:00")
        packed = alertbase.encoding.pack_time(time)
        unpacked = alertbase.encoding.unpack_time(packed)
        assert time == unpacked


class TestUint64Codec:
    cases = [0, 1, int(1e12), 1 << 32, 1 << 63, (1 << 64) - 1]

    @pytest.mark.parametrize("val", cases)
    def test_roundtrip(self, val):
        packed = alertbase.encoding.uint64_codec.pack(val)
        unpacked = alertbase.encoding.uint64_codec.unpack(packed)
        assert unpacked == val

    def test_negative(self):
        with pytest.raises(struct.error):
            alertbase.encoding.uint64_codec.pack(-1)

    def test_float(self):
        with pytest.raises(struct.error):
            alertbase.encoding.uint64_codec.pack(1.0)

    def test_overflow(self):
        with pytest.raises(struct.error):
            alertbase.encoding.uint64_codec.pack(1 << 64)


class TestVarintCodec:
    cases = [0, 1, -1, 1 << 6, 1 << 7, 1 << 8, int(1e12), int(-1e12), 1 << 63, 1 << 100]

    @pytest.mark.parametrize("val", cases)
    def test_roundtrip(self, val):
        packed = alertbase.encoding.varint_codec.pack(val)
        unpacked = alertbase.encoding.varint_codec.unpack(packed)
        assert unpacked == val

    def test_float(self):
        with pytest.raises(TypeError):
            alertbase.encoding.varint_codec.pack(1.0)


class TestStrCodec:
    cases = ["", "simple", "12345", "Ω"]

    @pytest.mark.parametrize("val", cases)
    def test_roundtrip(self, val):
        packed = alertbase.encoding.str_codec.pack(val)
        unpacked = alertbase.encoding.str_codec.unpack(packed)
        assert unpacked == val
