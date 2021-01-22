import alertbase.encoding
import pytest


class TestVarintPacking:
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
        have = alertbase.encoding.pack_varint_list(values)
        assert have == encoded

    @pytest.mark.parametrize("values,encoded", cases)
    def test_unpacking(self, values, encoded):
        have = alertbase.encoding.unpack_varint_list(encoded)
        assert have == values


class TestUint64Packing:
    cases = [
        (
            [],
            bytearray.fromhex(""),
        ),
        (
            [0],
            bytearray.fromhex("0000 0000 0000 0000"),
        ),
        (
            [1, 2, 3, 4],
            bytearray.fromhex(
                "0000 0000 0000 0001"
                + "0000 0000 0000 0002"
                + "0000 0000 0000 0003"
                + "0000 0000 0000 0004"
            ),
        ),
        (
            [1 << 32, 1 << 60, (1 << 64) - 1],
            bytearray.fromhex(
                "0000 0001 0000 0000" + "1000 0000 0000 0000" + "ffff ffff ffff ffff",
            ),
        ),
    ]

    @pytest.mark.parametrize("values,encoded", cases)
    def test_unpack(self, values, encoded):
        have = alertbase.encoding.unpack_uint64s(encoded)
        assert have == values

    @pytest.mark.parametrize("values,encoded", cases)
    def test_pack(self, values, encoded):
        have = alertbase.encoding.pack_uint64s(values)
        assert have == encoded

    def test_pack_overflow(self):
        with pytest.raises(Exception):
            alertbase.encoding.pack_uint64(1 << 64)

    def test_unpack_incomplete_input(self):
        with pytest.raises(Exception):
            alertbase.encoding.unpack_uint64s("1")
