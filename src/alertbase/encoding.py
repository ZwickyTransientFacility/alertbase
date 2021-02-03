from typing import Tuple, Iterator, Generic, TypeVar, Callable
import struct

from astropy.time import Time


# This file provides a variety of utilities for converting between python data
# types and byte arrays.
T = TypeVar("T")
Pack = Callable[[T], bytes]
Unpack = Callable[[bytes], T]


class Codec(Generic[T]):
    def __init__(
        self,
        name: str,
        pack: Pack[T],
        unpack: Unpack[T],
    ):
        self.name = name
        self._pack = pack
        self._unpack = unpack

    def __str__(self) -> str:
        return f"<Codec: {self.name}>"

    def pack(self, val: T) -> bytes:
        return self._pack(val)

    def unpack(self, raw: bytes) -> T:
        return self._unpack(raw)


# uint64 codec:
def pack_uint64(data: int) -> bytes:
    """Pack an integer as a fixed-size 64-bit unsigned integer. This is more
    efficient (both in space and compute) than pack_varint for large integers."""
    return struct.pack(">Q", data)


def unpack_uint64(data: bytes) -> int:
    val: int = struct.unpack(">Q", data)[0]
    return val


uint64_codec = Codec("uint64", pack_uint64, unpack_uint64)


# time codec:
def pack_time(t: Time) -> bytes:
    return pack_uint64(int(t.unix * 1e9))


def unpack_time(data: bytes) -> Time:
    i = unpack_uint64(data)
    t = Time(i / 1e9, format="unix")
    return t


time_codec = Codec("time", pack_time, unpack_time)


# varint codec:
def pack_varint(n: int) -> bytes:
    """Pack a zig-zag encoded, signed integer into bytes."""
    return _pack_uvarint(_zigzag_encode(n))


def _unpack_varint_with_readlength(data: bytes) -> Tuple[int, int]:
    """Unpacks a variable-length, zig-zag-encoded integer from a given byte buffer.

    Returns the integer and the number of bytes that were read.
    """
    result, n = _unpack_uvarint(data)
    return _zigzag_decode(result), n


def unpack_varint(data: bytes) -> int:
    """Unpacks a variable-length, zig-zag-encoded integer from a given byte buffer."""
    return _unpack_varint_with_readlength(data)[0]


varint_codec = Codec("varint", pack_varint, unpack_varint)


def _pack_uvarint(n: int) -> bytes:
    """Pack an unsigned variable-length integer into bytes. """
    result = b""
    while True:
        chunk = n & 0x7F
        n >>= 7
        if n:
            result += bytes((chunk | 0x80,))
        else:
            result += bytes((chunk,))
            break
    return result


def _unpack_uvarint(data: bytes) -> Tuple[int, int]:
    """Unpacks a variable-length integer stored in given byte buffer.

    Returns the integer and the number of bytes that were read."""
    shift = 0
    result = 0
    n = 0
    for b in data:
        n += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, n


def _zigzag_encode(x: int) -> int:
    if x >= 0:
        return x << 1
    return (x << 1) ^ (~0)


def _zigzag_decode(x: int) -> int:
    if not x & 0x1:
        return x >> 1
    return (x >> 1) ^ (~0)


# varint iterator codec
def pack_varint_iter(ints: Iterator[int]) -> bytes:
    result = b""
    for val in ints:
        result += pack_varint(val)
    return result


def unpack_varint_iter(data: bytes) -> Iterator[int]:
    """Calls unpack_varint repeatedly on data, iterating over the integers encoded
    therein.

    """
    pos = 0
    while pos < len(data):
        val, n_read = _unpack_varint_with_readlength(data[pos:])
        pos += n_read
        yield val


varint_iterator_codec = Codec("varint_iterator", pack_varint_iter, unpack_varint_iter)


# str codec
def pack_str(val: str) -> bytes:
    return val.encode("utf-8")


def unpack_str(val: bytes) -> str:
    return val.decode("utf-8", "strict")


str_codec = Codec("str", pack_str, unpack_str)
