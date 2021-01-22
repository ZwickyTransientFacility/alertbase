from typing import List, Tuple, Iterator
import struct


# This file provides a variety of utilities for converting between python data
# types and byte arrays.


def pack_uint64(data: int) -> bytes:
    """Pack an integer as a fixed-size 64-bit unsigned integer. This is more
    efficient (both in space and compute) than pack_varint for large integers."""
    return struct.pack(">Q", data)


def pack_uint64s(data: List[int]) -> bytes:
    """Pack a sequence of integers using pack_uint64. """
    result = b""
    for i in data:
        result += pack_uint64(i)
    return result


def unpack_uint64s(data: bytes) -> List[int]:
    """ Unpack a series of integers that were packed with pack_uint64."""
    if len(data) == 0:
        return []
    return list(x[0] for x in struct.iter_unpack(">Q", data))


def pack_jd_timestamp(jd: float) -> bytes:
    """Pack a julian date as a unix nanosecond timestamp (doesn't attempt to handle
    leap seconds)
    """
    return pack_uint64(int((jd - 2440587.5) * 86400000000000))


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


def pack_varint(n):
    """Pack a zig-zag encoded, signed integer into bytes."""
    return _pack_uvarint(_zigzag_encode(n))


def pack_varint_list(data: List[int]) -> bytes:
    """Pack a series of integers into bytes using pack_varint. """
    result = b""
    for value in data:
        result += pack_varint(value)
    return result


def _zigzag_encode(x):
    if x >= 0:
        return x << 1
    return (x << 1) ^ (~0)


def _zigzag_decode(x):
    if not x & 0x1:
        return x >> 1
    return (x >> 1) ^ (~0)


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


def unpack_varint(data: bytes) -> Tuple[int, int]:
    """Unpacks a variable-length, zig-zag-encoded integer from a given byte buffer.

    Returns the integer and the number of bytes that were read.
    """
    result, n = _unpack_uvarint(data)
    return _zigzag_decode(result), n


def unpack_varint_list(data: bytes) -> List[int]:
    """Calls unpack_varint repeatedly on data, returning the complete list of all
    integers encoded therein.

    """
    result = []
    pos = 0
    while pos < len(data):
        val, n_read = unpack_varint(data[pos:])
        pos += n_read
        result.append(val)
    return result


def iter_varints(data: bytes) -> Iterator[int]:
    """Calls unpack_varint repeatedly on data, iterating over the integers encoded
    therein.
    """
    pos = 0
    while pos < len(data):
        val, n_read = unpack_varint(data[pos:])
        pos += n_read
        yield val
