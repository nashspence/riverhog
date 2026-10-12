"""Diagnostic SHA-256 continuation; not an approved production crypto backend.

The state is private, untrusted on import without owner-side binding/integrity.
The algorithm is SHA-256 (RFC 6234), not a replacement archive commitment.
"""
from __future__ import annotations

import struct

_INITIAL = (0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
            0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19)
_K = tuple(int(x, 16) for x in """
428a2f98 71374491 b5c0fbcf e9b5dba5 3956c25b 59f111f1 923f82a4 ab1c5ed5
 d807aa98 12835b01 243185be 550c7dc3 72be5d74 80deb1fe 9bdc06a7 c19bf174
 e49b69c1 efbe4786 0fc19dc6 240ca1cc 2de92c6f 4a7484aa 5cb0a9dc 76f988da
 983e5152 a831c66d b00327c8 bf597fc7 c6e00bf3 d5a79147 06ca6351 14292967
 27b70a85 2e1b2138 4d2c6dfc 53380d13 650a7354 766a0abb 81c2c92e 92722c85
 a2bfe8a1 a81a664b c24b8b70 c76c51a3 d192e819 d6990624 f40e3585 106aa070
 19a4c116 1e376c08 2748774c 34b0bcb5 391c0cb3 4ed8aa4a 5b9cca4f 682e6ff3
 748f82ee 78a5636f 84c87814 8cc70208 90befffa a4506ceb bef9a3f7 c67178f2
""".split())
_MASK = (1 << 32) - 1


def _r(x: int, n: int) -> int:
    return ((x >> n) | (x << (32 - n))) & _MASK


class Sha256:
    def __init__(self) -> None:
        self.words = _INITIAL
        self.count = 0
        self.tail = b""

    def _block(self, block: bytes) -> None:
        w = list(struct.unpack(">16I", block))
        for i in range(16, 64):
            x, y = w[i - 15], w[i - 2]
            w.append((w[i - 16] + (_r(x, 7) ^ _r(x, 18) ^ (x >> 3))
                      + w[i - 7] + (_r(y, 17) ^ _r(y, 19) ^ (y >> 10))) & _MASK)
        a, b, c, d, e, f, g, h = self.words
        for k, v in zip(_K, w, strict=True):
            t1 = (h + (_r(e, 6) ^ _r(e, 11) ^ _r(e, 25))
                  + ((e & f) ^ (~e & g)) + k + v) & _MASK
            t2 = ((_r(a, 2) ^ _r(a, 13) ^ _r(a, 22))
                  + ((a & b) ^ (a & c) ^ (b & c))) & _MASK
            a, b, c, d, e, f, g, h = ((t1 + t2) & _MASK, a, b, c,
                                      (d + t1) & _MASK, e, f, g)
        self.words = tuple((x + y) & _MASK for x, y in
                           zip(self.words, (a,b,c,d,e,f,g,h), strict=True))

    def update(self, data: bytes) -> None:
        if type(data) is not bytes:
            raise TypeError("bytes required")
        if (self.count + len(data)) * 8 >= 1 << 64:
            raise ValueError("outside SHA-256 specified message domain")
        self.count += len(data)
        raw = self.tail + data
        end = len(raw) - len(raw) % 64
        for offset in range(0, end, 64):
            self._block(raw[offset:offset + 64])
        self.tail = raw[end:]

    def export(self) -> bytes:
        return b"S256\x01" + struct.pack(">Q8I", self.count, *self.words) + self.tail

    @classmethod
    def restore(cls, raw: bytes) -> Sha256:
        if not 45 <= len(raw) <= 108 or raw[:5] != b"S256\x01":
            raise ValueError("invalid private SHA state")
        count, *words = struct.unpack(">Q8I", raw[5:45])
        if count % 64 != len(raw) - 45 or count * 8 >= 1 << 64:
            raise ValueError("invalid SHA byte position")
        if count < 64 and tuple(words) != _INITIAL:
            raise ValueError("invalid initial state")
        obj = cls(); obj.count = count; obj.words = tuple(words); obj.tail = raw[45:]
        return obj

    def digest(self) -> bytes:
        obj = self.restore(self.export())
        padding = b"\x80" + b"\0" * ((55 - self.count) % 64)
        raw = obj.tail + padding + struct.pack(">Q", self.count * 8)
        for offset in range(0, len(raw), 64):
            obj._block(raw[offset:offset + 64])
        return struct.pack(">8I", *obj.words)
