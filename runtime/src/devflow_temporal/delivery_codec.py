"""Compress large Temporal payloads without changing their decoded contracts."""

from __future__ import annotations

import lzma
import zlib
from collections.abc import Sequence
from dataclasses import replace

from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, PayloadCodec

_ENCODING = b"binary/zlib"
_LARGE_ENCODING = b"binary/xz"
_COMPRESSION_THRESHOLD = 256 * 1024
_PAYLOAD_LIMIT = 2 * 1024 * 1024


class LargePayloadCodec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        encoded = []
        for payload in payloads:
            if len(payload.data) < _COMPRESSION_THRESHOLD:
                encoded.append(payload)
                continue
            compressed = zlib.compress(payload.SerializeToString())
            encoding = _ENCODING
            if len(compressed) >= _PAYLOAD_LIMIT:
                compressed = lzma.compress(payload.SerializeToString())
                encoding = _LARGE_ENCODING
            encoded.append(
                Payload(metadata={"encoding": encoding}, data=compressed)
                if len(compressed) < len(payload.data) else payload
            )
        return encoded

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        decoded = []
        for payload in payloads:
            encoding = payload.metadata.get("encoding")
            if encoding == _ENCODING:
                payload = Payload.FromString(zlib.decompress(payload.data))
            elif encoding == _LARGE_ENCODING:
                payload = Payload.FromString(lzma.decompress(payload.data))
            decoded.append(payload)
        return decoded


DELIVERY_DATA_CONVERTER = replace(
    DataConverter.default, payload_codec=LargePayloadCodec()
)
