"""Compress large Temporal payloads without changing their decoded contracts."""

from __future__ import annotations

import zlib
from collections.abc import Sequence
from dataclasses import replace

from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, PayloadCodec

_ENCODING = b"binary/zlib"
_COMPRESSION_THRESHOLD = 256 * 1024


class LargePayloadCodec(PayloadCodec):
    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        encoded = []
        for payload in payloads:
            if len(payload.data) < _COMPRESSION_THRESHOLD:
                encoded.append(payload)
                continue
            compressed = zlib.compress(payload.SerializeToString())
            encoded.append(
                Payload(metadata={"encoding": _ENCODING}, data=compressed)
                if len(compressed) < len(payload.data) else payload
            )
        return encoded

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload.FromString(zlib.decompress(payload.data))
            if payload.metadata.get("encoding") == _ENCODING else payload
            for payload in payloads
        ]


DELIVERY_DATA_CONVERTER = replace(
    DataConverter.default, payload_codec=LargePayloadCodec()
)
