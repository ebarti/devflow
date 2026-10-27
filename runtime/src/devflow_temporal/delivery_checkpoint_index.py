"""A trustworthy baseline for supported Git index semantics."""

from __future__ import annotations

import hashlib
import re

_DETAIL = re.compile(
    rb"  ctime: [0-9]+:[0-9]+\n  mtime: [0-9]+:[0-9]+\n"
    rb"  dev: [0-9]+\tino: [0-9]+\n  uid: [0-9]+\tgid: [0-9]+\n"
    rb"  size: [0-9]+\tflags: ([0-9a-fA-F]+)\n")


def observe_index(checkout, query):
    stages = query(checkout, "ls-files", "--stage", "-z")
    debug = query(checkout, "ls-files", "--debug", "-z")
    entries = []
    while debug:
        path, separator, detail = debug.partition(b"\0")
        match = _DETAIL.match(detail)
        if not separator or not path or match is None:
            raise ValueError("checkpoint Git index semantics are unsupported")
        if int(match[1], 16) != 0:
            raise ValueError("checkpoint Git index has unsupported semantic flags")
        entries.append(path)
        debug = detail[match.end():]
    return {"schema": "devflow-index-semantics-v1",
            "stage_sha256": hashlib.sha256(stages).hexdigest(),
            "normal_paths_sha256": hashlib.sha256(b"\0".join(entries)).hexdigest()}
