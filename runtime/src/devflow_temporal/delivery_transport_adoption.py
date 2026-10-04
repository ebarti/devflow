"""Explicit installation receipt for a transport-only update of a queued run.

The original native proof remains historical. Every other runtime source must
remain byte-identical, and native identity fields other than the payload hash
are still checked by the caller. This grants no preparation or coding turn.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import tarfile
from functools import lru_cache
from pathlib import Path

from .contracts import digest
from .delivery_broker import _git
from .delivery_resources import read_private
from .payload import payload_digest

_PACKAGE = "runtime/src/devflow_temporal/"
_TRANSPORT_FILES = {
    _PACKAGE + name for name in (
        "delivery_codec.py", "delivery_transport_adoption.py",
        "delivery_native_preparation.py", "delivery_investigation_adjudication.py",
    )
}


@lru_cache(maxsize=8)
def _historical_payload(source: str, revision: str) -> str:
    archive = subprocess.run(
        ["git", "archive", revision, "runtime/src/devflow_temporal"],
        cwd=source, check=True, capture_output=True,
    ).stdout
    manifest = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        for member in files.getmembers():
            if member.name.endswith(".py"):
                if not member.isfile():
                    raise ValueError("transport baseline contains a linked source")
                stream = files.extractfile(member)
                manifest[member.name.removeprefix("runtime/src/")] = hashlib.sha256(
                    stream.read()
                ).hexdigest()
    if not manifest:
        raise ValueError("transport baseline has no runtime source")
    return digest(manifest)


def transport_adoption(spec: dict, historical_payload: str,
                       historical_revision: str | None = None) -> dict:
    """Fail closed unless a private receipt binds the exact reviewed installation."""
    value = read_private(Path(spec["state_dir"]) / "transport-adoption.json")
    source = Path(__file__).resolve().parents[3]
    before, after = value["before"], value["after"]
    changed = set(_git(source, "diff", "--name-only", before["revision"],
                       after["revision"], "--", "runtime/src").splitlines())
    if (
        value.get("schema") != "devflow-queued-transport-adoption-v1"
        or value.get("owner") != "root"
        or value.get("run_id") != spec["run_id"]
        or value.get("source_root") != str(source)
        or before["payload_sha256"] != historical_payload
        or (historical_revision is not None and before["revision"] != historical_revision)
        or _historical_payload(str(source), before["revision"]) != historical_payload
        or after["revision"] != _git(source, "rev-parse", "HEAD")
        or after["tree"] != _git(source, "rev-parse", "HEAD^{tree}")
        or after["payload_sha256"] != payload_digest(source / _PACKAGE)
        or after.get("source_review") != "PASS"
        or after.get("required_ci") != "SUCCESS"
        or _git(source, "rev-parse", after["published_head"] + "^{tree}") != after["tree"]
        or _git(source, "status", "--porcelain", "--untracked-files=all")
        or not changed
        or not changed <= _TRANSPORT_FILES
        or _git(source, "merge-base", before["revision"], after["revision"])
        != before["revision"]
    ):
        raise ValueError("queued transport installation receipt or native source changed")
    return value


def controller_installation(spec: dict, original: dict) -> tuple[dict, bytes]:
    """Return current source authority while retaining the original process receipt."""
    receipt = transport_adoption(spec, original["runtime_payload_sha256"],
                                 original["source_revision"])
    retained = receipt["historical_service_manifest"]
    path = Path(retained["path"])
    # read_private authenticates ownership/mode/type before hashing exact bytes.
    read_private(path)
    raw = path.read_bytes()
    if (retained["sha256"] != original["service_manifest_sha256"]
            or hashlib.sha256(raw).hexdigest() != retained["sha256"]):
        raise ValueError("historical controller service receipt changed")
    current = {
        **original,
        "source_revision": receipt["after"]["revision"],
        "source_tree": receipt["after"]["tree"],
        "runtime_payload_sha256": receipt["after"]["payload_sha256"],
        "published_tree": receipt["after"]["tree"],
        "published_head": receipt["after"]["published_head"],
        "source_review": receipt["after"]["source_review"],
        "required_ci": receipt["after"]["required_ci"],
    }
    return current, raw
