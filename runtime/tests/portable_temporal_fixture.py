"""Build disposable, locked synthetic inputs from verified official Temporal assets.

This fixture is not a historical acquisition receipt. It records the bytes it
actually downloads, binds them to this test project, and leaves all production
archive/member/custody verification in place.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
from functools import lru_cache
from pathlib import Path

from devflow_temporal.delivery_temporal_prerequisites import TOOLS

PROJECT = Path(__file__).with_name("fixtures") / "temporal_project"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(root):
    root = Path(root).resolve()
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise RuntimeError("The production Temporal prerequisite supports macOS arm64 only")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    source = root / "immutable-source/workers/automation"
    shutil.copytree(PROJECT, source)
    subprocess.run(["uv", "sync", "--frozen", "--project", str(source)], check=True)
    evidence = []
    records = {}
    for name, pin in TOOLS.items():
        url = (f"https://github.com/{pin['repository']}/releases/download/"
               f"{pin['tag']}/{pin['archive']}")
        metadata = root / f"{name}-release.json"
        metadata_url = f"https://api.github.com/repos/{pin['repository']}/releases/tags/{pin['tag']}"
        subprocess.run(
            ["/usr/bin/curl", "--fail", "--silent", "--show-error", "--location",
             "--proto", "=https", "--proto-redir", "=https", "--retry", "2",
             "--connect-timeout", "15", "--max-time", "60", "--output", str(metadata),
             metadata_url],
            check=True,
        )
        release = json.loads(metadata.read_text())
        assets = [asset for asset in release["assets"] if asset["name"] == pin["archive"]]
        if (len(assets) != 1 or assets[0].get("digest") != "sha256:" + pin["archive_sha256"]
                or assets[0].get("browser_download_url") != url):
            raise ValueError(f"{name} official release metadata does not match the pinned asset")
        evidence.append({"path": str(metadata), "sha256": sha(metadata)})
        archive = root / pin["archive"]
        subprocess.run(
            ["/usr/bin/curl", "--fail", "--silent", "--show-error", "--location",
             "--proto", "=https", "--proto-redir", "=https", "--retry", "2",
             "--connect-timeout", "15", "--max-time", "180", "--output", str(archive), url],
            check=True,
        )
        if sha(archive) != pin["archive_sha256"]:
            raise ValueError(f"{name} official archive digest mismatch")
        binary = root / "tools" / name / pin["filename"]
        binary.parent.mkdir(parents=True)
        with tarfile.open(archive, "r:gz") as tar:
            members = [item for item in tar if item.name == pin["member"]]
            if len(members) != 1 or not members[0].isfile():
                raise ValueError(f"{name} official archive member missing")
            with tar.extractfile(members[0]) as data, binary.open("xb") as output:
                shutil.copyfileobj(data, output)
        binary.chmod(0o555)
        if sha(binary) != pin["binary_sha256"] or binary.stat().st_size != pin["binary_size"]:
            raise ValueError(f"{name} official binary digest or size mismatch")
        if archive.stat().st_size != assets[0]["size"]:
            raise ValueError(f"{name} archive size differs from official release metadata")
        records[name] = {
            "official_repository": pin["repository"], "pinned_tag": pin["tag"],
            "asset_name": pin["archive"], "asset_url": url,
            "published_digest": {"algorithm": "sha256", "value": pin["archive_sha256"]},
            "archive": {"path": str(archive), "sha256": sha(archive),
                        "size": archive.stat().st_size},
            "extraction": {"exact_member": pin["member"]},
            "binary": {"path": str(binary), "sha256": sha(binary),
                       "size": binary.stat().st_size, "platform": "darwin_arm64",
                       "mode": "0o555", "uid": os.getuid()},
        }
    receipt = {
        "schema": "jobctrl.temporal-test-prerequisites/v1", "schema_version": 1,
        "purpose": "Synthetic fixture; official pinned archives downloaded and hashed",
        "acquisition_facts": {"official_metadata": evidence},
        "compatibility_facts": {"locked_sdk_version": "1.26.0"},
        "source_facts": {"pyproject_sha256": sha(source / "pyproject.toml"),
                         "uv_lock_sha256": sha(source / "uv.lock")},
        "tools": records,
    }
    (root / "temporal-prerequisites.provenance.v1.json").write_text(
        json.dumps(receipt, indent=2) + "\n"
    )
    return root


@lru_cache(maxsize=1)
def fixture_root():
    if inherited := os.environ.get("DEVFLOW_PORTABLE_FIXTURE_ROOT"):
        return Path(inherited)
    if runner := os.environ.get("RUNNER_TEMP"):
        root = Path(runner) / "devflow-portable-temporal"
        if root.is_dir():
            return root
    return build(Path(tempfile.mkdtemp(prefix="devflow-portable-")) / "fixture")


if __name__ == "__main__":
    build(sys.argv[1])
