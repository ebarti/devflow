"""Actual public-index frozen host runtime dependency identity."""
from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[2]
PACKAGES = ("agent-runtime-kit", "openai-codex", "openai-codex-cli-bin")

def locked_dependency_identity(runtime: Path = RUNTIME) -> dict[str, str]:
    content = (runtime / "uv.lock").read_bytes()
    lock = tomllib.loads(content.decode())
    selected = {}
    for name in PACKAGES:
        matches = [package for package in lock["package"] if package["name"] == name]
        if len(matches) != 1 or matches[0]["source"] != {"registry": "https://pypi.org/simple"}:
            raise ValueError("runtime dependencies require one public-index locked package")
        selected[name] = matches[0]
    if selected["openai-codex"]["version"] != selected["openai-codex-cli-bin"]["version"]:
        raise ValueError("locked Codex SDK and CLI versions differ")
    kit = json.dumps(selected["agent-runtime-kit"], sort_keys=True, separators=(",", ":"))
    return {
        "lock_sha256": hashlib.sha256(content).hexdigest(),
        "kit_version": selected["agent-runtime-kit"]["version"],
        "kit_package_sha256": hashlib.sha256(kit.encode()).hexdigest(),
        "codex_sdk_version": selected["openai-codex"]["version"],
        "codex_cli_version": selected["openai-codex-cli-bin"]["version"],
    }
