"""One locked dependency identity and frozen export for host and runtime images."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tomllib
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[2]
CODEX_BINARY_SHA256 = "50b06603bdcdac39b714f5c3e68583c002b8ad8779ebfdaaf4932ff016b379c0"
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


def dependency_labels(identity: dict[str, str]) -> dict[str, str]:
    return {
        "devflow.runtime_lock_sha256": identity["lock_sha256"],
        "devflow.kit_version": identity["kit_version"],
        "devflow.kit_package_sha256": identity["kit_package_sha256"],
        "devflow.codex_sdk_version": identity["codex_sdk_version"],
        "devflow.codex_cli_version": identity["codex_cli_version"],
    }


def dependency_build_args(identity: dict[str, str]) -> dict[str, str]:
    return {
        "RUNTIME_LOCK_SHA256": identity["lock_sha256"],
        "KIT_VERSION": identity["kit_version"],
        "KIT_PACKAGE_SHA256": identity["kit_package_sha256"],
        "CODEX_SDK_VERSION": identity["codex_sdk_version"],
        "CODEX_CLI_VERSION": identity["codex_cli_version"],
        "CODEX_BIN_SHA256": CODEX_BINARY_SHA256,
    }


def frozen_requirements(identity: dict[str, str], runtime: Path = RUNTIME) -> str:
    if locked_dependency_identity(runtime) != identity:
        raise ValueError("runtime dependency lock changed before image export")
    result = subprocess.run(
        [
            "uv", "export", "--directory", str(runtime), "--frozen", "--no-dev",
            "--no-emit-project", "--no-header", "--no-annotate", "--format", "requirements.txt",
        ],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if result.returncode or not result.stdout.strip():
        raise ValueError("could not export frozen runtime dependencies; install uv")
    if locked_dependency_identity(runtime) != identity:
        raise ValueError("runtime dependency lock changed during image export")
    return result.stdout


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--build-args", type=Path, required=True)
    args = parser.parse_args()
    identity = locked_dependency_identity()
    args.export.write_text(frozen_requirements(identity))
    args.build_args.write_text(
        "".join(f"{key}={value}\n" for key, value in dependency_build_args(identity).items())
    )
