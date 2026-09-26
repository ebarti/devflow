"""Candidate browser/API/SQLite gate inside the shared durable Docker boundary."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import canonical_json, digest
from .delivery_browser_qa import _artifacts
from .delivery_container import Bind, OwnedContainer
from .delivery_output import observed_test_count, visible_output


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _write_once(path: Path, content: bytes) -> None:
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()
            or path.read_bytes() != content
        ):
            raise ValueError("browser QA receipt changed across retries")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def run_browser_qa_container(
    broker: Any, iteration: int, candidate: dict[str, Any]
) -> dict[str, Any]:
    qa = broker.spec["policy"].get("browser_qa")
    if not isinstance(qa, dict):
        raise ValueError("browser QA is not configured")
    if broker.candidate() != candidate:
        raise ValueError("browser QA candidate changed before the gate")
    checkout = broker.gate_checkout("verify", iteration, candidate).resolve(strict=True)
    cwd = (checkout / qa.get("cwd", ".")).resolve(strict=True)
    if checkout not in (cwd, *cwd.parents):
        raise ValueError("browser QA cwd escaped the gate checkout")
    ports = tuple(qa["ports"].values())
    volume = broker._ensure_dependency_store()
    git_metadata = broker.git_metadata(candidate)
    key = f"browser_qa:{broker.spec['run_id']}:{iteration}"
    request = {
        "iteration": iteration,
        "candidate_id": candidate["id"],
        "policy_digest": broker.spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "argv": qa["argv"],
        "ports": qa["ports"],
        "image_id": broker.spec["policy"]["container"]["image_id"],
    }
    folder = broker.state_dir / "browser-qa" / str(iteration)
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = folder.lstat()
    if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.getuid():
        raise ValueError("browser QA evidence directory is not private")
    # Any prelaunch dependency/configuration failure above has no browser effect.
    prior = broker._effect(key, "browser_qa", request)
    env = {
        "HOME": "/tmp",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "COREPACK_HOME": "/usr/local/share/corepack",
        "COREPACK_ENABLE_NETWORK": "0",
        "PNPM_STORE_DIR": "/store",
        "npm_config_nodedir": "/usr",
        "CI": "1",
        "JOBCTRL_E2E_ISOLATED": "1",
        "PLAYWRIGHT_BROWSERS_PATH": "/ms-playwright",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.hooksPath",
        "GIT_CONFIG_VALUE_0": "/dev/null",
        "GIT_DIR": "/gitmeta",
        "GIT_WORK_TREE": "/work",
        "GIT_OPTIONAL_LOCKS": "0",
        **{key: str(value) for key, value in qa["ports"].items()},
    }
    command = (
        "/usr/bin/python3",
        "/opt/devflow/landlock_exec.py",
        *(item for port in ports for item in ("--allow-port", str(port))),
        "--",
        *qa["argv"],
    )
    contained = OwnedContainer(
        broker.spec,
        kind="browser-qa",
        identity={"iteration": iteration, "candidate_id": candidate["id"]},
        evidence_dir=folder / "container",
        binds=(
            Bind(checkout, "/work"),
            Bind(checkout / ".git", "/work/.git", True),
            Bind(git_metadata, "/gitmeta", True),
        ),
        command=command,
        cwd="/work" if cwd == checkout else "/work/" + cwd.relative_to(checkout).as_posix(),
        environment=env,
        network="none",
        timeout_seconds=int(qa["timeout_seconds"]),
        volume_mounts=((volume, "/store", True),),
    ).run()
    log = contained.log
    output = visible_output(log.read_text(encoding="utf-8", errors="replace"))
    count = observed_test_count(output, qa["test_count_regex"])
    rejected = bool(qa.get("reject_regex") and re.search(qa["reject_regex"], output))
    rejected = rejected or bool(
        re.search(r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b", output)
    )
    artifacts = _artifacts(checkout, qa.get("artifact_paths", []))
    unchanged = candidate_for(checkout)["id"] == candidate["id"]
    result = {
        "state": "passed"
        if contained.exit_code == 0
        and contained.cleanup == "confirmed"
        and count >= qa["min_tests"]
        and not rejected
        and unchanged
        else "failed",
        "candidate_id": candidate["id"],
        "iteration": iteration,
        "qa_config_sha256": digest(qa),
        "argv": qa["argv"],
        "cwd": str(cwd),
        "ports": qa["ports"],
        "network": "private-none",
        "port_policy": "landlock-exact",
        "container_id": contained.container_id,
        "image_id": contained.image_id,
        "seccomp_sha256": contained.seccomp_sha256,
        "exit_code": contained.exit_code,
        "test_count": count,
        "rejected_output": rejected,
        "log": str(log),
        "log_sha256": contained.log_sha256,
        "artifacts": artifacts,
        "source_unchanged": unchanged,
        "cleanup": contained.cleanup,
    }
    receipt = folder / "receipt.json"
    _write_once(receipt, (canonical_json(result) + "\n").encode())
    result["receipt"] = str(receipt)
    result["receipt_sha256"] = _hash(receipt)
    if prior and prior != result:
        raise ValueError("completed browser QA effect differs from its container receipt")
    broker._finish_effect(key, result)
    return result
