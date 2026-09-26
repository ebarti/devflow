"""Outer macOS role sandbox; kit permission declarations alone are not a host boundary."""

from __future__ import annotations

import json
import os
import shutil
import stat
from pathlib import Path
from typing import Any


def _path(value: Path) -> str:
    return json.dumps(str(value.resolve()))


def _private(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.getuid()
    ):
        raise ValueError("role sandbox directory is not private and owned")


def prepare_sandbox(request: dict[str, Any], attempt_dir: Path) -> tuple[Path, dict[str, str]]:
    if not Path("/usr/bin/sandbox-exec").is_file():
        raise ValueError("required macOS sandbox-exec is unavailable")
    spec = request["spec"]
    role = request["role"]
    if spec.get("provider") == "codex" and spec["policy"].get("host_sandbox") != "seatbelt":
        raise ValueError("role policy did not require the host sandbox")
    state_root = Path(spec["state_dir"]).parent.parent.resolve()
    workspace = Path(request["workspace"]).resolve(strict=True)
    role_home = Path(spec["state_dir"]) / "role-homes" / role
    codex_home = role_home / "codex"
    scratch = role_home / "tmp"
    for path in (role_home, codex_home, scratch, attempt_dir):
        _private(path)
    auth_source = Path(
        spec["policy"].get("codex_auth_path") or Path.home() / ".codex" / "auth.json"
    )
    if spec.get("provider") == "codex":
        source_info = auth_source.lstat()
        if (
            not stat.S_ISREG(source_info.st_mode)
            or source_info.st_uid != os.getuid()
            or stat.S_IMODE(source_info.st_mode) & 0o077
        ):
            raise ValueError("configured provider credential is not an owned private file")
        target = codex_home / "auth.json"
        if not target.exists():
            descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as stream, auth_source.open("rb") as source:
                shutil.copyfileobj(source, stream)
    protected = [
        state_root,
        Path(spec["config_path"]),
        Path(spec["policy"].get("tracking_db", "")) if spec["policy"].get("tracking_db") else None,
        auth_source.parent,
        Path.home() / ".config" / "gh",
        Path.home() / ".ssh",
        Path.home() / ".aws",
        Path.home() / ".npmrc",
    ]
    lines = ["(version 1)", "(allow default)", "(deny file-write*)"]
    for path in protected:
        if path is None:
            continue
        operation = "literal" if path.is_file() else "subpath"
        lines.append(f"(deny file-read* ({operation} {_path(path)}))")
    # realpath() must traverse ancestors of the explicitly allowed role home.
    # Metadata alone does not grant access to controller file contents.
    lines.append(f"(allow file-read-metadata (subpath {_path(state_root)}))")
    readable = [workspace, attempt_dir, role_home, Path(__file__).resolve().parents[2]]
    if role == "implement":
        recovery = Path(spec["state_dir"]) / "recovery"
        if recovery.is_dir():
            readable.append(recovery)
    for path in readable:
        lines.append(f"(allow file-read* (subpath {_path(path)}))")
    writable = [attempt_dir, role_home]
    if role != "review":
        writable.append(workspace)
    for path in writable:
        lines.append(f"(allow file-write* (subpath {_path(path)}))")
    # sandbox-exec itself and common child tools may need /dev/null.
    lines.append('(allow file-write* (literal "/dev/null"))')
    profile = attempt_dir / "seatbelt.sb"
    profile.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(profile, 0o600)
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        in {"PATH", "LANG", "LC_ALL", "USER", "LOGNAME", "SHELL", "SSL_CERT_FILE", "SSL_CERT_DIR"}
    }
    env.update(
        {
            "HOME": str(role_home),
            "CODEX_HOME": str(codex_home),
            "TMPDIR": str(scratch),
            "XDG_CACHE_HOME": str(role_home / ".cache"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "credential.helper",
            "GIT_CONFIG_VALUE_0": "",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/usr/bin/false",
            "GCM_INTERACTIVE": "never",
        }
    )
    return profile, env
