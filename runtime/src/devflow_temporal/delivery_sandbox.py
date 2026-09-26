"""Native Codex permission profiles for real roles and checks; Seatbelt for fake tests."""

from __future__ import annotations

import ipaddress
import json
import os
import re
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
    if request["spec"].get("provider") != "fake":
        raise ValueError("legacy Seatbelt launcher is only for the fake provider")
    if not Path("/usr/bin/sandbox-exec").is_file():
        raise ValueError("required macOS sandbox-exec is unavailable")
    spec = request["spec"]
    role = request["role"]
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


def _private_file(path: Path) -> None:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_uid != os.getuid()
    ):
        raise ValueError("role configuration is not a private owned regular file")


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        _private_file(path)
        if path.read_bytes() != content:
            raise ValueError("role configuration changed across attempts")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)


def _native_env(
    home: Path, codex_home: Path, scratch: Path, toolchain_roots: tuple[Path, ...] = ()
) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key in {"LANG", "LC_ALL", "USER", "LOGNAME", "SSL_CERT_FILE", "SSL_CERT_DIR"}
    }
    env.update(
        {
            "PATH": ":".join(
                [
                    *(str(root / "bin") for root in toolchain_roots),
                    "/opt/homebrew/bin",
                    "/usr/local/bin",
                    "/usr/bin",
                    "/bin",
                    "/usr/sbin",
                    "/sbin",
                ]
            ),
            "HOME": str(home),
            "CODEX_HOME": str(codex_home),
            "TMPDIR": str(scratch),
            "XDG_CACHE_HOME": str(home / ".cache"),
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
    return env


def _profile_lines(
    name: str,
    *,
    workspace: Path,
    workspace_access: str,
    home: Path,
    codex_home: Path,
    scratch: Path,
    extra_read: tuple[Path, ...] = (),
    network_domains: tuple[str, ...] = (),
) -> list[str]:
    if workspace_access not in {"read", "write"}:
        raise ValueError("unsupported workspace access")
    lines = [
        f'default_permissions = "{name}"',
        'web_search = "disabled"',
        "[features]",
        "plugins = false",
        "network_proxy = true",
        f"[permissions.{name}.filesystem]",
        '":root" = "deny"',
        '":minimal" = "read"',
        f'{_path(Path("/opt/homebrew"))} = "read"',
        f'{_path(Path("/usr/local"))} = "read"',
        f'{_path(Path("/System/Library/OpenSSL"))} = "read"',
        f'{_path(workspace)} = "{workspace_access}"',
        f'{_path(workspace / ".git")} = "deny"',
        f'{_path(workspace / ".codex")} = "deny"',
        f'{_path(home)} = "write"',
        f'{_path(scratch)} = "write"',
        f'{_path(codex_home)} = "deny"',
    ]
    for path in extra_read:
        lines.append(f'{_path(path)} = "read"')
    lines.extend(
        (f"[permissions.{name}.network]", f"enabled = {str(bool(network_domains)).lower()}")
    )
    if network_domains:
        lines.append(f"[permissions.{name}.network.domains]")
        for domain in network_domains:
            lines.append(f'{json.dumps(validate_network_domain(domain))} = "allow"')
    # Codex otherwise appends this trust record after the first model turn,
    # changing the frozen profile file before a same-session repair. The
    # controller admits only an owned checkout with no project Codex config.
    lines.extend((f"[projects.{_path(workspace)}]", 'trust_level = "trusted"'))
    return lines


def validate_network_domain(domain: str) -> str:
    normalized = domain.lower()
    try:
        ipaddress.ip_address(normalized)
    except ValueError:
        pass
    else:
        raise ValueError("check network domain must not be an IP address")
    if (
        normalized == "localhost"
        or normalized.endswith(".localhost")
        or not re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)+", normalized)
        or any(label.startswith("-") or label.endswith("-") for label in normalized.split("."))
    ):
        raise ValueError("check network domain is not an exact public host")
    return normalized


def prepare_native_role(request: dict[str, Any], attempt_dir: Path) -> tuple[str, dict[str, str]]:
    """Keep provider auth in the trusted CLI, while its commands use a native profile."""

    spec = request["spec"]
    if spec.get("provider") != "codex" or spec["policy"].get("host_sandbox") != "native-profile":
        raise ValueError("real role did not require the native profile boundary")
    workspace = Path(request["workspace"]).resolve(strict=True)
    if (workspace / ".codex").exists() or (workspace / ".codex").is_symlink():
        raise ValueError("project Codex configuration is not admitted")
    role_home = Path(spec["state_dir"]) / "role-homes" / request["role"]
    if request["role"] != "implement":
        role_home /= str(request["iteration"])
    codex_home = role_home / "codex"
    scratch = role_home / "tmp"
    for path in (role_home, codex_home, scratch, attempt_dir):
        _private(path)
    source = Path(spec["policy"].get("codex_auth_path") or Path.home() / ".codex" / "auth.json")
    _private_file(source)
    _write_once(codex_home / "auth.json", source.read_bytes())
    recovery = Path(spec["state_dir"]) / "recovery"
    toolchain_roots = tuple(Path(root) for root in spec["policy"].get("toolchain_roots", []))
    cache = spec["policy"].get("package_manager_cache")
    extra_read = (
        toolchain_roots
        + ((Path(cache),) if cache else ())
        + ((recovery,) if request["role"] == "implement" and recovery.is_dir() else ())
    )
    profile_name = "devflow-role"
    lines = _profile_lines(
        profile_name,
        workspace=workspace,
        workspace_access="read" if request["role"] == "review" else "write",
        home=role_home,
        codex_home=codex_home,
        scratch=scratch,
        extra_read=extra_read,
    )
    _write_once(codex_home / "config.toml", ("\n".join(lines) + "\n").encode())
    env = _native_env(role_home, codex_home, scratch, toolchain_roots)
    if cache:
        env["COREPACK_HOME"] = cache
    return profile_name, env


def prepare_native_check(
    spec: dict[str, Any], checkout: Path, evidence_dir: Path, check: dict[str, Any]
) -> tuple[str, dict[str, str]]:
    """Run candidate-controlled check commands in a separate credential-free profile."""

    if spec.get("provider") != "codex" or spec["policy"].get("host_sandbox") != "native-profile":
        raise ValueError("real checks require the native profile boundary")

    home = evidence_dir / check["id"] / "home"
    codex_home = home / "codex"
    scratch = home / "tmp"
    for path in (home, codex_home, scratch):
        _private(path)
    domains = tuple(check.get("network_domains", ()))
    toolchain_roots = tuple(Path(root) for root in spec["policy"].get("toolchain_roots", []))
    cache = spec["policy"].get("package_manager_cache")
    profile_name = "devflow-check"
    lines = _profile_lines(
        profile_name,
        workspace=checkout,
        workspace_access="write",
        home=home,
        codex_home=codex_home,
        scratch=scratch,
        extra_read=toolchain_roots + ((Path(cache),) if cache else ()),
        network_domains=domains,
    )
    _write_once(codex_home / "config.toml", ("\n".join(lines) + "\n").encode())
    env = _native_env(home, codex_home, scratch, toolchain_roots)
    env.update(
        {
            "CI": "1",
            "NO_COLOR": "1",
            "COREPACK_HOME": str(home / ".corepack"),
            "PNPM_HOME": str(home / ".pnpm"),
            "npm_config_cache": str(home / ".npm"),
            "npm_config_build_from_source": "true",
        }
    )
    if toolchain_roots:
        env["npm_config_nodedir"] = str(toolchain_roots[0])
    if cache:
        env["COREPACK_HOME"] = cache
    return profile_name, env
