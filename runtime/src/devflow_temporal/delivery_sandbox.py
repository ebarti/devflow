"""Native Codex permission profiles for real roles and checks; Seatbelt for fake tests."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import shlex
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any


def _path(value: Path) -> str:
    return json.dumps(str(value.resolve()))


def _protected_native_commands(spec: dict[str, Any]) -> tuple[Path, ...]:
    from .delivery_preparation import require_native_execution

    require_native_execution(spec)
    from .delivery_native_guard import protected_commands

    return protected_commands(spec["policy"]["codex_bin"])


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
    if role not in {"intake", "review"}:
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
            "TMP": str(scratch),
            "TEMP": str(scratch),
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


def _remove_generated_project_directory(workspace: Path) -> None:
    """Discard only Codex's empty project directory between owned role turns.

    rmdir supplies the final atomic emptiness check. A project config, symlink,
    non-owned directory, or concurrent replacement remains a hard failure.
    """

    project = workspace / ".codex"
    try:
        info = project.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("project Codex configuration is not admitted")
    try:
        current = project.lstat()
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise ValueError("project Codex directory changed before role launch")
        project.rmdir()
    except OSError as exc:
        raise ValueError("project Codex configuration is not admitted") from exc
    if project.exists() or project.is_symlink():
        raise ValueError("project Codex directory changed before role launch")


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
            "TMP": str(scratch),
            "TEMP": str(scratch),
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
    extra_write: tuple[Path, ...] = (),
    network_domains: tuple[str, ...] = (),
    protected_executables: tuple[Path, ...] = (),
) -> list[str]:
    if workspace_access not in {"read", "write"}:
        raise ValueError("unsupported workspace access")
    lines = [
        f'default_permissions = "{name}"',
        'web_search = "disabled"',
        "[features]",
        "plugins = false",
        "network_proxy = true",
        *(["multi_agent = false"] if protected_executables else []),
        f"[permissions.{name}.filesystem]",
        '":root" = "deny"',
        '":minimal" = "read"',
        # Native launches always supply the registered scratch as TMPDIR; the
        # alias and literal must agree. Global /tmp remains denied. Historical
        # contained profiles retain their recorded alias policy.
        '":tmpdir" = "write"' if protected_executables else '":tmpdir" = "deny"',
        '":slash_tmp" = "deny"',
        f'{_path(Path("/opt/homebrew"))} = "read"',
        f'{_path(Path("/usr/local"))} = "read"',
        f'{_path(Path("/System/Library/OpenSSL"))} = "read"',
        f'{_path(workspace)} = "{workspace_access}"',
        f'{_path(workspace / ".git")} = "deny"',
        f'{_path(home)} = "write"',
        f'{_path(scratch)} = "write"',
        f'{_path(codex_home)} = "deny"',
    ]
    for path in extra_write:
        lines.append(f'{_path(path)} = "write"')
    for path in extra_read:
        lines.append(f'{_path(path)} = "read"')
    for path in sorted({item.resolve() for item in protected_executables}, key=str):
        lines.append(f'{_path(path)} = "deny"')
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
    if protected_executables:
        lines.extend(("[agents]", "enabled = false"))
    return lines


def trusted_local(spec: dict[str, Any]) -> bool:
    return spec["policy"].get("host_sandbox") == "trusted-local"


def _trusted_lines(workspace: Path) -> list[str]:
    # The SDK supplies full_access and deny_all. Checks run directly on the host.
    # These controls prevent ordinary nested tools, not hostile-code escape.
    return [
        'approval_policy = "never"', 'sandbox_mode = "danger-full-access"',
        'web_search = "disabled"', '[features]', 'plugins = false',
        'multi_agent = false', '[agents]', 'enabled = false',
        f"[projects.{_path(workspace)}]", 'trust_level = "trusted"',
    ]


def native_check_argv(
    spec: dict[str, Any], profile: str, cwd: Path, argv: list[str],
) -> list[str]:
    if trusted_local(spec):
        return list(argv)
    return [spec["policy"]["codex_bin"], "sandbox", "-P", profile, "-C", str(cwd), "--", *argv]


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


def prepare_native_role(
    request: dict[str, Any], attempt_dir: Path
) -> tuple[str, dict[str, str]]:
    """Keep provider auth in the trusted CLI, while its commands use a native profile."""

    spec = request["spec"]
    from .delivery_preparation import require_native_execution

    require_native_execution(spec)
    if spec.get("provider") != "codex" or spec["policy"].get("host_sandbox") not in {
        "native-profile", "trusted-local",
    }:
        raise ValueError("real role did not require the native profile boundary")
    diff_path = _review_diff_path(request)
    _browser_qa_evidence(request)
    workspace = Path(request["workspace"]).resolve(strict=True)
    _remove_generated_project_directory(workspace)
    role_home = _native_role_home(request)
    codex_home = role_home / "codex"
    from .delivery_resources import RunResources

    ephemeral_home = RunResources(spec).scratch("role", request["role"])
    scratch = RunResources(spec).execution_scratch("role", request["role"])
    for path in (role_home, codex_home, ephemeral_home, scratch, attempt_dir):
        _private(path)
    source = Path(spec["policy"].get("codex_auth_path") or Path.home() / ".codex" / "auth.json")
    _private_file(source)
    _write_once(codex_home / "auth.json", source.read_bytes())
    recovery = Path(spec["state_dir"]) / "recovery"
    toolchain_roots = tuple(Path(root) for root in spec["policy"].get("toolchain_roots", []))
    cache = spec["policy"].get("package_manager_cache")
    review_diff = request.get("review_diff")
    qa_evidence = request.get("qa_evidence")
    extra_read = (
        toolchain_roots
        + ((Path(cache),) if cache else ())
        + ((recovery,) if request["role"] == "implement" and recovery.is_dir() else ())
        + ((diff_path,) if review_diff else ())
        + ((Path(qa_evidence["path"]), Path(qa_evidence["log"])) if qa_evidence else ())
        + ((Path(spec["state_dir"]) / "role-evidence",)
           if request.get("role_evidence_key") else ())
        + (Path(sys.base_prefix),)
    )
    profile_workspace = workspace
    profile_home = ephemeral_home
    profile_codex_home = codex_home
    profile_scratch = scratch
    profile_name = "devflow-role"
    lines = _profile_lines(
        profile_name,
        workspace=profile_workspace,
        workspace_access="read" if request["role"] in {"intake", "review"} else "write",
        home=profile_home,
        codex_home=profile_codex_home,
        scratch=profile_scratch,
        extra_read=extra_read,
        extra_write=((Path(request["artifact_write_root"]),)
                     if request.get("artifact_write_root") else ()),
        protected_executables=_protected_native_commands(spec),
    )
    if trusted_local(spec):
        lines = _trusted_lines(workspace)
    _write_once(codex_home / "config.toml", ("\n".join(lines) + "\n").encode())
    env = _native_env(ephemeral_home, codex_home, scratch, toolchain_roots)
    if cache:
        env["COREPACK_HOME"] = cache
    return profile_name, env


def _native_role_home(request: dict[str, Any]) -> Path:
    spec = request['spec']
    home = Path(spec['state_dir']) / 'role-homes' / (
        request['role'] + ('-' + spec['role_home_generation']
                           if spec.get('role_home_generation') else '')
    )
    if request['role'] != 'implement':
        home /= str(request['iteration'])
    if request['role'] in {'review', 'verify'}:
        from .delivery_resources import _gate_evidence_root

        namespace = _gate_evidence_root(spec).relative_to(Path(spec['state_dir']))
        if namespace != Path('.'):
            # A retained independent config binds its old workspace. Keep it intact
            # and allocate the new independent home from authenticated custody.
            home /= namespace.parent.name
    return home


def _review_diff_path(request: dict[str, Any]) -> Path | None:
    """Consume the exact broker diff in its authenticated gate namespace."""
    review_diff = request.get('review_diff')
    if request['role'] not in {'review', 'verify'}:
        if review_diff is not None:
            raise ValueError('non-gate role may not receive an independent gate diff')
        return None
    if not isinstance(review_diff, dict):
        raise ValueError('independent role requires the controller-bound diff')
    from .delivery_resources import _ancestors, _gate_evidence_root, _gate_path

    spec = request['spec']
    diff_path = Path(review_diff['path'])
    parent = (_gate_evidence_root(spec) / 'gate-evidence'
              / str(request['iteration']) / request['role'])
    if (diff_path.parent != parent
            or diff_path.name != request['candidate']['id'] + '.patch'
            or Path(request['workspace']) != _gate_path(spec, request['role'], request['iteration'])
            or review_diff.get('candidate_id') != request['candidate']['id']
            or review_diff.get('head') != request['candidate']['head']
            or review_diff.get('base_sha') != spec['base_sha']):
        raise ValueError('controller-bound diff is unavailable or changed')
    _ancestors(diff_path)
    fd = os.open(diff_path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or hashlib.file_digest(stream, 'sha256').hexdigest() != review_diff.get('sha256')):
            raise ValueError('controller-bound diff is unavailable or changed')
    return diff_path


def _browser_qa_evidence(request: dict[str, Any]) -> None:
    evidence = request.get('qa_evidence')
    if evidence is None:
        if request['role'] == 'verify' and request['spec']['policy'].get('browser_qa'):
            raise ValueError('configured browser QA receipt is required for verification')
        return
    if request['role'] != 'verify' or not isinstance(evidence, dict):
        raise ValueError('browser QA evidence belongs only to independent verification')
    from .delivery_resources import _ancestors, _gate_evidence_root

    parent = _gate_evidence_root(request['spec']) / 'browser-qa' / str(request['iteration'])
    for field, hash_field in (('path', 'sha256'), ('log', 'log_sha256')):
        path = Path(evidence[field])
        if path.parent != parent:
            raise ValueError('browser QA evidence is unavailable or changed')
        _ancestors(path)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                    or hashlib.file_digest(stream, 'sha256').hexdigest() != evidence[hash_field]):
                raise ValueError('browser QA evidence is unavailable or changed')
    receipt = json.loads(Path(evidence['path']).read_bytes())
    if (evidence.get('candidate_id') != request['candidate']['id']
            or type(evidence.get('iteration')) is not int
            or evidence['iteration'] != request['iteration']
            or receipt.get('candidate_id') != request['candidate']['id']
            or type(receipt.get('iteration')) is not int
            or receipt['iteration'] != request['iteration']
            or receipt.get('state') != 'passed'
            or receipt.get('log_sha256') != evidence['log_sha256']):
        raise ValueError('browser QA evidence assessed a different candidate')


def prepare_native_check(
    spec: dict[str, Any], checkout: Path, evidence_dir: Path, check: dict[str, Any],
    *, dependency_store: Path | None = None,
) -> tuple[str, dict[str, str]]:
    """Run candidate-controlled check commands in a separate credential-free profile."""

    from .delivery_preparation import require_native_execution

    require_native_execution(spec)
    if spec.get("provider") != "codex" or spec["policy"].get("host_sandbox") not in {
        "native-profile", "trusted-local",
    }:
        raise ValueError("real checks require the native profile boundary")

    from .delivery_resources import RunResources

    key = str(evidence_dir.relative_to(Path(spec["state_dir"]))) + "/" + check["id"]
    home = RunResources(spec).scratch("checks", key)
    codex_home = home / "codex"
    scratch = RunResources(spec).execution_scratch("checks", key)
    for path in (home, codex_home, scratch):
        _private(path)
    domains = tuple(check.get("network_domains", ()))
    toolchain_roots = tuple(Path(root) for root in spec["policy"].get("toolchain_roots", []))
    cache = spec["policy"].get("package_manager_cache")
    browser_cache = Path.home() / "Library/Caches/ms-playwright"
    profile_name = "devflow-check"
    lines = _profile_lines(
        profile_name,
        workspace=checkout,
        workspace_access="write",
        home=home,
        codex_home=codex_home,
        scratch=scratch,
        extra_read=(
            toolchain_roots
            + ((Path(cache),) if cache else ())
            + ((dependency_store,) if dependency_store else ())
            + (browser_cache,)
            + (Path(sys.base_prefix),)
        ),
        network_domains=domains,
        protected_executables=_protected_native_commands(spec),
    )
    if trusted_local(spec):
        lines = _trusted_lines(checkout)
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
            # Private HOME must not hide already prepared browser executables.
            "PLAYWRIGHT_BROWSERS_PATH": str(browser_cache),
        }
    )
    if trusted_local(spec) and check.get("kind") == "test":
        artifacts = evidence_dir / check["id"] / "pytest-artifacts"
        _private(artifacts)
        env["PYTEST_ADDOPTS"] = "--basetemp=" + shlex.quote(str(artifacts))
    if toolchain_roots:
        env["npm_config_nodedir"] = str(toolchain_roots[0])
    if cache:
        env["COREPACK_HOME"] = cache
    return profile_name, env


def prepare_browser_qa(
    spec: dict[str, Any],
    checkout: Path,
    evidence_dir: Path,
    scratch: Path,
    qa: dict[str, Any],
) -> tuple[Path, dict[str, str]]:
    """An OS boundary for fixture, API and browser children on exact owned ports.

    The Codex command sandbox cannot be nested inside this Seatbelt profile.
    This separate, credential-free profile is applied before launching the
    deterministic QA command and is inherited by its services and browser.
    """

    from .delivery_preparation import require_native_execution

    require_native_execution(spec)
    if spec.get("provider") != "codex" or spec["policy"].get("host_sandbox") not in {
        "native-profile", "trusted-local",
    }:
        raise ValueError("real browser QA requires the admitted macOS boundary")
    if not trusted_local(spec) and not Path("/usr/bin/sandbox-exec").is_file():
        raise ValueError("required macOS sandbox-exec is unavailable")
    checkout = checkout.resolve(strict=True)
    state_root = Path(spec["state_dir"]).parent.parent.resolve(strict=True)
    scratch = scratch.resolve(strict=True)
    if scratch.parent != Path("/private/tmp") or not scratch.name.startswith("dfqa-"):
        raise ValueError("browser QA scratch is outside the owned short temp root")
    from .delivery_resources import RunResources

    home = RunResources(spec).scratch("browser", evidence_dir.name)
    _private(home)
    _private(scratch)
    ports = tuple(qa["ports"].values())
    if len(ports) != 2 or len(set(ports)) != 2:
        raise ValueError("browser QA requires two distinct owned ports")
    tcp_local = " ".join(f'(local tcp "localhost:{port}")' for port in ports)
    tcp_remote = " ".join(f'(remote tcp "localhost:{port}")' for port in ports)
    protected_home = Path.home().resolve(strict=True)
    readable_roots = (
        *(Path(root) for root in spec["policy"].get("toolchain_roots", [])),
        *(Path(root) for root in qa.get("read_roots", [])),
        *(
            (Path(spec["policy"]["package_manager_cache"]),)
            if spec["policy"].get("package_manager_cache")
            else ()
        ),
    )
    if any(not root.is_dir() or root.resolve(strict=True) != root for root in readable_roots):
        raise ValueError("browser QA read root changed after admission")
    lines = [
        "(version 1)",
        "(allow default)",
        "(deny file-write*)",
        f"(deny file-read* (subpath {_path(protected_home)}))",
        f"(allow file-read-metadata (subpath {_path(protected_home)}))",
        f"(deny file-read* (subpath {_path(state_root)}))",
        f"(allow file-read-metadata (subpath {_path(state_root)}))",
        f"(deny file-read* (subpath {_path(Path('/private/tmp'))}))",
        f"(allow file-read-metadata (subpath {_path(Path('/private/tmp'))}))",
    ]
    host_tmp = Path(tempfile.gettempdir()).resolve(strict=True)
    if host_tmp != Path("/private/tmp"):
        lines.extend(
            (
                f"(deny file-read* (subpath {_path(host_tmp)}))",
                f"(allow file-read-metadata (subpath {_path(host_tmp)}))",
            )
        )
    for path in (
        checkout,
        home,
        scratch,
        *readable_roots,
    ):
        lines.append(f"(allow file-read* (subpath {_path(path)}))")
    for path in (checkout, home, scratch):
        lines.append(f"(allow file-write* (subpath {_path(path)}))")
    for path in (checkout / ".git", checkout / ".codex"):
        lines.append(f"(deny file-read* (subpath {_path(path)}))")
        lines.append(f"(deny file-write* (subpath {_path(path)}))")
    for path in _protected_native_commands(spec):
        lines.append(f"(deny file-read* (literal {_path(path)}))")
        lines.append(f"(deny file-read* (subpath {_path(path)}))")
    lines.extend(
        (
            '(allow file-write* (literal "/dev/null"))',
            "(deny network-bind)",
            "(deny network-inbound)",
            "(deny network-outbound)",
            f"(allow network-bind {tcp_local} (subpath {_path(scratch)}))",
            f"(allow network-inbound {tcp_local} (subpath {_path(scratch)}))",
            f"(allow network-outbound {tcp_remote} (subpath {_path(scratch)}))",
        )
    )
    profile = evidence_dir / "browser-qa.sb"
    if trusted_local(spec):
        lines = ["(version 1)", "(allow default)"]
    _write_once(profile, ("\n".join(lines) + "\n").encode())
    env = _native_env(
        home,
        home / "unused-codex-home",
        scratch,
        tuple(Path(root) for root in spec["policy"].get("toolchain_roots", [])),
    )
    env.pop("CODEX_HOME")
    env.update(qa.get("env", {}))
    env.update({name: str(port) for name, port in qa["ports"].items()})
    env["CI"] = ""
    env["NO_COLOR"] = "1"
    env["npm_config_cache"] = str(home / ".npm")
    if spec["policy"].get("package_manager_cache"):
        env["COREPACK_HOME"] = spec["policy"]["package_manager_cache"]
    return profile, env
