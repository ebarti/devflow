"""Frozen, server-owned scope for managed local deliveries."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .contracts import RUN_ID_RE, digest
from .delivery_sandbox import validate_network_domain

BRANCH_RE = re.compile(r"^(?:feat|fix|docs|chore)/[A-Za-z0-9][A-Za-z0-9._/-]{0,120}$")
COMMAND_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
CHECK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
REQUIRED_CODEX_VERSION = "0.157.1"
BOUNDARY_DENIAL_FIELDS = (
    "copied_auth_read",
    "host_credential_read",
    "state_read",
    "state_write",
    "outside_write",
    "slash_tmp_read",
    "slash_tmp_write",
    "private_tmp_read",
    "private_tmp_write",
    "loopback",
)
QA_PORT_ENV_RE = re.compile(r"^[A-Z][A-Z0-9_]*_PORT$")
QA_ENV_KEYS = {"JOBCTRL_E2E_ISOLATED", "PLAYWRIGHT_BROWSERS_PATH"}


def _boundary_probe_passed(observed: Any) -> bool:
    if not isinstance(observed, dict) or not isinstance(observed.get("child"), dict):
        return False
    return (
        observed.get("allowed_write") is True
        and observed.get("child_returncode") == 0
        and observed["child"].get("allowed_write") is True
        and all(
            result.get(field) == "PermissionError:1"
            for result in (observed, observed["child"])
            for field in BOUNDARY_DENIAL_FIELDS
        )
    )


def _browser_qa_probe_passed(observed: Any) -> bool:
    if not isinstance(observed, dict):
        return False
    denied = (
        "host_credential_read",
        "state_read",
        "outside_write",
        "slash_tmp_read",
        "slash_tmp_write",
        "private_tmp_read",
        "private_tmp_write",
        "unrelated_port_connect",
        "unrelated_port_bind",
        "unrelated_host_connect",
    )
    return (
        observed.get("browser_api_sqlite") is True
        and observed.get("owned_listeners") is True
        and observed.get("cleanup") == "confirmed"
        and observed.get("allowed_scratch_write") == "ALLOWED"
        and type(observed.get("test_count")) is int
        and observed["test_count"] >= 2
        and all(observed.get(field) == "PermissionError:1" for field in denied)
        and isinstance(observed.get("child"), dict)
        and observed["child"].get("allowed_scratch_write") == "ALLOWED"
        and all(observed["child"].get(field) == "PermissionError:1" for field in denied)
    )


def _contained_denied(value: Any) -> bool:
    return value in {
        "PermissionError:1",
        "PermissionError:13",
        "FileNotFoundError:2",
        "OSError:101",
    }


def _contained_probe_passed(observed: Any, *, role: bool) -> bool:
    if not isinstance(observed, dict) or not isinstance(observed.get("child"), dict):
        return False
    fields = (
        "host_credential_read",
        "state_read",
        "state_write",
        "outside_write",
        "docker_socket_read",
        "unrelated_host_connect",
    )
    if role:
        fields += ("copied_auth_read", "loopback")
    else:
        fields += ("unrelated_port_bind", "unrelated_port_connect")
    return (
        observed.get("allowed_write") is True
        and observed.get("child_returncode") == 0
        and observed["child"].get("allowed_write") is True
        and all(
            _contained_denied(item.get(field))
            for item in (observed, observed["child"])
            for field in fields
        )
    )


def _container_identity(container: dict[str, Any], *, source: Path) -> dict[str, Any]:
    """Inspect the exact image, CLI launch chain and security profile."""

    required = {
        "docker_bin",
        "docker_bin_sha256",
        "image_id",
        "platform",
        "seccomp_profile",
        "seccomp_sha256",
        "codex_bin",
        "codex_bin_sha256",
        "role_runner_sha256",
        "pnpm_lock_sha256",
    }
    if not isinstance(container, dict) or required - set(container):
        raise ValueError("real delivery requires a complete container policy")
    docker = Path(container["docker_bin"]).resolve(strict=True)
    if not docker.is_file() or not os.access(docker, os.X_OK):
        raise ValueError("configured Docker CLI is unavailable")
    if hashlib.sha256(docker.read_bytes()).hexdigest() != container["docker_bin_sha256"]:
        raise ValueError("Docker CLI changed after boundary attestation")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", container["image_id"]):
        raise ValueError("container image must be an immutable content ID")
    if container["platform"] != "linux/arm64":
        raise ValueError("container platform was not tested")
    profile = Path(container["seccomp_profile"]).resolve(strict=True)
    expected_profile = (
        Path(__file__).resolve().parents[2] / "docker" / "moby-56be731-codex-bwrap-seccomp.json"
    )
    if profile != expected_profile.resolve(strict=True):
        raise ValueError("container seccomp profile is not the reviewed source")
    if hashlib.sha256(profile.read_bytes()).hexdigest() != container["seccomp_sha256"]:
        raise ValueError("container seccomp profile changed")
    runner = Path(__file__).with_name("role_runner.py")
    if hashlib.sha256(runner.read_bytes()).hexdigest() != container["role_runner_sha256"]:
        raise ValueError("role runner source changed after image build")
    lock = source / "pnpm-lock.yaml"
    if (
        lock.is_symlink()
        or not lock.is_file()
        or hashlib.sha256(lock.read_bytes()).hexdigest() != container["pnpm_lock_sha256"]
    ):
        raise ValueError("admitted package lock is unavailable or changed")
    inspected = subprocess.run(
        [str(docker), "image", "inspect", container["image_id"]],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if inspected.returncode:
        raise ValueError("admitted container image is unavailable")
    try:
        values = json.loads(inspected.stdout)
        image = values[0]
    except (ValueError, IndexError, KeyError, TypeError) as exc:
        raise ValueError("container image inspection is malformed") from exc
    labels = image.get("Config", {}).get("Labels") or {}
    if (
        len(values) != 1
        or image.get("Id") != container["image_id"]
        or f"{image.get('Os')}/{image.get('Architecture')}" != container["platform"]
        or labels.get("devflow.role_runner_sha256") != container["role_runner_sha256"]
        or labels.get("devflow.codex_bin_sha256") != container["codex_bin_sha256"]
        or labels.get("devflow.kit_revision") != "d9ed6e186ce028d0db3b044ce959a94f409510c5"
        or labels.get("devflow.codex_cli_version") != REQUIRED_CODEX_VERSION
        or container["codex_bin"]
        != "/opt/devflow-venv/lib/python3.12/site-packages/codex_cli_bin/bin/codex"
    ):
        raise ValueError("container image or launch chain does not match the tested policy")
    return {
        "image_id": image["Id"],
        "platform": container["platform"],
        "docker_bin_sha256": container["docker_bin_sha256"],
        "seccomp_sha256": container["seccomp_sha256"],
        "role_runner_sha256": container["role_runner_sha256"],
        "codex_bin_sha256": container["codex_bin_sha256"],
        "pnpm_lock_sha256": container["pnpm_lock_sha256"],
    }


def security_binding(
    *,
    supplied: dict[str, Any],
    repository: dict[str, Any],
    source: Path,
    origin: str,
    base_sha: str,
    state_dir: Path,
    checkout: Path,
    policy: dict[str, Any],
) -> str:
    """Bind a real boundary probe to one admitted repository and workspace layout."""

    return digest(
        {
            "repository_key": supplied["repository_key"],
            "run_id": supplied["run_id"],
            "branch": supplied["branch"],
            "source": str(source),
            "origin": origin,
            "base_sha": base_sha,
            "state_dir": str(state_dir),
            "checkout": str(checkout),
            "repository_policy": repository,
            "role_and_check_policy": policy,
        }
    )


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


@dataclass(frozen=True)
class DeliveryConfig:
    path: Path
    raw: dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> DeliveryConfig:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("unsupported service configuration")
        for key in ("tracking_db", "state_root", "helpers_dir", "codex_bin"):
            if not Path(value[key]).is_absolute():
                raise ValueError(f"{key} must be absolute")
        if not value.get("repositories") or not value.get("roles"):
            raise ValueError("service requires repository and role policy")
        if set(value["roles"]) != {"implement", "review", "verify"}:
            raise ValueError("service role policy must define implement, review, and verify")
        if value.get("provider", "codex") not in {"codex", "fake"}:
            raise ValueError("unsupported configured role provider")
        return cls(path=path.resolve(), raw=value)

    @property
    def state_root(self) -> Path:
        return Path(self.raw["state_root"])

    @property
    def tracking_db(self) -> Path:
        return Path(self.raw["tracking_db"])

    @property
    def helpers_dir(self) -> Path:
        return Path(self.raw["helpers_dir"])

    @property
    def queue(self) -> str:
        return self.raw.get("queue", "devflow-delivery")

    @property
    def temporal_address(self) -> str:
        return self.raw.get("temporal_address", "127.0.0.1:17333")

    @property
    def dashboard_url(self) -> str:
        return self.raw.get("dashboard_url", "http://127.0.0.1:18770")

    def public_policy(self) -> dict[str, Any]:
        return {
            "roles": self.raw["roles"],
            "repositories": [
                {
                    "key": key,
                    "label": value.get("label", key),
                    "base_ref": value["base_ref"],
                    "base_sha": value.get("expected_base_sha"),
                    "recovery_keys": sorted(value.get("recovery", {})),
                }
                for key, value in sorted(self.raw["repositories"].items())
            ],
            "authorized_endpoint": "published_unmerged",
        }

    def admit(self, supplied: dict[str, Any]) -> dict[str, Any]:
        required = {
            "command_id",
            "run_id",
            "work_id",
            "issue_url",
            "repository_key",
            "goal",
            "accepted_plan",
            "base_ref",
            "branch",
            "authorized_endpoint",
        }
        optional = {"recovery_key", "supersedes_run_id"}
        if set(supplied) - (required | optional) or required - set(supplied):
            raise ValueError("submit fields do not match the delivery contract")
        if not all(isinstance(supplied[key], str) and supplied[key].strip() for key in required):
            raise ValueError("required submit fields must be non-empty strings")
        if not COMMAND_RE.fullmatch(supplied["command_id"]):
            raise ValueError("invalid command ID")
        if not RUN_ID_RE.fullmatch(supplied["run_id"]):
            raise ValueError("invalid run ID")
        if not RUN_ID_RE.fullmatch(supplied["work_id"]):
            raise ValueError("invalid work ID")
        if supplied["authorized_endpoint"] != "published_unmerged":
            raise ValueError("only published_unmerged delivery is authorized")
        if not BRANCH_RE.fullmatch(supplied["branch"]) or ".." in supplied["branch"]:
            raise ValueError("branch is outside the allowed naming policy")
        repository = self.raw["repositories"].get(supplied["repository_key"])
        if repository is None:
            raise ValueError("repository key is not configured")
        if supplied["base_ref"] != repository["base_ref"]:
            raise ValueError("base ref does not match the configured repository")
        issue = urlsplit(supplied["issue_url"])
        owner_repo = repository["github_repo"]
        prefix = f"/{owner_repo}/issues/"
        if (
            issue.scheme != "https"
            or issue.netloc != "github.com"
            or issue.username
            or issue.password
            or issue.query
            or issue.fragment
            or not issue.path.startswith(prefix)
            or not issue.path.removeprefix(prefix).isdigit()
        ):
            raise ValueError("issue URL is outside the configured repository")
        recovery_key = supplied.get("recovery_key")
        if recovery_key is not None and recovery_key not in repository.get("recovery", {}):
            raise ValueError("recovery key is not configured")
        supersedes = supplied.get("supersedes_run_id")
        if supersedes is not None and (
            not isinstance(supersedes, str)
            or not RUN_ID_RE.fullmatch(supersedes)
            or supersedes == supplied["run_id"]
        ):
            raise ValueError("superseded run ID is invalid")
        source = Path(repository["source_path"]).resolve(strict=True)
        if not source.is_dir() or _git(source, "rev-parse", "--show-toplevel") != str(source):
            raise ValueError("configured source is not a Git working-copy root")
        actual_remote = _git(source, "remote", "get-url", "origin")
        if actual_remote != repository["origin_url"]:
            raise ValueError("configured Git origin changed")
        base_sha = _git(source, "rev-parse", supplied["base_ref"])
        base_paths = _git(source, "ls-tree", "-r", "--name-only", base_sha).splitlines()
        if any(path == ".codex" or path.startswith(".codex/") for path in base_paths):
            raise ValueError("project Codex configuration is not admitted")
        expected = repository.get("expected_base_sha")
        if expected and base_sha != expected:
            raise ValueError("base ref moved from the accepted plan")
        state_dir = self.state_root / "runs" / supplied["run_id"]
        checkout = self.state_root / "checkouts" / supplied["run_id"]
        policy = {
            "roles": self.raw["roles"],
            "checks": repository.get("checks", []),
            "prepublish_checks": repository.get("prepublish_checks", []),
            "browser_qa": repository.get("browser_qa"),
            "required_ci": repository.get("required_ci", []),
            "allowed_paths": repository.get("allowed_paths", []),
            "pr_body": repository.get("pr_body"),
            "initial_decision_prompt": repository.get("initial_decision_prompt"),
            "recovery": repository.get("recovery", {}).get(recovery_key),
            "codex_bin": self.raw["codex_bin"],
            "codex_auth_path": self.raw.get("codex_auth_path"),
            "tracking_db": str(self.tracking_db),
            "config_overrides": self.raw.get("config_overrides", ["features.plugins=false"]),
            "toolchain_roots": self.raw.get("toolchain_roots", []),
            "package_manager_cache": self.raw.get("package_manager_cache"),
            "container": self.raw.get("container"),
            "max_repairs": int(self.raw.get("max_repairs", 2)),
            "capacity": int(self.raw.get("capacity", 2)),
            "fake_findings": self.raw.get("fake_findings", {})
            if self.raw.get("provider") == "fake"
            else {},
        }
        if policy["max_repairs"] < 0 or policy["max_repairs"] > 3:
            raise ValueError("max_repairs must be between 0 and 3")
        prompt = policy["initial_decision_prompt"]
        if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
            raise ValueError("initial decision prompt must be a non-empty string")
        if self.raw.get("provider", "codex") == "codex":
            container_identity = _container_identity(policy["container"], source=source)
            if policy["config_overrides"] != ["features.plugins=false"]:
                raise ValueError("real role configuration overrides must disable plugins")
            if (
                not policy["allowed_paths"]
                or not policy["prepublish_checks"]
                or not policy["checks"]
            ):
                raise ValueError("real delivery requires source scope and both check stages")
            for raw_path in policy["allowed_paths"]:
                if not isinstance(raw_path, str):
                    raise ValueError("allowed feature path must be a relative file")
                path = Path(raw_path)
                if (
                    path.is_absolute()
                    or not path.parts
                    or any(part in {".", "..", ".git", ".codex"} for part in path.parts)
                    or path.name in {".gitattributes", ".gitmodules"}
                    or raw_path != path.as_posix()
                ):
                    raise ValueError("allowed feature path controls Git or Codex configuration")
            if (
                not isinstance(policy["toolchain_roots"], list)
                or len(policy["toolchain_roots"]) > 3
            ):
                raise ValueError("toolchain roots must be a bounded service list")
            for raw_root in policy["toolchain_roots"]:
                if not isinstance(raw_root, str) or not Path(raw_root).is_absolute():
                    raise ValueError("toolchain root must be an absolute directory")
                root = Path(raw_root)
                if root.is_symlink() or not root.is_dir() or not (root / "bin").is_dir():
                    raise ValueError("toolchain root is unavailable")
            cache = policy["package_manager_cache"]
            if cache is not None:
                if not isinstance(cache, str) or not Path(cache).is_absolute():
                    raise ValueError("package manager cache must be an absolute directory")
                cache_path = Path(cache)
                if cache_path.is_symlink() or not cache_path.is_dir():
                    raise ValueError("package manager cache is unavailable")
            if not policy["required_ci"]:
                raise ValueError("real delivery requires named CI checks")
            if not repository.get("project_url") or not repository.get("assignee"):
                raise ValueError("real delivery requires a managed tracker target")
            for role in ("implement", "review", "verify"):
                selected = policy["roles"][role]
                if not selected.get("model") or not selected.get("effort"):
                    raise ValueError(f"{role} model and effort must be configured")
            for stage in ("prepublish_checks", "checks"):
                ids = set()
                for check in policy[stage]:
                    if (
                        not isinstance(check, dict)
                        or not isinstance(check.get("id"), str)
                        or not CHECK_ID_RE.fullmatch(check["id"])
                        or not isinstance(check.get("argv"), list)
                        or not check["argv"]
                        or any(not isinstance(item, str) or not item for item in check["argv"])
                    ):
                        raise ValueError(f"{stage} has an incomplete check command")
                    if check["id"] in ids:
                        raise ValueError(f"{stage} contains a duplicate check ID")
                    ids.add(check["id"])
                    if check.get("env"):
                        raise ValueError("real check environment cannot carry arbitrary variables")
                    if not isinstance(check.get("network_domains", []), list) or any(
                        not isinstance(domain, str) for domain in check.get("network_domains", [])
                    ):
                        raise ValueError("check network domains must be exact hosts")
                    for domain in check.get("network_domains", []):
                        validate_network_domain(domain)
                    if check.get("network_domains"):
                        raise ValueError("contained checks must run with networking disabled")
                    if check["id"] == "install" and (
                        "--offline" not in check["argv"]
                        or "--frozen-lockfile" not in check["argv"]
                        or "--store-dir" not in check["argv"]
                    ):
                        raise ValueError(
                            "contained package installation must use the frozen offline store"
                        )
                    if check.get("kind") == "test" and (
                        not check.get("test_count_regex") or int(check.get("min_tests", 0)) < 1
                    ):
                        raise ValueError("test checks require an observed positive count")
            qa = policy["browser_qa"]
            if qa is not None:
                if not isinstance(qa, dict) or not CHECK_ID_RE.fullmatch(qa.get("id", "")):
                    raise ValueError("browser QA needs a stable check ID")
                argv = qa.get("argv")
                if (
                    not isinstance(argv, list)
                    or not argv
                    or any(not isinstance(arg, str) or not arg for arg in argv)
                ):
                    raise ValueError("browser QA needs a fixed nonempty argv")
                cwd = qa.get("cwd", ".")
                if not isinstance(cwd, str) or Path(cwd).is_absolute() or ".." in Path(cwd).parts:
                    raise ValueError("browser QA cwd must remain in the checkout")
                ports = qa.get("ports")
                if (
                    not isinstance(ports, dict)
                    or len(ports) != 2
                    or any(not QA_PORT_ENV_RE.fullmatch(key) for key in ports)
                    or any(
                        type(port) is not int or not 1024 <= port <= 65535
                        for port in ports.values()
                    )
                    or len(set(ports.values())) != 2
                ):
                    raise ValueError("browser QA requires two distinct exact TCP port leases")
                env = qa.get("env", {})
                if (
                    not isinstance(env, dict)
                    or set(env) - QA_ENV_KEYS
                    or any(not isinstance(value, str) for value in env.values())
                ):
                    raise ValueError("browser QA environment exceeds the fixed fixture controls")
                if env.get("JOBCTRL_E2E_ISOLATED") != "1":
                    raise ValueError("browser QA fixture isolation must be enabled")
                read_roots = qa.get("read_roots", [])
                if read_roots not in (None, []):
                    raise ValueError("container browser QA cannot mount host read roots")
                browser_path = env.get("PLAYWRIGHT_BROWSERS_PATH")
                if browser_path != "/ms-playwright":
                    raise ValueError("browser executable must use the pinned image path")
                artifact_paths = qa.get("artifact_paths", [])
                if (
                    not isinstance(artifact_paths, list)
                    or len(artifact_paths) > 3
                    or any(
                        not isinstance(path, str)
                        or Path(path).is_absolute()
                        or ".." in Path(path).parts
                        for path in artifact_paths
                    )
                ):
                    raise ValueError("browser artifacts must be bounded checkout-relative paths")
                if (
                    not isinstance(qa.get("test_count_regex"), str)
                    or not qa["test_count_regex"]
                    or type(qa.get("min_tests")) is not int
                    or qa["min_tests"] < 1
                    or type(qa.get("timeout_seconds")) is not int
                    or not 30 <= qa["timeout_seconds"] <= 1800
                ):
                    raise ValueError("browser QA requires positive count and bounded timeout")
            security_digest = security_binding(
                supplied=supplied,
                repository=repository,
                source=source,
                origin=actual_remote,
                base_sha=base_sha,
                state_dir=state_dir,
                checkout=checkout,
                policy=policy,
            )
            attestation_path = Path(self.raw.get("sandbox_attestation_path", ""))
            if not attestation_path.is_absolute():
                raise ValueError("real role policy requires an absolute sandbox attestation path")
            metadata = attestation_path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise ValueError("sandbox attestation must be an owned private file")
            attestation_bytes = attestation_path.read_bytes()
            attestation = json.loads(attestation_bytes)
            project_root = Path(__file__).resolve().parents[2]
            with (project_root / "pyproject.toml").open("rb") as stream:
                project = tomllib.load(stream)
            kit_revision = project["tool"]["uv"]["sources"]["agent-runtime-kit"]["rev"]
            if kit_revision != "d9ed6e186ce028d0db3b044ce959a94f409510c5":
                raise ValueError("runtime kit source is not the tested container revision")
            package = Path(__file__).resolve().parent
            source_hashes = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(package.glob("*.py"))
            }
            role = attestation.get("role_observed", {})
            check = attestation.get("check_observed", {})
            browser = attestation.get("browser_qa_observed", {})
            evidence = attestation.get("evidence", {})
            if not isinstance(evidence, dict) or set(evidence) != {
                "role",
                "check",
                "browser",
                "detached",
                "resume",
            }:
                raise ValueError("container boundary evidence is incomplete")
            for recorded in evidence.values():
                if not isinstance(recorded, dict):
                    raise ValueError("container evidence reference is malformed")
                path = Path(recorded.get("path", ""))
                if not path.is_absolute():
                    raise ValueError("container evidence reference is not absolute")
                info = path.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or hashlib.sha256(path.read_bytes()).hexdigest() != recorded.get("sha256")
                ):
                    raise ValueError("container evidence changed after the conformance probe")
            if (
                attestation.get("schema") != "devflow-container-v4"
                or attestation.get("effective_mode")
                != "Docker private PID namespace with Codex native role permissions"
                or attestation.get("container_identity") != container_identity
                or attestation.get("kit_revision") != kit_revision
                or attestation.get("codex_sdk_version") != REQUIRED_CODEX_VERSION
                or attestation.get("codex_cli_version") != REQUIRED_CODEX_VERSION
                or attestation.get("source_hashes") != source_hashes
                or attestation.get("security_binding_sha256") != security_digest
                or attestation.get("requested_model") != policy["roles"]["implement"]["model"]
                or attestation.get("requested_effort") != policy["roles"]["implement"]["effort"]
                or not isinstance(attestation.get("role_session_id"), str)
                or not attestation["role_session_id"]
                or not _contained_probe_passed(role, role=True)
                or not _contained_probe_passed(check, role=False)
                or role.get("cleanup") != "confirmed"
                or check.get("cleanup") != "confirmed"
                or attestation.get("same_session_resume") is not True
                or attestation.get("detached_cleanup")
                != {"role": True, "check": True, "browser": True}
                or attestation.get("install_exit_code") != 0
                or attestation.get("api_check_exit_code") != 0
                or (
                    qa is not None
                    and not (
                        _contained_probe_passed(browser, role=False)
                        and browser.get("browser_api_sqlite") is True
                        and browser.get("owned_listeners") is True
                        and browser.get("cleanup") == "confirmed"
                        and type(browser.get("test_count")) is int
                        and browser["test_count"] >= 2
                    )
                )
            ):
                raise ValueError("sandbox attestation does not match this executable and boundary")
            policy["sandbox_attestation_sha256"] = hashlib.sha256(attestation_bytes).hexdigest()
            policy["security_binding_sha256"] = security_digest
            policy["kit_revision"] = kit_revision
            policy["host_sandbox"] = "native-profile"
        return {
            **supplied,
            "version": 1,
            "provider": self.raw.get("provider", "codex"),
            "source_path": str(source),
            "origin_url": actual_remote,
            "github_repo": owner_repo,
            "base_sha": base_sha,
            "state_dir": str(state_dir),
            "checkout": str(checkout),
            "policy": policy,
            "policy_digest": digest(policy),
            "config_digest": digest(self.raw),
            "config_path": str(self.path),
        }
