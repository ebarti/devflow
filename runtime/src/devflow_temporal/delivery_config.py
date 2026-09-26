"""Frozen, server-owned scope for managed local deliveries."""

from __future__ import annotations

import hashlib
import importlib.metadata
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


def _installed_codex_binary() -> Path:
    if importlib.metadata.version("openai-codex") != REQUIRED_CODEX_VERSION:
        raise ValueError("installed Codex SDK is not the tested version")
    distribution = importlib.metadata.distribution("openai-codex-cli-bin")
    if distribution.version != REQUIRED_CODEX_VERSION:
        raise ValueError("installed Codex CLI is not the tested version")
    binary = Path(distribution.locate_file("codex_cli_bin/bin/codex"))
    if not binary.is_file() or binary.is_symlink():
        raise ValueError("tested Codex CLI binary is unavailable")
    return binary.resolve(strict=True)


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
            if os.uname().sysname != "Darwin" or not Path("/usr/bin/sandbox-exec").is_file():
                raise ValueError("this host lacks the required macOS profile sandbox")
            binary = Path(policy["codex_bin"])
            if binary != _installed_codex_binary():
                raise ValueError("real provider requires the tested installed Codex CLI binary")
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise ValueError("configured Codex executable is unavailable")
            if binary.is_symlink():
                raise ValueError("configured Codex executable must not be a symlink")
            with binary.open("rb") as stream:
                if stream.read(2) == b"#!":
                    raise ValueError("configured Codex executable must be a pinned binary")
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
                    if check.get("kind") == "test" and (
                        not check.get("test_count_regex") or int(check.get("min_tests", 0)) < 1
                    ):
                        raise ValueError("test checks require an observed positive count")
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
            with Path(policy["codex_bin"]).open("rb") as stream:
                binary_digest = hashlib.file_digest(stream, "sha256").hexdigest()
            project_root = Path(__file__).resolve().parents[2]
            with (project_root / "pyproject.toml").open("rb") as stream:
                project = tomllib.load(stream)
            kit_revision = project["tool"]["uv"]["sources"]["agent-runtime-kit"]["rev"]
            installed_url = importlib.metadata.distribution("agent-runtime-kit").read_text(
                "direct_url.json"
            )
            if (
                not installed_url
                or json.loads(installed_url).get("vcs_info", {}).get("commit_id") != kit_revision
            ):
                raise ValueError("installed kit does not match the pinned source revision")
            package = Path(__file__).resolve().parent
            source_hashes = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(package.glob("*.py"))
            }
            role = attestation.get("role_observed", {})
            check = attestation.get("check_observed", {})
            role_denied = _boundary_probe_passed(role)
            check_denied = _boundary_probe_passed(check)
            if (
                attestation.get("schema") != "devflow-native-profile-v2"
                or attestation.get("effective_mode") != "Codex native named profile"
                or attestation.get("codex_bin_sha256") != binary_digest
                or attestation.get("kit_revision") != kit_revision
                or attestation.get("codex_sdk_version") != REQUIRED_CODEX_VERSION
                or attestation.get("codex_cli_version") != REQUIRED_CODEX_VERSION
                or attestation.get("source_hashes") != source_hashes
                or attestation.get("security_binding_sha256") != security_digest
                or attestation.get("requested_model") != policy["roles"]["implement"]["model"]
                or attestation.get("requested_effort") != policy["roles"]["implement"]["effort"]
                or not isinstance(attestation.get("role_session_id"), str)
                or not attestation["role_session_id"]
                or not role_denied
                or not check_denied
                or attestation.get("network_enabled_check_loopback") != "PermissionError:1"
                or attestation.get("network_enabled_check_credential") != "PermissionError:1"
                or attestation.get("install_exit_code") != 0
                or attestation.get("api_check_exit_code") != 0
            ):
                raise ValueError("sandbox attestation does not match this executable and boundary")
            policy["codex_bin_sha256"] = binary_digest
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
