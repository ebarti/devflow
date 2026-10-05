"""Frozen, server-owned scope for managed local deliveries."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .contracts import RUN_ID_RE, digest
from .delivery_native_guard import NATIVE_OVERRIDES
from .delivery_origin import thread_uuid
from .delivery_sandbox import validate_network_domain
from .runtime_dependencies import locked_dependency_identity

BRANCH_RE = re.compile(r"^(?:feat|fix|docs|chore)/[A-Za-z0-9][A-Za-z0-9._/-]{0,120}$")
COMMAND_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
CHECK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
QA_PORT_ENV_RE = re.compile(r"^[A-Z][A-Z0-9_]*_PORT$")
QA_ENV_KEYS = {"JOBCTRL_E2E_ISOLATED", "PLAYWRIGHT_BROWSERS_PATH"}


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


def publication_base_ref(source: Path, base_ref: str, base_sha: str) -> str:
    """Bind GitHub's branch primitive independently of the pinned Git object."""
    try:
        ref = _git(source, "rev-parse", "--symbolic-full-name", base_ref)
        if not ref and re.fullmatch(r"[0-9a-fA-F]{40}", base_ref):
            ref = _git(source, "symbolic-ref", "refs/remotes/origin/HEAD")
        if _git(source, "rev-parse", ref) != base_sha:
            raise ValueError("publication branch does not identify the frozen base commit")
        for prefix in ("refs/remotes/origin/", "refs/heads/"):
            if ref.startswith(prefix):
                branch = ref.removeprefix(prefix)
                if branch != "HEAD":
                    _git(source, "check-ref-format", "refs/heads/" + branch)
                    return branch
    except subprocess.CalledProcessError as exc:
        raise ValueError("publication base requires a resolvable branch") from exc
    raise ValueError("publication base requires a branch, not a tag or ambiguous commit")


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
        for repository in value["repositories"].values():
            statuses = repository.get("project_statuses", {})
            if (not isinstance(statuses, dict)
                    or statuses.keys() - {"in-progress", "in-review", "blocked", "paused", "done"}
                    or any(not isinstance(name, str) or not name.strip()
                           for name in statuses.values())):
                raise ValueError("repository Project status mapping is invalid")
        roles = set(value["roles"])
        if not {"implement", "review", "verify"} <= roles or roles - {
            "intake", "implement", "review", "verify"
        }:
            raise ValueError("service role policy has missing or unknown roles")
        if value.get("provider", "codex") not in {"codex", "fake"}:
            raise ValueError("unsupported configured role provider")
        if value.get("execution_mode", "native-profile") not in {"native-profile", "trusted-local"}:
            raise ValueError("unsupported local execution mode")
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
            "intake_enabled": "intake" in self.raw["roles"],
            "execution_backend": self.raw.get("execution_backend", "native-macos"),
            "execution_mode": self.raw.get("execution_mode", "native-profile"),
        }

    def admit(self, supplied: dict[str, Any]) -> dict[str, Any]:
        required = {
            "command_id",
            "run_id",
            "work_id",
            "issue_url",
            "repository_key",
            "goal",
            "base_ref",
            "branch",
            "authorized_endpoint",
        }
        optional = {
            "accepted_plan", "recovery_key", "supersedes_run_id",
            "plan_approval", "origin_thread_id"
        }
        if set(supplied) - (required | optional) or required - set(supplied):
            raise ValueError("submit fields do not match the delivery contract")
        if not all(isinstance(supplied[key], str) and supplied[key].strip() for key in required):
            raise ValueError("required submit fields must be non-empty strings")
        accepted_plan = supplied.get("accepted_plan")
        if "origin_thread_id" in supplied:
            thread_uuid(supplied["origin_thread_id"])
        plan_approval = supplied.get("plan_approval", "automatic")
        if not isinstance(plan_approval, str) or plan_approval not in {"automatic", "required"}:
            raise ValueError("plan_approval must be automatic or required")
        if accepted_plan is not None and plan_approval == "required":
            raise ValueError("plan_approval required contradicts a supplied accepted_plan")
        if accepted_plan is not None and (
            not isinstance(accepted_plan, str) or not accepted_plan.strip()
        ):
            raise ValueError("supplied accepted plan must be a non-empty string")
        if accepted_plan is None and "intake" not in self.raw["roles"]:
            raise ValueError("raw goals require a configured intake role")
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
        publication_branch = (
            publication_base_ref(source, supplied["base_ref"], base_sha)
            if self.raw.get("provider", "codex") == "codex" else None
        )
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
            "max_repairs": int(self.raw.get("max_repairs", 2)),
            "capacity": int(self.raw.get("capacity", 2)),
            "fake_findings": self.raw.get("fake_findings", {})
            if self.raw.get("provider") == "fake"
            else {},
            "fake_intake": self.raw.get("fake_intake", [])
            if self.raw.get("provider") == "fake"
            else [],
        }
        if self.raw.get("execution_backend", "native-macos") != "native-macos":
            raise ValueError("Docker execution is retired; only native-macos is supported")
        if self.raw.get("provider", "codex") == "codex":
            policy["execution_backend"] = "native-macos"
            policy["config_overrides"] = self.raw.get("config_overrides", NATIVE_OVERRIDES)
            policy["max_intake_rounds"] = 8
        protected_native = (
            self.state_root,
            self.tracking_db,
            self.path,
            Path(policy["codex_auth_path"] or Path.home() / ".codex" / "auth.json"),
            Path.home() / ".codex",
            Path.home() / ".config" / "gh",
            Path.home() / ".ssh",
            Path.home() / ".aws",
            Path.home() / ".npmrc",
        )

        def native_read_root(raw_root: str) -> None:
            root = Path(raw_root)
            if (
                not root.is_absolute()
                or not root.is_dir()
                or root.resolve(strict=True) != root
                or any(
                    item.resolve().is_relative_to(root) or root.is_relative_to(item.resolve())
                    for item in protected_native
                )
            ):
                raise ValueError("native read root overlaps controller authority or credentials")

        for role, selected in policy["roles"].items():
            if (
                not isinstance(selected, dict)
                or not isinstance(selected.get("model"), str)
                or not selected["model"].strip()
                or not isinstance(selected.get("effort"), str)
                or not selected["effort"].strip()
            ):
                raise ValueError(f"{role} model and effort must be configured")
            if self.raw.get("provider", "codex") == "codex":
                timeout = selected.get("timeout_seconds", 7200)
                if type(timeout) is not int or not 1 <= timeout <= 7200:
                    raise ValueError("native role deadline must be between 1 and 7200 seconds")
        if policy["max_repairs"] < 0 or policy["max_repairs"] > 3:
            raise ValueError("max_repairs must be between 0 and 3")
        prompt = policy["initial_decision_prompt"]
        if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
            raise ValueError("initial decision prompt must be a non-empty string")
        if self.raw.get("provider", "codex") == "codex":
            policy["host_sandbox"] = self.raw.get("execution_mode", "native-profile")
            policy["runtime_dependencies"] = locked_dependency_identity()
            expected_overrides = NATIVE_OVERRIDES
            if policy["config_overrides"] != expected_overrides:
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
                if policy["execution_backend"] == "native-macos":
                    native_read_root(raw_root)
            cache = policy["package_manager_cache"]
            if cache is not None:
                if not isinstance(cache, str) or not Path(cache).is_absolute():
                    raise ValueError("package manager cache must be an absolute directory")
                cache_path = Path(cache)
                if cache_path.is_symlink() or not cache_path.is_dir():
                    raise ValueError("package manager cache is unavailable")
                if policy["execution_backend"] == "native-macos":
                    native_read_root(cache)
            if not policy["required_ci"]:
                raise ValueError("real delivery requires named CI checks")
            if not repository.get("project_url") or not repository.get("assignee"):
                raise ValueError("real delivery requires a managed tracker target")
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
                    if policy["execution_backend"] == "native-macos":
                        timeout = check.get("timeout_seconds", 600)
                        if type(timeout) is not int or not 1 <= timeout <= 7200:
                            raise ValueError("native check deadline must be bounded")
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
                if policy["execution_backend"] == "native-macos":
                    if not isinstance(read_roots or [], list) or len(read_roots or []) > 4:
                        raise ValueError("native browser read roots must be a bounded list")
                    for raw_root in read_roots or []:
                        if not isinstance(raw_root, str):
                            raise ValueError("native browser read root must be a directory")
                        native_read_root(raw_root)
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
        return {
            **supplied,
            "plan_approval": plan_approval,
            "blocking_questions_version": 1,
            "accepted_plan": accepted_plan or "",
            "intake_required": accepted_plan is None,
            "version": 1,
            **(
                {"preparation_version": 1}
                if self.raw.get("provider", "codex") == "codex"
                else {}
            ),
            **(
                {"resource_cleanup_version": 1}
                if self.raw.get("provider", "codex") == "codex"
                else {}
            ),
            **({"terminal_tracker_version": 1} if self.raw.get("provider", "codex") == "codex"
               else {}),
            "provider": self.raw.get("provider", "codex"),
            "source_path": str(source),
            "origin_url": actual_remote,
            "github_repo": owner_repo,
            "base_sha": base_sha,
            **({"publication_base_ref": publication_branch} if publication_branch else {}),
            "state_dir": str(state_dir),
            "checkout": str(checkout),
            "policy": policy,
            "policy_digest": digest(policy),
            "config_digest": digest(self.raw),
            "config_path": str(self.path),
        }


def scope_amendment_config(
    original: dict[str, Any], config_path: Path, config_sha256: str,
    added_paths: list[str],
) -> DeliveryConfig:
    """Check the sealed local config delta without consulting external services."""

    if (
        not isinstance(added_paths, list)
        or not 1 <= len(added_paths) <= 2
        or any(not isinstance(path, str) for path in added_paths)
        or added_paths != sorted(set(added_paths))
        or not re.fullmatch(r"[0-9a-f]{64}", config_sha256)
    ):
        raise ValueError("scope amendment must name one or two distinct sorted files")
    if not config_path.is_absolute():
        raise ValueError("scope amendment configuration must be absolute")
    root = Path(original["state_dir"]).parents[1].resolve(strict=True)
    info = config_path.lstat()
    path = config_path.resolve(strict=True)
    if (
        not path.is_relative_to(root)
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or hashlib.sha256(path.read_bytes()).hexdigest() != config_sha256
    ):
        raise ValueError("scope amendment configuration is not private or changed")
    original_config = DeliveryConfig.load(Path(original["config_path"]))
    if digest(original_config.raw) != original["config_digest"]:
        raise ValueError("original service configuration changed")
    amended = DeliveryConfig.load(path)
    repository_key = original["repository_key"]
    old_raw, new_raw = deepcopy(original_config.raw), deepcopy(amended.raw)
    old_repository = old_raw["repositories"][repository_key]
    new_repository = new_raw["repositories"][repository_key]
    old_allowed = old_repository["allowed_paths"]
    if (
        old_allowed != original["policy"]["allowed_paths"]
        or any(name in old_allowed for name in added_paths)
        or new_repository["allowed_paths"] != [*old_allowed, *added_paths]
    ):
        raise ValueError("scope amendment changed more than the named file list")
    new_repository["allowed_paths"] = old_allowed
    if new_raw != old_raw:
        raise ValueError("scope amendment changed unrelated execution authority")
    return amended


def scope_amended_spec(
    original: dict[str, Any], config_path: Path, config_sha256: str,
    added_paths: list[str],
) -> dict[str, Any]:
    """Admit one explicit file-scope delta without changing the submitted run."""

    amended = scope_amendment_config(original, config_path, config_sha256, added_paths)
    submit_keys = {
        "command_id", "run_id", "work_id", "issue_url", "repository_key",
        "goal", "accepted_plan", "base_ref", "branch", "authorized_endpoint",
        "recovery_key", "supersedes_run_id",
        "plan_approval",
        "origin_thread_id",
    }
    from .delivery_preparation import require_native_execution

    require_native_execution(original)
    supplied = {key: original[key] for key in submit_keys if key in original}
    if original.get("intake_required") or original.get("plan_approval") == "required":
        # Re-admit the original raw goal; its separately bound plan is immutable.
        supplied.pop("accepted_plan", None)
        supplied["plan_approval"] = original.get("plan_approval", "required")
    effective = amended.admit(supplied)
    effective["accepted_plan"] = original["accepted_plan"]
    if "plan_approval" in original:
        effective["plan_approval"] = original["plan_approval"]
    else:
        effective.pop("plan_approval")
    if "blocking_questions_version" not in original:
        effective.pop("blocking_questions_version")
    effective["intake_required"] = original.get("intake_required", False)
    for key in (
        "run_id", "work_id", "issue_url", "repository_key", "goal", "accepted_plan",
        "base_ref", "branch", "authorized_endpoint", "source_path", "origin_url",
        "github_repo", "base_sha", "state_dir", "checkout", "provider",
    ):
        if effective[key] != original[key]:
            raise ValueError("scope amendment changed the admitted run identity")
    if effective.get("origin_thread_id") != original.get("origin_thread_id"):
        raise ValueError("scope amendment changed the originating thread")
    effective["request_digest"] = original["request_digest"]
    if "continuation" in original:
        effective["continuation"] = original["continuation"]
    if original.get("preparation_version") == 1:
        from .delivery_native_preparation import bind_native_spec
        from .delivery_preparation import verify_prepared_spec

        verify_prepared_spec(original)
        proof_path = Path(original["preparation"]["environment"]["path"])
        effective = bind_native_spec(
            effective, original["policy"]["native_identity"], proof_path, reused=True
        )
        verify_prepared_spec(effective)
    return effective
