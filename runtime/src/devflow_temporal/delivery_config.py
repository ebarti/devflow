"""Frozen, server-owned scope for managed local deliveries."""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .contracts import RUN_ID_RE, digest

BRANCH_RE = re.compile(r"^(?:feat|fix|docs|chore)/[A-Za-z0-9][A-Za-z0-9._/-]{0,120}$")
COMMAND_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


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
        optional = {"recovery_key"}
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
        source = Path(repository["source_path"]).resolve(strict=True)
        if not source.is_dir() or _git(source, "rev-parse", "--show-toplevel") != str(source):
            raise ValueError("configured source is not a Git working-copy root")
        actual_remote = _git(source, "remote", "get-url", "origin")
        if actual_remote != repository["origin_url"]:
            raise ValueError("configured Git origin changed")
        base_sha = _git(source, "rev-parse", supplied["base_ref"])
        expected = repository.get("expected_base_sha")
        if expected and base_sha != expected:
            raise ValueError("base ref moved from the accepted plan")
        state_dir = self.state_root / "runs" / supplied["run_id"]
        checkout = self.state_root / "checkouts" / supplied["run_id"]
        policy = {
            "roles": self.raw["roles"],
            "checks": repository.get("checks", []),
            "required_ci": repository.get("required_ci", []),
            "allowed_paths": repository.get("allowed_paths", []),
            "recovery": repository.get("recovery", {}).get(recovery_key),
            "codex_bin": self.raw["codex_bin"],
            "codex_auth_path": self.raw.get("codex_auth_path"),
            "tracking_db": str(self.tracking_db),
            "config_overrides": self.raw.get("config_overrides", ["features.plugins=false"]),
            "max_repairs": int(self.raw.get("max_repairs", 2)),
            "capacity": int(self.raw.get("capacity", 2)),
            "fake_findings": self.raw.get("fake_findings", {})
            if self.raw.get("provider") == "fake"
            else {},
        }
        if policy["max_repairs"] < 0 or policy["max_repairs"] > 3:
            raise ValueError("max_repairs must be between 0 and 3")
        if self.raw.get("provider", "codex") == "codex":
            if os.uname().sysname != "Darwin" or not Path("/usr/bin/sandbox-exec").is_file():
                raise ValueError("this host lacks the required outer macOS role sandbox")
            if not Path(policy["codex_bin"]).is_file():
                raise ValueError("configured Codex executable is unavailable")
            policy["host_sandbox"] = "seatbelt"
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
