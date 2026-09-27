"""Validated Git, GitHub and check effects for an admitted delivery."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import time
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import canonical_json
from .delivery_browser_qa import run_browser_qa as execute_browser_qa
from .delivery_container import Bind, OwnedContainer, dependency_volume
from .delivery_continuation import copy_session_state, selected_digest, session_state_digest
from .delivery_output import observed_test_count, visible_output
from .delivery_store import DeliveryStore, _now


def _run(argv: list[str], *, cwd: Path | None = None, timeout: int = 120) -> str:
    result = subprocess.run(
        argv, cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False
    )
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {argv[0]} {argv[1]}: "
            + (result.stderr.strip() or result.stdout.strip())[:500]
        )
    return result.stdout.strip()


def _git(path: Path, *args: str) -> str:
    return _run(
        [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(path),
            *args,
        ]
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class BrokerReadbackUnavailable(RuntimeError):
    """A remote PR query failed before its authority could be inspected."""


class DeliveryBroker:
    def __init__(self, store: DeliveryStore, spec: dict[str, Any]) -> None:
        self.store = store
        self.spec = spec
        self.source = Path(spec["source_path"])
        self.checkout = Path(spec["checkout"])
        self.state_dir = Path(spec["state_dir"])

    def _effect(self, key: str, kind: str, request: dict[str, Any]) -> dict[str, Any] | None:
        serialized = canonical_json(request)
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            saved = db.execute(
                """SELECT kind,request_json,state,observed_json
                   FROM delivery_effects WHERE effect_key=?""",
                (key,),
            ).fetchone()
            if saved:
                if saved[0] != kind or saved[1] != serialized:
                    raise ValueError("effect identity belongs to a different request")
                return json.loads(saved[3]) if saved[2] == "complete" and saved[3] else None
            db.execute(
                """INSERT INTO delivery_effects
                   (effect_key,run_id,kind,request_json,state,updated_at)
                   VALUES (?,?,?,?,'pending',?)""",
                (key, self.spec["run_id"], kind, serialized, _now()),
            )
        return None

    def _finish_effect(self, key: str, result: dict[str, Any]) -> None:
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_effects SET state='complete',observed_json=?,updated_at=?
                   WHERE effect_key=?""",
                (canonical_json(result), _now(), key),
            )

    def prepare(self) -> dict[str, Any]:
        key = f"prepare:{self.spec['run_id']}"
        request = {
            "base_sha": self.spec["base_sha"],
            "branch": self.spec["branch"],
            "checkout": str(self.checkout),
            "recovery": self.spec["policy"].get("recovery"),
        }
        done = self._effect(key, "prepare", request)
        if done:
            if not self.checkout.is_dir():
                raise RuntimeError("prepared checkout disappeared")
            return done
        self.checkout.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.checkout.exists():
            if _git(self.checkout, "rev-parse", "--show-toplevel") != str(self.checkout):
                raise RuntimeError("owned checkout path was replaced")
            if _git(self.checkout, "branch", "--show-current") != self.spec["branch"]:
                raise RuntimeError("owned checkout branch changed")
        else:
            local_branch = subprocess.run(
                [
                    "git",
                    "-C",
                    str(self.source),
                    "show-ref",
                    "--verify",
                    "--quiet",
                    f"refs/heads/{self.spec['branch']}",
                ],
                check=False,
            )
            if local_branch.returncode == 0:
                raise RuntimeError("branch already exists without its owned checkout")
            _git(
                self.source,
                "worktree",
                "add",
                "-b",
                self.spec["branch"],
                str(self.checkout),
                self.spec["base_sha"],
            )
        recovery = self.spec["policy"].get("recovery")
        provenance = self._recover(recovery) if recovery else None
        candidate = self.candidate()
        continuation = self.spec.get("continuation")
        if continuation:
            source = Path(recovery["source_path"])
            if (
                selected_digest(source, recovery["paths"]) != continuation["source_manifest_sha256"]
                or candidate["id"] != continuation["candidate_id"]
            ):
                raise ValueError("continuation import differs from the finished role candidate")
            source_home = (
                self.state_dir.parent / continuation["from_run_id"] / "role-homes" / "implement"
            )
            destination_home = self.state_dir / "role-homes" / "implement"
            copy_session_state(
                source_home,
                destination_home,
                continuation["session_id"],
                continuation["session_state_sha256"],
            )
            if (
                session_state_digest(destination_home, continuation["session_id"])
                != continuation["session_state_sha256"]
            ):
                raise ValueError("continuation session state changed after import")
        result = {
            "checkout": str(self.checkout),
            "candidate": candidate,
            "provenance": provenance,
            "continuation": continuation,
        }
        self._finish_effect(key, result)
        return result

    def _recover(self, recovery: dict[str, Any]) -> dict[str, Any]:
        old = Path(recovery["source_path"])
        if not old.is_dir() or _git(old, "rev-parse", "--show-toplevel") != str(old):
            raise ValueError("configured recovery source is unavailable")
        owned = set(self.spec["policy"].get("allowed_paths", []))
        selected = set(recovery["paths"])
        if not selected or not selected <= owned:
            raise ValueError("recovery paths exceed the admitted source scope")
        expected_base = recovery.get("base_sha")
        if (
            expected_base
            and _git(old, "merge-base", "HEAD", self.spec["base_sha"]) != expected_base
        ):
            raise ValueError("recovery source ancestry changed")
        evidence_dir = self.state_dir / "recovery"
        provenance_path = evidence_dir / "provenance.json"
        patch = evidence_dir / "feature.patch"
        if provenance_path.exists():
            saved = json.loads(provenance_path.read_text(encoding="utf-8"))
            for relative, initial in saved["source_manifest"].items():
                path = old / relative
                if (_sha256(path) if path.is_file() else None) != initial:
                    raise RuntimeError("recovery source changed after the prior import")
            return saved
        if patch.exists():
            raise RuntimeError("recovery import was interrupted; inspect the owned checkout")
        manifest: dict[str, str | None] = {}
        for relative in sorted(selected | set(recovery.get("preserve_paths", []))):
            path = old / relative
            manifest[relative] = _sha256(path) if path.is_file() else None
        evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        diff = subprocess.run(
            ["git", "-C", str(old), "diff", "HEAD", "--binary", "--", *sorted(selected)],
            capture_output=True,
            check=True,
        ).stdout
        patch.write_bytes(diff)
        os.chmod(patch, 0o600)
        applied = True
        error = None
        if diff:
            result = subprocess.run(
                ["git", "-C", str(self.checkout), "apply", "--3way", str(patch)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode:
                applied = False
                error = (result.stderr or result.stdout).strip()[:1000]
        untracked = set(_git(old, "ls-files", "--others", "--exclude-standard").splitlines())
        for relative in sorted(untracked & selected):
            source = old / relative
            target = self.checkout / relative
            if not source.is_file() or source.is_symlink() or target.exists():
                raise ValueError("recovery untracked file cannot be copied safely")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        for relative, initial in manifest.items():
            path = old / relative
            if (_sha256(path) if path.is_file() else None) != initial:
                raise RuntimeError("recovery source changed during import")
        provenance = {
            "source_head": _git(old, "rev-parse", "HEAD"),
            "source_manifest": manifest,
            "patch_sha256": _sha256(patch),
            "patch_applied": applied,
            "patch_error": error,
            "copied_untracked": sorted(untracked & selected),
            "preserved_source": str(old),
        }
        provenance_path.write_text(
            json.dumps(provenance, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        os.chmod(provenance_path, 0o600)
        return provenance

    def candidate(self) -> dict[str, Any]:
        content = candidate_for(self.checkout)
        return {
            **content,
            "base_sha": self.spec["base_sha"],
            "policy_digest": self.spec["policy_digest"],
            "environment_digest": self._environment_digest(),
        }

    def _environment_digest(self) -> str:
        relevant = []
        for name in ("uv.lock", "pnpm-lock.yaml", "package.json", "pyproject.toml"):
            file = self.checkout / name
            if file.is_file():
                relevant.append((name, _sha256(file)))
        return hashlib.sha256(canonical_json(relevant).encode()).hexdigest()

    def _changed_paths(self) -> set[str]:
        changed = set(_git(self.checkout, "diff", "--name-only", "HEAD").splitlines())
        changed.update(
            _git(self.checkout, "ls-files", "--others", "--exclude-standard").splitlines()
        )
        return {item for item in changed if item}

    def gate_checkout(self, role: str, iteration: int, candidate: dict[str, Any]) -> Path:
        if role not in {"review", "verify"}:
            raise ValueError("only independent gates use gate checkouts")
        path = self.state_dir / "gates" / str(iteration) / role
        if path.exists():
            if _git(path, "rev-parse", "HEAD") != candidate["head"]:
                raise RuntimeError("gate checkout head changed")
        else:
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            _git(self.source, "worktree", "add", "--detach", str(path), candidate["head"])
        observed = candidate_for(path)
        if observed["id"] != candidate["id"]:
            raise RuntimeError("gate checkout does not match the published candidate")
        return path

    def gate_diff(self, role: str, iteration: int, candidate: dict[str, Any]) -> dict[str, str]:
        """Freeze the controller's base-to-head diff for a role without Git access."""

        if role not in {"review", "verify"}:
            raise ValueError("only independent gates receive a controller diff")
        checkout = self.state_dir / "gates" / str(iteration) / role
        if candidate_for(checkout)["id"] != candidate["id"]:
            raise ValueError("gate checkout changed before diff production")
        base = self.spec["base_sha"]
        head = candidate["head"]
        command = [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(checkout),
            "diff",
            "--binary",
            "--no-ext-diff",
            "--no-textconv",
            base,
            head,
            "--",
        ]
        result = subprocess.run(command, capture_output=True, check=False, timeout=120)
        if result.returncode or not result.stdout:
            raise RuntimeError("controller could not produce a nonempty bound candidate diff")
        folder = self.state_dir / "gate-evidence" / str(iteration) / role
        folder.mkdir(parents=True, mode=0o700, exist_ok=True)
        folder_info = folder.lstat()
        if (
            not stat.S_ISDIR(folder_info.st_mode)
            or stat.S_IMODE(folder_info.st_mode) != 0o700
            or folder_info.st_uid != os.getuid()
        ):
            raise RuntimeError("controller diff directory is not private and owned")
        path = folder / f"{candidate['id']}.patch"
        if path.exists() or path.is_symlink():
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()
                or path.read_bytes() != result.stdout
            ):
                raise RuntimeError("controller candidate diff changed across attempts")
        else:
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(result.stdout)
        return {
            "path": str(path),
            "sha256": hashlib.sha256(result.stdout).hexdigest(),
            "base_sha": base,
            "head": head,
            "candidate_id": candidate["id"],
        }

    def git_metadata(self, candidate: dict[str, Any]) -> Path:
        """Freeze an owned Git index for contained, read-only candidate inspection."""

        candidate_id = candidate["id"]
        if not re.fullmatch(r"[a-f0-9]{64}", candidate_id):
            raise ValueError("candidate Git metadata needs an exact digest")
        root = self.state_dir / "git-metadata"
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_uid != os.getuid()
        ):
            raise ValueError("candidate Git metadata root is not private")
        metadata = root / f"{candidate_id}.git"
        staging = root / f".{candidate_id}.staging"
        lock = root / f"{candidate_id}.lock"
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "rb") as stream:
            lock_info = lock.lstat()
            if (
                not stat.S_ISREG(lock_info.st_mode)
                or stat.S_IMODE(lock_info.st_mode) != 0o600
                or lock_info.st_uid != os.getuid()
            ):
                raise RuntimeError("candidate Git metadata lock is not private")
            fcntl.flock(stream, fcntl.LOCK_EX)
            if not metadata.exists() and not metadata.is_symlink():
                # Staging is never mounted. An interruption here occurred
                # before any check or browser command could start.
                if staging.exists() or staging.is_symlink():
                    stage_info = staging.lstat()
                    if not stat.S_ISDIR(stage_info.st_mode) or stage_info.st_uid != os.getuid():
                        raise RuntimeError("candidate Git metadata staging is not owned")
                    shutil.rmtree(staging)
                command = [
                    "git",
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "core.fsmonitor=false",
                    "clone",
                    "--bare",
                    "--no-local",
                    "--quiet",
                    str(self.source),
                    str(staging),
                ]
                _run(command, timeout=180)
                _run(
                    [
                        "git",
                        f"--git-dir={staging}",
                        "fetch",
                        "--no-tags",
                        "--quiet",
                        str(self.checkout),
                        candidate["head"],
                    ],
                    timeout=180,
                )
                for key, value in (
                    ("core.bare", "false"),
                    ("core.worktree", "/work"),
                    ("core.hooksPath", "/dev/null"),
                    ("core.fsmonitor", "false"),
                ):
                    _run(["git", f"--git-dir={staging}", "config", key, value])
                subprocess.run(
                    ["git", f"--git-dir={staging}", "config", "--remove-section", "remote.origin"],
                    capture_output=True,
                    check=False,
                    timeout=30,
                )
                _run(
                    [
                        "git",
                        f"--git-dir={staging}",
                        "update-ref",
                        "refs/heads/devflow-candidate",
                        candidate["head"],
                    ]
                )
                _run(
                    [
                        "git",
                        f"--git-dir={staging}",
                        "symbolic-ref",
                        "HEAD",
                        "refs/heads/devflow-candidate",
                    ]
                )
                _run(["git", f"--git-dir={staging}", "read-tree", candidate["head"]])
                manifest = {
                    "candidate_id": candidate_id,
                    "head": candidate["head"],
                    "config_sha256": _sha256(staging / "config"),
                    "index_sha256": _sha256(staging / "index"),
                    "head_sha256": _sha256(staging / "HEAD"),
                }
                receipt = staging / "devflow-manifest.json"
                receipt_descriptor = os.open(receipt, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(receipt_descriptor, "w", encoding="utf-8") as saved:
                    saved.write(canonical_json(manifest) + "\n")
                os.chmod(staging, 0o700)
                staging.rename(metadata)
            receipt = metadata / "devflow-manifest.json"
            if metadata.is_symlink() or not metadata.is_dir() or not receipt.is_file():
                raise RuntimeError("candidate Git metadata preparation is incomplete")
            recorded = json.loads(receipt.read_text(encoding="utf-8"))
            if (
                recorded.get("candidate_id") != candidate_id
                or recorded.get("head") != candidate["head"]
                or recorded.get("config_sha256") != _sha256(metadata / "config")
                or recorded.get("index_sha256") != _sha256(metadata / "index")
                or recorded.get("head_sha256") != _sha256(metadata / "HEAD")
                or _run(["git", f"--git-dir={metadata}", "rev-parse", "HEAD"]) != candidate["head"]
            ):
                raise RuntimeError("candidate Git metadata changed after it was frozen")
            return metadata

    def _run_check_list(
        self,
        checkout: Path,
        checks: list[dict[str, Any]],
        evidence_dir: Path,
        candidate: dict[str, Any],
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        checkout = checkout.resolve(strict=True)
        evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        store_volume = self._ensure_dependency_store() if self.spec["provider"] == "codex" else None
        git_metadata = self.git_metadata(candidate) if self.spec["provider"] == "codex" else None
        for check in checks:
            argv = check.get("argv")
            relative = check.get("cwd", ".")
            cwd = (checkout / relative).resolve(strict=True)
            if (
                not isinstance(argv, list)
                or not argv
                or any(not isinstance(item, str) or not item for item in argv)
                or checkout not in (cwd, *cwd.parents)
            ):
                raise ValueError("configured check command or cwd is invalid")
            if self.spec["provider"] == "codex":
                if check.get("network_domains"):
                    raise ValueError("container checks cannot request host network domains")
                container = OwnedContainer(
                    self.spec,
                    kind="check",
                    identity={
                        "candidate_id": candidate["id"],
                        "stage": evidence_dir.parent.name,
                        "iteration": evidence_dir.name,
                        "check_id": check["id"],
                    },
                    evidence_dir=evidence_dir / check["id"] / "container",
                    binds=(
                        Bind(checkout, "/work"),
                        Bind(checkout / ".git", "/work/.git", True),
                        Bind(git_metadata, "/gitmeta", True),
                    ),
                    command=(
                        "/usr/bin/python3",
                        "/opt/devflow/landlock_exec.py",
                        "--",
                        *argv,
                    ),
                    cwd="/work"
                    if cwd == checkout
                    else "/work/" + cwd.relative_to(checkout).as_posix(),
                    environment={
                        "HOME": "/tmp",
                        "PATH": "/usr/local/bin:/usr/bin:/bin",
                        "COREPACK_HOME": "/usr/local/share/corepack",
                        "COREPACK_ENABLE_NETWORK": "0",
                        "PNPM_STORE_DIR": "/store",
                        "npm_config_nodedir": "/usr",
                        "CI": "1",
                        "GIT_CONFIG_NOSYSTEM": "1",
                        "GIT_CONFIG_GLOBAL": "/dev/null",
                        "GIT_CONFIG_COUNT": "1",
                        "GIT_CONFIG_KEY_0": "core.hooksPath",
                        "GIT_CONFIG_VALUE_0": "/dev/null",
                        "GIT_DIR": "/gitmeta",
                        "GIT_WORK_TREE": "/work",
                        "GIT_OPTIONAL_LOCKS": "0",
                    },
                    network="none",
                    timeout_seconds=int(check.get("timeout_seconds", 600)),
                    volume_mounts=((store_volume, "/store", True),),
                )
                outcome = container.run()
                output = outcome.log.read_text(encoding="utf-8", errors="replace")
                exit_code = outcome.exit_code
                artifact = outcome.log
            elif self.spec["provider"] == "fake":
                # Explicit fixture provider only; real deliveries never take
                # this unsandboxed path.
                check_env = {"PATH": os.environ.get("PATH", ""), "CI": "1"}
                command = argv
                checked = subprocess.run(
                    command,
                    cwd=cwd,
                    env=check_env,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=int(check.get("timeout_seconds", 600)),
                )
                output = checked.stdout + checked.stderr
                exit_code = checked.returncode
                artifact = evidence_dir / f"{check['id']}.log"
                artifact.write_text(output, encoding="utf-8")
                os.chmod(artifact, 0o600)
            else:
                raise ValueError("unknown delivery provider")
            parsed_output = visible_output(output)
            count = None
            if check.get("test_count_regex"):
                count = observed_test_count(output, check["test_count_regex"])
            rejected_output = False
            if check.get("reject_regex"):
                rejected_output = re.search(check["reject_regex"], parsed_output) is not None
            passed = (
                exit_code == 0
                and (count is None or count >= int(check.get("min_tests", 1)))
                and not rejected_output
            )
            results.append(
                {
                    "id": check["id"],
                    "argv": argv,
                    "cwd": str(cwd),
                    "exit_code": exit_code,
                    "test_count": count,
                    "rejected_output": rejected_output,
                    "passed": passed,
                    "diagnostic": parsed_output[-2000:] if not passed else None,
                    "log": str(artifact),
                    "log_sha256": _sha256(artifact),
                    "container_id": outcome.container_id
                    if self.spec["provider"] == "codex"
                    else None,
                    "cleanup": outcome.cleanup if self.spec["provider"] == "codex" else "confirmed",
                }
            )
            if not passed:
                break
        after = candidate_for(checkout)
        source_unchanged = after["id"] == candidate["id"]
        return {
            "state": "passed"
            if results and all(item["passed"] for item in results) and source_unchanged
            else "failed",
            "results": results,
            "candidate_id": candidate["id"],
            "source_unchanged": source_unchanged,
        }

    def _ensure_dependency_store(self) -> str:
        """Fetch only lockfile metadata/tarballs, without any candidate script."""

        container_policy = self.spec["policy"].get("container")
        if not isinstance(container_policy, dict):
            raise ValueError("real check requires an admitted container policy")
        lock = self.checkout / "pnpm-lock.yaml"
        if (
            lock.is_symlink()
            or not lock.is_file()
            or _sha256(lock) != container_policy["pnpm_lock_sha256"]
        ):
            raise ValueError("admitted package lock changed before dependency preparation")
        volume = dependency_volume(self.spec, container_policy["pnpm_lock_sha256"])
        folder = self.state_dir / "dependency-preparation"
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        if (
            folder.is_symlink()
            or folder.stat().st_uid != os.getuid()
            or stat.S_IMODE(folder.stat().st_mode) != 0o700
        ):
            raise ValueError("dependency preparation evidence is not private")
        scratch = folder / "scratch"
        scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
        if scratch.is_symlink() or scratch.stat().st_uid != os.getuid():
            raise ValueError("dependency preparation scratch is not owned")
        frozen_lock = scratch / "pnpm-lock.yaml"
        if frozen_lock.exists() or frozen_lock.is_symlink():
            if (
                frozen_lock.is_symlink()
                or not frozen_lock.is_file()
                or frozen_lock.stat().st_uid != os.getuid()
                or _sha256(frozen_lock) != container_policy["pnpm_lock_sha256"]
            ):
                raise ValueError("dependency preparation lock changed")
        else:
            descriptor = os.open(frozen_lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(lock.read_bytes())
        prepared = OwnedContainer(
            self.spec,
            kind="dependency-preparation",
            identity={"lock_sha256": container_policy["pnpm_lock_sha256"]},
            evidence_dir=folder,
            binds=(Bind(scratch, "/deps"),),
            command=(
                "corepack",
                "pnpm",
                "fetch",
                "--frozen-lockfile",
                "--ignore-scripts",
                "--ignore-pnpmfile",
                "--store-dir",
                "/store",
            ),
            cwd="/deps",
            environment={
                "HOME": "/tmp",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "COREPACK_HOME": "/usr/local/share/corepack",
                "COREPACK_ENABLE_NETWORK": "0",
            },
            network="bridge",
            timeout_seconds=int(container_policy.get("prefetch_timeout_seconds", 1800)),
            volume_mounts=((volume, "/store", False),),
        ).run()
        if prepared.exit_code:
            raise RuntimeError("credential-free lockfile dependency fetch failed")
        if _sha256(frozen_lock) != container_policy["pnpm_lock_sha256"]:
            raise ValueError("dependency preparation changed the admitted lock")
        return volume

    def run_prechecks(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        if self.candidate() != candidate:
            raise ValueError("prepublication candidate is stale")
        return self._run_check_list(
            self.checkout,
            self.spec["policy"].get("prepublish_checks", []),
            self.state_dir / "prechecks" / str(iteration),
            candidate,
        )

    def run_checks(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        checkout = self.gate_checkout("verify", iteration, candidate)
        return self._run_check_list(
            checkout,
            self.spec["policy"].get("checks", []),
            self.state_dir / "checks" / str(iteration),
            candidate,
        )

    def run_browser_qa(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        return execute_browser_qa(self, iteration, candidate)

    def _existing_pr(self) -> dict[str, Any] | None:
        try:
            output = _run(
                [
                    "gh",
                    "pr",
                    "list",
                    "--repo",
                    self.spec["github_repo"],
                    "--state",
                    "all",
                    "--head",
                    self.spec["branch"],
                    "--json",
                    "number,url,state,isDraft,baseRefName,headRefName,headRefOid",
                ],
                timeout=60,
            )
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
            raise BrokerReadbackUnavailable("owned branch PR readback unavailable") from exc
        matches = json.loads(output)
        if len(matches) > 1:
            raise RuntimeError("multiple PRs use the owned branch")
        if not matches:
            return None
        found = matches[0]
        if (
            found["headRefName"] != self.spec["branch"]
            or found["baseRefName"] != self.spec["base_ref"].removeprefix("origin/")
            or found["isDraft"]
            or found["state"] != "OPEN"
        ):
            raise RuntimeError("owned branch PR is not the authorized open regular PR")
        return found

    def publish(self, iteration: int, input_candidate: dict[str, Any]) -> dict[str, Any]:
        key = f"publish:{self.spec['run_id']}:{iteration}"
        before = self.candidate()
        request = {"iteration": iteration, "input_candidate_id": input_candidate["id"]}
        done = self._effect(key, "publish", request)
        if done:
            found = self._existing_pr()
            if found is None or found["headRefOid"] != done["head"]:
                return {"state": "pending", "reason": "pr_head_readback", "head": done["head"]}
            return done
        if before["id"] != input_candidate["id"] and self._changed_paths():
            raise RuntimeError("candidate changed during publication recovery")
        changed = self._changed_paths()
        allowed = set(self.spec["policy"].get("allowed_paths", []))
        if changed - allowed:
            raise ValueError(
                "candidate changed outside allowed paths: " + ", ".join(sorted(changed - allowed))
            )
        existing = self._existing_pr()
        if changed:
            for relative in sorted(changed):
                attribute = _git(self.checkout, "check-attr", "filter", "--", relative)
                if not attribute.endswith(": filter: unspecified") and not attribute.endswith(
                    ": filter: unset"
                ):
                    raise ValueError("candidate path would invoke a Git clean filter")
            _git(self.checkout, "add", "--", *sorted(changed))
            if _git(self.checkout, "diff", "--cached", "--name-only"):
                _git(
                    self.checkout,
                    "-c",
                    "core.hooksPath=/dev/null",
                    "commit",
                    "-m",
                    f"Implement {self.spec['goal'].splitlines()[0][:65]}",
                )
        head = _git(self.checkout, "rev-parse", "HEAD")
        if head == self.spec["base_sha"]:
            raise ValueError("no meaningful commit is available for publication")
        remote = _git(self.source, "ls-remote", "origin", f"refs/heads/{self.spec['branch']}")
        if _git(self.checkout, "remote", "get-url", "--push", "origin") != self.spec["origin_url"]:
            raise RuntimeError("Git push destination changed from the admitted origin")
        remote_head = remote.split()[0] if remote else None
        if remote_head != head:
            if remote_head:
                ancestry = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(self.checkout),
                        "merge-base",
                        "--is-ancestor",
                        remote_head,
                        head,
                    ],
                    check=False,
                )
                if ancestry.returncode != 0:
                    raise RuntimeError("remote feature branch diverged")
            _git(self.checkout, "push", "origin", f"HEAD:refs/heads/{self.spec['branch']}")
        if existing is None:
            title = self.spec["goal"].splitlines()[0][:100]
            body = self.state_dir / "pull-request.md"
            body.write_text(
                self.spec["policy"].get("pr_body")
                or (
                    f"{title}\n\n"
                    f"Implements issue {self.spec['issue_url']} under the accepted local plan. "
                    "This PR is published for review and remains unmerged.\n"
                ),
                encoding="utf-8",
            )
            os.chmod(body, 0o600)
            _run(
                [
                    "gh",
                    "pr",
                    "create",
                    "--repo",
                    self.spec["github_repo"],
                    "--head",
                    self.spec["branch"],
                    "--base",
                    self.spec["base_ref"].removeprefix("origin/"),
                    "--title",
                    title,
                    "--body-file",
                    str(body),
                ],
                cwd=self.checkout,
                timeout=90,
            )
        # A successful push can precede GitHub's PR-head projection. The
        # durable effect remains pending until a separate read-only
        # reconciliation proves all three heads equal.
        for delay in (0, 1, 2, 4, 8):
            if delay:
                time.sleep(delay)
            found = self._existing_pr()
            if found is not None and found["headRefOid"] == head:
                break
        else:
            return {"state": "pending", "reason": "pr_head_readback", "head": head}
        result = {
            "number": found["number"],
            "url": found["url"],
            "state": found["state"],
            "head": head,
            "base": self.spec["base_sha"],
            "candidate": self.candidate(),
        }
        self._finish_effect(key, result)
        return result

    def reconcile_publish(
        self,
        iteration: int,
        input_candidate: dict[str, Any],
        *,
        expected_head: str | None = None,
        expected_pr_number: int | None = None,
        complete: bool = True,
    ) -> dict[str, Any]:
        """Complete only an existing publish effect after external readback.

        This path cannot commit, push, or create a PR. It is safe after a
        successful push followed by a stale GitHub PR projection.
        """
        key = f"publish:{self.spec['run_id']}:{iteration}"
        request = {"iteration": iteration, "input_candidate_id": input_candidate["id"]}
        with self.store._connect() as db:
            saved = db.execute(
                "SELECT kind,request_json,state,observed_json "
                "FROM delivery_effects WHERE effect_key=?",
                (key,),
            ).fetchone()
        if (
            saved is None
            or saved["kind"] != "publish"
            or saved["request_json"] != canonical_json(request)
        ):
            raise ValueError("publication effect does not match this candidate")
        current = self.candidate()
        head = current["head"]
        if expected_head is not None and head != expected_head:
            raise ValueError("published checkout head changed")
        if any(
            current.get(field) != input_candidate.get(field)
            for field in ("content_sha256", "base_sha", "policy_digest", "environment_digest")
        ):
            raise ValueError("published candidate content or authority changed")
        if self._changed_paths():
            raise ValueError("published checkout has uncommitted changes")
        if (
            head != input_candidate["head"]
            and _git(self.checkout, "rev-parse", "HEAD^") != input_candidate["head"]
        ):
            raise ValueError("published commit does not descend directly from checked candidate")
        if _git(self.checkout, "remote", "get-url", "--push", "origin") != self.spec["origin_url"]:
            raise ValueError("published Git destination changed")
        remote = _git(self.source, "ls-remote", "origin", f"refs/heads/{self.spec['branch']}")
        if not remote or remote.split()[0] != head:
            raise ValueError("remote feature branch differs from the published checkout")
        found = self._existing_pr()
        if found is None or found["headRefOid"] != head:
            return {"state": "pending", "reason": "pr_head_readback", "head": head}
        if expected_pr_number is not None and found["number"] != expected_pr_number:
            raise ValueError("publication resolved to a different PR")
        if saved["state"] == "complete":
            done = json.loads(saved["observed_json"])
            expected = {
                "number": found["number"],
                "url": found["url"],
                "state": found["state"],
                "head": head,
                "base": self.spec["base_sha"],
                "candidate": current,
            }
            if done != expected:
                raise ValueError("durable publication receipt disagrees with current PR")
            return done
        if saved["state"] != "pending":
            raise ValueError("publication effect is not recoverable")
        result = {
            "number": found["number"],
            "url": found["url"],
            "state": found["state"],
            "head": head,
            "base": self.spec["base_sha"],
            "candidate": current,
        }
        if complete:
            self._finish_effect(key, result)
        return result

    async def checks(self, pr: dict[str, Any], *, timeout_seconds: int = 1200) -> dict[str, Any]:
        required = set(self.spec["policy"].get("required_ci", []))
        if not required:
            return {"state": "unverified", "reason": "no required CI checks configured"}
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            found = json.loads(
                _run(
                    [
                        "gh",
                        "pr",
                        "view",
                        str(pr["number"]),
                        "--repo",
                        self.spec["github_repo"],
                        "--json",
                        "headRefOid,statusCheckRollup",
                    ],
                    timeout=60,
                )
            )
            if found["headRefOid"] != pr["head"]:
                return {"state": "stale", "reason": "PR head changed while checks were pending"}
            checks = {
                item["name"]: item
                for item in found.get("statusCheckRollup", [])
                if item.get("__typename") == "CheckRun"
            }
            failed = [
                name
                for name in required
                if checks.get(name, {}).get("conclusion") in {"FAILURE", "CANCELLED", "TIMED_OUT"}
            ]
            if failed:
                return {"state": "failed", "failed": failed, "checks": checks}
            if all(checks.get(name, {}).get("conclusion") == "SUCCESS" for name in required):
                return {"state": "passed", "checks": checks, "head": pr["head"]}
            if asyncio.get_running_loop().time() >= deadline:
                return {"state": "pending", "checks": checks, "reason": "CI deadline reached"}
            await asyncio.sleep(15)
