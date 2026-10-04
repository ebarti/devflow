"""Validated Git, GitHub and check effects for an admitted delivery."""

from __future__ import annotations

import asyncio
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
from .delivery_continuation import copy_session_state, selected_digest, session_state_digest
from .delivery_output import observed_test_count, rejection_causes, visible_output
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


def conventional_subject(goal: str) -> str:
    """Keep an admitted conventional subject, otherwise use a neutral type."""
    subject = goal.splitlines()[0].strip()
    if not subject or any(ord(character) < 32 for character in subject):
        raise ValueError("publication subject is empty or contains control characters")
    if not _conventional_subject(subject):
        subject = "chore: " + subject
    return subject


def _conventional_subject(subject: str) -> bool:
    return bool(re.fullmatch(r"[a-z][a-z0-9-]*(?:\([^()\r\n]+\))?!?: \S.*", subject))


class BrokerReadbackUnavailable(RuntimeError):
    """A remote PR query failed before its authority could be inspected."""


class DeliveryBroker:
    def __init__(self, store: DeliveryStore, spec: dict[str, Any]) -> None:
        from .delivery_preparation import require_native_execution

        require_native_execution(spec)
        self.store = store
        self.spec = spec
        self.source = Path(spec["source_path"])
        self.checkout = Path(spec["checkout"])
        self.state_dir = Path(spec["state_dir"])
        from .delivery_resources import _gate_evidence_root

        self.evidence_dir = _gate_evidence_root(spec)
        self.effect_namespace = ""

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
        resources = None
        if self.spec.get("resource_cleanup_version") == 1:
            from .delivery_resources import RunResources

            resources = RunResources(self.spec)
            resources.register(self.checkout, "checkout")
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
        if resources:
            resources.created(self.checkout)
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

    def _changed_paths(self, base_ref: str = "HEAD") -> set[str]:
        changed = set(_git(self.checkout, "diff", "--name-only", base_ref).splitlines())
        changed.update(
            _git(self.checkout, "ls-files", "--others", "--exclude-standard").splitlines()
        )
        return {item for item in changed if item}

    def gate_checkout(self, role: str, iteration: int, candidate: dict[str, Any]) -> Path:
        if role not in {"review", "verify"}:
            raise ValueError("only independent gates use gate checkouts")
        if self.spec["policy"].get("execution_backend") == "native-macos":
            from .delivery_native_guard import validate_native_turn

            validate_native_turn(self.spec, role, iteration, self.store)
        current = self.candidate()
        if any(candidate.get(key) != current[key] for key in ('id', 'head')):
            raise RuntimeError('gate candidate no longer matches its owned source')
        path = self._gate_path(role, iteration)
        from .delivery_resources import private_directory

        private_directory(path.parent)
        resources = None
        if self.spec.get("resource_cleanup_version") == 1:
            from .delivery_resources import RunResources

            resources = RunResources(self.spec)
            resources.register(path, "gate")
        if path.exists():
            if _git(path, "rev-parse", "HEAD") != candidate["head"]:
                raise RuntimeError("gate checkout head changed")
        else:
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            _git(self.source, "worktree", "add", "--detach", str(path), candidate["head"])
        if resources:
            resources.created(path)
        observed = candidate_for(path)
        if observed["id"] != candidate["id"]:
            raise RuntimeError("gate checkout does not match the published candidate")
        return path

    def _gate_path(self, role: str, iteration: int) -> Path:
        from .delivery_resources import _gate_evidence_root, _gate_path

        if self.evidence_dir != _gate_evidence_root(self.spec):
            raise ValueError('broker gate namespace differs from durable admission')
        return _gate_path(self.spec, role, iteration)

    def gate_diff(self, role: str, iteration: int, candidate: dict[str, Any]) -> dict[str, str]:
        """Freeze the controller's base-to-head diff for a role without Git access."""

        if role not in {"review", "verify"}:
            raise ValueError("only independent gates receive a controller diff")
        checkout = self._gate_path(role, iteration)
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
        folder = self.evidence_dir / "gate-evidence" / str(iteration) / role
        from .delivery_resources import private_directory

        private_directory(folder)
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
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec)
        native_dependencies = (
            self._ensure_native_dependency_store(checkout)
            if self.spec["provider"] == "codex"
            and any("/store" in check["argv"] for check in checks)
            else None
        )
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
            native_result = None
            check_evidence = None
            if self.spec["provider"] == "codex":
                from .delivery_native_process import NativeProcess
                from .delivery_preparation import verify_prepared_spec
                from .delivery_sandbox import native_check_argv, prepare_native_check

                verify_prepared_spec(self.spec)
                profile, environment = prepare_native_check(
                    self.spec, checkout, evidence_dir, check,
                    dependency_store=(
                        Path(native_dependencies["store"]) if native_dependencies else None
                    ),
                )
                generated = self._register_generated(checkout, ["node_modules"])
                # The same frozen lock populates an owned store before offline
                # installation. Candidate commands may read, never mutate it.
                command = [
                    native_dependencies["store"] if item == "/store" else item for item in argv
                ]
                native_result = NativeProcess(
                    self.spec,
                    evidence_dir / check["id"] / "native",
                    argv=native_check_argv(self.spec, profile, cwd, command),
                    cwd=cwd,
                    environment=environment,
                    timeout=int(check.get("timeout_seconds", 600)),
                    cancelled=self._native_cancelled,
                ).run()
                self._record_generated(generated)
                if native_result["cleanup"] == "unknown":
                    return {
                        "state": "unknown",
                        "cleanup": "unknown",
                        "candidate_id": candidate["id"],
                        "native_process": native_result,
                    }
                artifact = Path(native_result["log"])
                output = artifact.read_text(encoding="utf-8", errors="replace")
                exit_code = native_result["exit_code"]
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
            evidence_failure = None
            if (self.spec["policy"].get("host_sandbox") == "trusted-local"
                    and check.get("kind") == "test"):
                from .delivery_check_evidence import retain_artifacts

                try:
                    check_evidence = retain_artifacts(evidence_dir / check["id"], candidate)
                except (ValueError, OSError) as exc:
                    evidence_failure = str(exc)
            parsed_output = visible_output(output)
            count = None
            if check.get("test_count_regex"):
                count = observed_test_count(output, check["test_count_regex"])
            rejected_causes = rejection_causes(
                parsed_output, [check["reject_regex"]] if check.get("reject_regex") else [],
                test_results=check.get("kind") == "test",
            )
            rejected_output = bool(rejected_causes)
            passed = (
                exit_code == 0
                and evidence_failure is None
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
                    "rejection_causes": rejected_causes,
                    "passed": passed,
                    "diagnostic": parsed_output[-2000:] if not passed else None,
                    "log": str(artifact),
                    "log_sha256": _sha256(artifact),
                    **({"artifacts": check_evidence} if check_evidence else {}),
                    **({"evidence_failure": evidence_failure} if evidence_failure else {}),
                    "cleanup": "confirmed",
                    **(
                        {
                            "process_cleanup": native_result["cleanup"],
                            "native_process": native_result,
                            "dependency_preparation": native_dependencies,
                        }
                        if native_result
                        else {}
                    ),
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

    def _native_cancelled(self) -> bool:
        with self.store._connect() as db:
            row = db.execute(
                "SELECT phase FROM delivery_runs WHERE run_id=?", (self.spec["run_id"],)
            ).fetchone()
        return row is not None and row["phase"] == "cancelling"

    def _register_generated(self, checkout: Path, names: list[str]) -> list[Path]:
        from .delivery_resources import RunResources

        resources = RunResources(self.spec)
        roots = []
        for name in names:
            root = checkout / name
            if _git(checkout, "ls-files", "--", name):
                raise ValueError("configured generated directory contains tracked source")
            if not root.parent.is_dir() or root.parent.resolve() != root.parent:
                raise ValueError("generated directory parent is not a fixed candidate directory")
            resources.register(root, "generated")
            roots.append(root)
        return roots

    def _record_generated(self, roots: list[Path]) -> None:
        from .delivery_resources import RunResources

        resources = RunResources(self.spec)
        for root in roots:
            if os.path.lexists(root):
                resources.created(root)

    def _ensure_native_dependency_store(self, checkout: Path) -> dict[str, Any]:
        """Fetch frozen registry data without exposing candidate setup or credentials."""
        from .delivery_native_dependencies import REGISTRY, frozen_pnpm_inputs, write_frozen_inputs
        from .delivery_native_process import NativeProcess
        from .delivery_preparation import verify_prepared_spec
        from .delivery_resources import RunResources, private_directory, read_private, write_private
        from .delivery_sandbox import native_check_argv, prepare_native_check

        verify_prepared_spec(self.spec)
        provenance = {}
        manager, inputs = frozen_pnpm_inputs(self.spec, checkout, provenance=provenance)
        resources = RunResources(self.spec)
        scratch = resources.scratch("dependencies", self.spec["policy_digest"])
        transient = read_private(resources.manifest)["roots"][str(self.state_dir / "transient")]
        generation = transient.get("generation", 0)
        folder = self.evidence_dir / "dependency-preparation" / f"native-{generation}"
        staging = scratch / "staging"
        dependencies = staging / "store"
        for path in (folder, staging, dependencies):
            private_directory(path)
        hashes = write_frozen_inputs(staging, inputs)
        receipt = folder / "receipt.json"
        identity = {"device": dependencies.stat().st_dev, "inode": dependencies.stat().st_ino}
        request = {
            "base_sha": self.spec["base_sha"], "policy_digest": self.spec["policy_digest"],
            "package_manager": manager, "input_hashes": hashes, "registry": REGISTRY,
            "input_provenance": provenance,
            "store": str(dependencies), "store_identity": identity, "generation": generation,
        }
        if receipt.exists():
            observed = read_private(receipt)
            if observed["request"] != request or observed["state"] != "passed":
                raise ValueError("native frozen dependency preparation conflicts with its receipt")
            observed.update(receipt=str(receipt), receipt_sha256=_sha256(receipt))
            return observed
        profile, environment = prepare_native_check(
            self.spec, staging, folder,
            {"id": "pnpm-fetch", "network_domains": [REGISTRY]},
        )
        environment.update({
            "COREPACK_ENABLE_NETWORK": "0", "npm_config_registry": "https://" + REGISTRY + "/",
        })
        process = NativeProcess(
            self.spec, folder / "process",
            argv=native_check_argv(self.spec, profile, staging, [
                "corepack", manager, "fetch", "--frozen-lockfile", "--ignore-scripts",
                "--ignore-pnpmfile", "--store-dir", str(dependencies),
            ]),
            cwd=staging, environment=environment, timeout=1800, cancelled=self._native_cancelled,
        ).run()
        unchanged = hashes == write_frozen_inputs(staging, inputs)
        passed = (
            process["exit_code"] == 0
            and process["cleanup"] == "observed-native-confirmed" and unchanged
        )
        result = {
            **request, "request": request,
            "state": "passed" if passed else "failed",
            "native_process": process, "log": process["log"],
            "log_sha256": _sha256(Path(process["log"])),
            "candidate_setup_executed": False,
            "network_authority": ("trusted-local full host network"
                                  if self.spec["policy"].get("host_sandbox") == "trusted-local"
                                  else "registry.npmjs.org fetch only; "
                                       "candidate checks remain offline"
                                  ),
        }
        write_private(receipt, result)
        result.update(receipt=str(receipt), receipt_sha256=_sha256(receipt))
        if result["state"] != "passed":
            diagnostic = visible_output(Path(process["log"]).read_text(errors="replace"))[-1200:]
            raise RuntimeError("credential-free native frozen lockfile fetch failed: " + diagnostic)
        return result


    def run_prechecks(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        if self.spec["policy"].get("execution_backend") == "native-macos":
            from .delivery_native_guard import validate_native_turn

            validate_native_turn(self.spec, "implement", iteration, self.store)
        if self.candidate() != candidate:
            raise ValueError("prepublication candidate is stale")
        return self._run_check_list(
            self.checkout,
            self.spec["policy"].get("prepublish_checks", []),
            self.evidence_dir / "prechecks" / str(iteration),
            candidate,
        )

    def run_checks(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        checkout = self.gate_checkout("verify", iteration, candidate)
        return self._run_check_list(
            checkout,
            self.spec["policy"].get("checks", []),
            self.evidence_dir / "checks" / str(iteration),
            candidate,
        )

    def run_browser_qa(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        return execute_browser_qa(self, iteration, candidate)

    def _existing_pr(self, *, validate_metadata: bool = True) -> dict[str, Any] | None:
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
                    "number,url,state,isDraft,baseRefName,headRefName,headRefOid,title",
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
        if validate_metadata and not _conventional_subject(found.get("title", "")):
            raise ValueError("owned PR title does not satisfy Conventional Commits")
        return found

    def _validate_publication_commits(self) -> None:
        """Check every owned commit; a valid head cannot mask invalid ancestors."""
        _git(self.checkout, "merge-base", "--is-ancestor", self.spec["base_sha"], "HEAD")
        commits = _git(
            self.checkout, "rev-list", "--reverse", self.spec["base_sha"] + "..HEAD"
        ).splitlines()
        for commit in commits:
            subject = _git(self.checkout, "show", "-s", "--format=%s", commit)
            if not _conventional_subject(subject):
                raise ValueError("owned commit is not a Conventional Commit: " + commit)
            author = _git(self.checkout, "show", "-s", "--format=%an <%ae>", commit)
            signers = _git(
                self.checkout, "show", "-s",
                "--format=%(trailers:key=Signed-off-by,valueonly)", commit,
            ).splitlines()
            if author not in signers:
                raise ValueError("owned commit lacks its author Signed-off-by trailer: " + commit)

    def publish(self, iteration: int, input_candidate: dict[str, Any]) -> dict[str, Any]:
        self._validate_publication_commits()
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
            author = _git(self.checkout, "var", "GIT_AUTHOR_IDENT").rsplit(" ", 2)[0]
            committer = _git(self.checkout, "var", "GIT_COMMITTER_IDENT").rsplit(" ", 2)[0]
            if author != committer:
                raise ValueError("publication author and configured sign-off identity disagree")
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
                    "--signoff",
                    "-m",
                    conventional_subject(self.spec["goal"]),
                )
        self._validate_publication_commits()
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
            title = conventional_subject(self.spec["goal"])
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
        self._validate_publication_commits()
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
