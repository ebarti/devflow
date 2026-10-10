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
import sys
import time
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import canonical_json
from .delivery_browser_qa import run_browser_qa as execute_browser_qa
from .delivery_config import publication_base_ref
from .delivery_continuation import copy_session_state, selected_digest, session_state_digest
from .delivery_output import observed_test_count, rejection_causes, visible_output
from .delivery_publication import conventional, publication_summary
from .delivery_store import DeliveryStore, _now


def _run(argv: list[str], *, cwd: Path | None = None, timeout: int = 120) -> str:
    nul_output = argv[0] == 'git' and '-z' in argv
    result = subprocess.run(
        argv, cwd=cwd, text=not nul_output, capture_output=True, timeout=timeout, check=False
    )
    stdout = result.stdout.decode('utf-8', 'surrogateescape') if nul_output else result.stdout
    stderr = result.stderr.decode('utf-8', 'surrogateescape') if nul_output else result.stderr
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {argv[0]} {argv[1]}: "
            + (stderr.strip() or stdout.strip())[:500]
        )
    # Preserve exact filenames, including CR/LF and leading/trailing whitespace.
    return stdout if nul_output else stdout.strip()


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
    return conventional(subject)


def publication_subject(spec: dict[str, Any]) -> str:
    """Use frozen summary metadata; preserve old goals for historical specs."""
    if "publication_summary" in spec:
        return publication_summary(spec["goal"], spec["publication_summary"])
    return conventional_subject(spec["goal"])


def publication_title(goal: str) -> str:
    """Bound PR metadata without shortening the admitted commit subject or goal."""
    subject = conventional_subject(goal)
    if len(subject) <= 256:
        return subject
    title = subject[:253].rsplit(" ", 1)[0].rstrip() + "..."
    if not _conventional_subject(title):
        raise ValueError("publication subject prefix exceeds the PR title limit")
    return title


class BrokerReadbackUnavailable(RuntimeError):
    """A remote PR query failed before its authority could be inspected."""


class CheckPreparationFailure(ValueError):
    """Preparation failed before this check's NativeProcess could launch."""

    def __init__(self, check_id: str, cause: Exception, results: list | None = None):
        super().__init__(str(cause)[:500])
        self.results = [*(results or []), {
            'id': check_id, 'passed': False, 'cleanup': 'confirmed',
            'launched': False, 'failure_kind': 'preparation', 'diagnostic': str(self),
        }]


class CheckCancelledBeforeLaunch(RuntimeError):
    """Cancellation was observed before entering the next native process."""


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
        self.check_cancelled = lambda: False
        self.native_cleanup_confirmed = True
        self.publication_may_have_effect = True

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
        local_branch_name = self.spec.get("local_branch", self.spec["branch"])
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
            if _git(self.checkout, "branch", "--show-current") != local_branch_name:
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
                    f"refs/heads/{local_branch_name}",
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
                local_branch_name,
                str(self.checkout),
                self.spec["base_sha"],
            )
        if resources:
            resources.created(self.checkout)
        recovery = self.spec["policy"].get("recovery")
        provenance = self._recover(recovery) if recovery else None
        if self.spec.get("feature_worker", {}).get("previous_publication"):
            from .delivery_feature_activities import integrate_previous

            integrate_previous(self)
        elif self.spec.get("feature_worker", {}).get("seed"):
            from .delivery_feature_activities import apply_seed

            apply_seed(self)
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

    def _changed_paths(self, base_ref: str = "HEAD", *, include_index: bool = False) -> set[str]:
        changed = set(_git(
            self.checkout, "diff", "--no-renames", "--name-only", "-z", base_ref, "--"
        ).split('\0'))
        if include_index:
            changed.update(_git(
                self.checkout, 'diff', '--cached', '--no-renames', '--name-only', '-z',
                base_ref, '--'
            ).split('\0'))
        changed.update(
            _git(self.checkout, "ls-files", "--others", "--exclude-standard", "-z").split('\0')
        )
        return {item for item in changed if item}

    def validate_candidate_scope(self) -> set[str]:
        changed = self._changed_paths(self.spec['base_sha'], include_index=True)
        escaped = changed - set(self.spec['policy'].get('allowed_paths', []))
        if escaped:
            raise ValueError(
                'candidate changed outside allowed paths: ' + ', '.join(sorted(escaped))
            )
        return changed

    def admit_implementation(self, input_candidate: dict[str, Any]) -> dict[str, Any]:
        """Retain source/index, but leave every new commit to publication."""
        expected = input_candidate['head']
        try:
            _git(self.checkout, 'merge-base', '--is-ancestor', expected, 'HEAD')
        except RuntimeError as exc:
            raise ValueError('implementation commit ancestry changed during its role') from exc
        self.validate_candidate_scope()
        if _git(self.checkout, 'rev-parse', 'HEAD') != expected:
            _git(self.checkout, 'reset', '--soft', expected)
        return self.candidate()

    def is_imported_feature_candidate(self, candidate: dict[str, Any]) -> bool:
        """A completed chunk import may need validation without additional edits."""
        worker = self.spec.get("feature_worker", {})
        if (worker.get("kind") != "chunk"
                or not (worker.get("seed") or worker.get("previous_publication"))):
            return False
        with self.store._connect() as db:
            row = db.execute(
                "SELECT observed_json FROM delivery_effects "
                "WHERE effect_key=? AND run_id=? AND kind='prepare' AND state='complete'",
                ("prepare:" + self.spec["run_id"], self.spec["run_id"]),
            ).fetchone()
        return bool(row and row[0] and json.loads(row[0]).get("candidate") == candidate
                    and not _git(self.checkout, "ls-files", "-u"))

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
        *, python_environments: dict[str, Path] | None = None,
        native_projects: list[str] | None = None,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        checkout = checkout.resolve(strict=True)
        evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec)
        if self.spec['provider'] == 'codex' and any('/store' in c['argv'] for c in checks):
            from .delivery_native_dependencies import frozen_native_projects

            try:
                native_projects = sorted(set(native_projects or []) | set(
                    frozen_native_projects(self.spec, checkout)))
            except (ValueError, OSError, KeyError) as exc:
                raise CheckPreparationFailure('native-addon-authority', exc) from exc
        if self.spec['provider'] == 'codex' and native_projects:
            from .delivery_native_dependencies import native_addon_authority

            try:
                # Validate candidate setup before even the original ignored-script
                # install; that install must not load a candidate PNPM hook/config.
                authority = native_addon_authority(self.spec, checkout, native_projects)
                if authority and not any('/store' in c['argv'] for c in checks):
                    raise ValueError(
                        'native addon requires the frozen ignore-scripts install first'
                    )
            except (ValueError, OSError, KeyError) as exc:
                raise CheckPreparationFailure('native-addon-authority', exc) from exc
        native_dependencies = (
            self._ensure_native_dependency_store(checkout)
            if self.spec["provider"] == "codex"
            and any("/store" in check["argv"] for check in checks)
            else None
        )
        node_toolchain = None
        native_addon = None
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
            range_binding = None
            if check.get('plan_provenance', {}).get('recipe') == 'checks.diff':
                if argv != ['git', 'diff', '--check', 'origin/main...HEAD']:
                    raise ValueError(
                        'tracked checks.diff must retain its exact committed-range command'
                    )
                range_binding = self._diff_range_binding(checkout, candidate)
            native_result = None
            check_evidence = None
            if self.spec["provider"] == "codex":
                from .delivery_native_process import NativeProcess
                from .delivery_preparation import verify_prepared_spec
                from .delivery_sandbox import native_check_argv, prepare_native_check

                verify_prepared_spec(self.spec)
                try:
                    profile, environment = prepare_native_check(
                        self.spec, checkout, evidence_dir, check,
                        dependency_store=(
                            Path(native_dependencies["store"]) if native_dependencies else None
                        ),
                    )
                    if check.get('native_addon_prerequisite'):
                        environment['npm_config_python'] = str(Path(sys.executable).resolve())
                    isolated_python = (python_environments or {}).get(check['id'])
                    if isolated_python is not None:
                        if (not isolated_python.is_relative_to(
                                self.state_dir / 'transient/implementation-python')
                                or isolated_python.resolve() != isolated_python):
                            raise ValueError('locked Python environment left controller ownership')
                        environment['UV_PROJECT_ENVIRONMENT'] = str(isolated_python)
                    generated = self._register_generated(checkout,
                        [] if isolated_python is not None else
                        ["node_modules", *check.get("generated_directories", [])])
                except (ValueError, OSError) as exc:
                    raise CheckPreparationFailure(check['id'], exc, results) from exc
                # The same frozen lock populates an owned store before offline
                # installation. Candidate commands may read, never mutate it.
                command = [
                    native_dependencies["store"] if item == "/store" else item for item in argv
                ]
                native_result = self._run_native_check(NativeProcess(
                    self.spec,
                    evidence_dir / check["id"] / "native",
                    argv=native_check_argv(self.spec, profile, cwd, command),
                    cwd=cwd,
                    environment=environment,
                    timeout=int(check.get("timeout_seconds", 600)),
                    cancelled=self._native_cancelled,
                ))
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
                if check['id'] in (python_environments or {}):
                    check_env['UV_PROJECT_ENVIRONMENT'] = str(python_environments[check['id']])
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
            if range_binding and self._diff_range_binding(checkout, candidate) != range_binding:
                raise ValueError('tracked diff range changed during controller execution')
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
            junit = None
            if check.get("junit_required"):
                from .delivery_check_evidence import junit_counts

                try:
                    if not check_evidence:
                        raise ValueError('required owned JUnit report was not retained')
                    junit = junit_counts(check_evidence, candidate['id'], self.state_dir)
                    count = junit['passed']
                    if junit['failures'] or junit['errors']:
                        raise ValueError('required JUnit report records failed test cases')
                except (ValueError, OSError) as exc:
                    evidence_failure = str(exc)
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
                    **({"junit": junit} if junit is not None else {}),
                    "rejected_output": rejected_output,
                    "rejection_causes": rejected_causes,
                    "passed": passed,
                    "diagnostic": parsed_output[-2000:] if not passed else None,
                    "log": str(artifact),
                    "log_sha256": _sha256(artifact),
                    **({"plan_provenance": check["plan_provenance"]}
                       if check.get("plan_provenance") else {}),
                    **({"artifacts": check_evidence} if check_evidence else {}),
                    **({"evidence_failure": evidence_failure} if evidence_failure else {}),
                    "cleanup": "confirmed",
                    **({'range_binding': range_binding} if range_binding else {}),
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
            if (self.spec['provider'] == 'codex' and native_projects and '/store' in argv):
                if (argv[:3] != ['corepack', 'pnpm', 'install']
                        or not {'--offline', '--frozen-lockfile', '--ignore-scripts'} <= set(argv)):
                    raise ValueError(
                        'native addon requires the unchanged frozen ignore-scripts install'
                    )
                try:
                    addon = self._prepare_native_addon(checkout, native_projects,
                                                       evidence_dir, candidate, native_dependencies)
                except (ValueError, OSError, KeyError) as exc:
                    raise CheckPreparationFailure('native-addon-build', exc, results) from exc
                if addon:
                    results.extend(addon.get('results', []))
                    native_addon = addon
                    node_toolchain = addon.get('node_toolchain')
                    if addon['state'] != 'passed':
                        return {**addon, 'results': results}
                native_projects = None
        after = candidate_for(checkout)
        source_unchanged = after["id"] == candidate["id"]
        return {
            "state": "passed"
            if results and all(item["passed"] for item in results) and source_unchanged
            else "failed",
            "results": results,
            "candidate_id": candidate["id"],
            "source_unchanged": source_unchanged,
            **({'node_toolchain': node_toolchain} if node_toolchain else {}),
            **({'native_addon_preparation': native_addon} if native_addon else {}),
        }

    def _diff_range_binding(self, checkout: Path, candidate: dict) -> dict:
        observed = candidate_for(checkout)
        if observed["id"] != candidate["id"]:
            raise ValueError("tracked diff source candidate changed")
        return {
            "candidate_id": candidate["id"],
            "head": observed["head"],
            "content_sha256": observed["content_sha256"],
            "base_sha": self.spec["base_sha"],
            "origin_main_sha": _git(checkout, "rev-parse", "origin/main"),
            "merge_base_sha": _git(checkout, "merge-base", "origin/main", "HEAD"),
        }

    def _prepare_native_addon(
        self,
        checkout: Path,
        projects: list[str],
        evidence: Path,
        candidate: dict,
        dependencies: dict,
    ) -> dict | None:
        from .delivery_native_dependencies import (
            frozen_native_builder,
            native_addon_authority,
            validate_native_addon,
        )
        from .delivery_resources import RunResources, read_private, write_private
        from .delivery_sandbox import _native_addon_environment_version, _native_path

        authority = native_addon_authority(self.spec, checkout, projects)
        if authority is None:
            return None
        resources = RunResources(self.spec)
        owned = read_private(resources.manifest)
        # The original install owns this generated root; a historical/foreign tree
        # cannot be adopted merely because it contains the expected package name.
        root_record = owned["roots"].get(str(checkout / "node_modules"), {})
        if root_record.get("kind") != "generated" or not root_record.get("identity"):
            raise ValueError("native addon requires a controller-created dependency root")
        resources.register(checkout / "node_modules", "generated")
        package = validate_native_addon(checkout, authority, Path(dependencies["store"]))
        roots = [Path(p) for p in self.spec["policy"].get("toolchain_roots", [])]
        if not roots:
            raise ValueError("native addon requires the frozen Node22 toolchain root")
        toolchain = roots[0]
        receipt = evidence / "native-addon-preparation.json"
        environment_version = _native_addon_environment_version(self.spec, evidence)
        node, corepack = toolchain / "bin/node", toolchain / "bin/corepack"
        if (
            node.resolve(strict=True) != node
            or not node.is_file()
            or not corepack.resolve(strict=True).is_relative_to(toolchain)
            or node.stat().st_uid != os.getuid()
            or corepack.stat().st_uid != os.getuid()
        ):
            raise ValueError("native addon toolchain escaped its frozen root")
        tools = {
            "node_interpreter": {
                "absolute_path": str(node),
                "realpath": str(node),
                "sha256": _sha256(node),
            },
            "corepack": {
                "absolute_path": str(corepack),
                "realpath": str(corepack.resolve()),
                "sha256": _sha256(corepack),
            },
            "package_manager": authority["package_manager"],
            "environment": {
                "PATH": _native_path(tuple(roots), packaged_go=environment_version >= 3),
                "COREPACK_HOME": self.spec["policy"]["package_manager_cache"],
                "npm_config_nodedir": str(toolchain),
                "npm_config_build_from_source": "true",
                "npm_config_python": str(Path(sys.executable).resolve()),
            },
        }
        builder = frozen_native_builder(self.spec, authority["package_manager"])
        tools["native_builder"] = builder
        if receipt.exists():
            old = read_private(receipt)
            if old["candidate_id"] != candidate["id"] or old["native_addon_authority"] != authority:
                raise ValueError("native addon preparation receipt has stale source authority")
            recorded = old.get("node_toolchain", {})
            for key in ("node_interpreter", "corepack", "native_builder"):
                if recorded and any(recorded[key].get(k) != v for k, v in tools[key].items()):
                    raise ValueError("native addon preparation toolchain drifted across replay")
            if recorded and (recorded['environment'] != tools['environment']
                             or recorded['package_manager'] != tools['package_manager']):
                raise ValueError('native addon preparation environment changed across replay')
            if recorded:
                binary = package / "build/Release/better_sqlite3.node"
                if (
                    binary.resolve(strict=True) != binary
                    or _sha256(binary) != recorded["native_binding"]["sha256"]
                ):
                    raise ValueError("native addon binary changed across replay")
            for row in old["results"]:
                if (
                    _sha256(Path(row["log"])) != row["log_sha256"]
                    or self._reconcile_native_check(
                        Path(row["native_process"]["journal"]))["cleanup"]
                    != "observed-native-confirmed"
                ):
                    raise ValueError("native addon process/log readback changed across replay")
            return {**old, "receipt": str(receipt), "receipt_sha256": _sha256(receipt)}
        checks = [
            {
                "id": "native-addon-node-identity",
                "argv": [
                    str(node),
                    "-e",
                    "const a=require('node:assert/strict');"
                    "a.equal(process.versions.node.split('.')[0],'22');"
                    "console.log(JSON.stringify({version:process.version,modules_ABI:process.versions.modules,"
                    "architecture:process.arch,platform:process.platform,execPath:process.execPath}))",
                ],
                "timeout_seconds": 30,
            },
            {
                "id": "native-addon-build",
                "cwd": package.relative_to(checkout).as_posix(),
                "argv": [str(node), builder['absolute_path'], 'rebuild', '--release',
                         '--nodedir=' + str(toolchain),
                         '--python=' + str(Path(sys.executable).resolve())],
                "timeout_seconds": 600,
                "native_addon_prerequisite": True,
            },
            {
                "id": "native-addon-load",
                "argv": [
                    str(node),
                    "-e",
                    "const a=require('node:assert/strict');const Database=require("
                    + json.dumps(str(package))
                    + ");"
                    "const fs=require('node:fs'),crypto=require('node:crypto');"
                    "const binary="
                    + json.dumps(str(package / 'build/Release/better_sqlite3.node'))
                    + ";"
                    "a.equal(fs.realpathSync(binary),binary);"
                    "const hash=()=>crypto.createHash('sha256')"
                    ".update(fs.readFileSync(binary)).digest('hex');"
                    "const before=hash();const db=new Database(':memory:',{nativeBinding:binary});"
                    "db.exec('CREATE TABLE smoke(value INTEGER)');"
                    "db.prepare('INSERT INTO smoke VALUES (?)').run(42);"
                    "a.equal(db.prepare('SELECT value FROM smoke').get().value,42);db.close();"
                    "a.equal(hash(),before);console.log(JSON.stringify({native_binding_sha256:before,"
                    "smoke:'native SQLite create/insert/query passed'}))",
                ],
                "timeout_seconds": 30,
            },
        ]
        for check in checks:
            check["native_addon_environment_version"] = environment_version
        result = self._run_check_list(checkout, checks, evidence, candidate)
        # Even failure retains original process logs and its exact source authority.
        result["native_addon_authority"] = authority
        result.setdefault("results", [])
        if result["state"] == "passed":
            validate_native_addon(checkout, authority, Path(dependencies["store"]))
            if native_addon_authority(self.spec, checkout, projects) != authority:
                raise ValueError("native addon frozen source changed during preparation")
            if (
                _sha256(node) != tools["node_interpreter"]["sha256"]
                or _sha256(corepack) != tools["corepack"]["sha256"]
            ):
                raise ValueError("native addon toolchain changed during preparation")
            if frozen_native_builder(self.spec, authority["package_manager"]) != builder:
                raise ValueError("native builder module closure changed during preparation")
            runtime = json.loads(Path(result["results"][0]["log"]).read_text().strip())
            tools["node_interpreter"].update(runtime)
            binary = package / "build/Release/better_sqlite3.node"
            if binary.resolve(strict=True) != binary or not binary.is_file():
                raise ValueError("native addon binary escaped its generated target")
            smoke = json.loads(Path(result['results'][-1]['log']).read_text().strip())
            binary_hash = _sha256(binary)
            if smoke.get('native_binding_sha256') != binary_hash:
                raise ValueError('native load smoke describes a different binary')
            tools["native_binding"] = {"absolute_path": str(binary), "sha256": binary_hash}
            result["node_toolchain"] = tools
        write_private(receipt, result)
        result.update(receipt=str(receipt), receipt_sha256=_sha256(receipt))
        return result

    def _native_cancelled(self) -> bool:
        if self.check_cancelled():
            return True
        with self.store._connect() as db:
            row = db.execute(
                "SELECT phase FROM delivery_runs WHERE run_id=?", (self.spec["run_id"],)
            ).fetchone()
        return row is not None and row["phase"] == "cancelling"

    def _run_native_check(self, process) -> dict:
        if self._native_cancelled():
            raise CheckCancelledBeforeLaunch("native check cancelled before launch")
        self.native_cleanup_confirmed = False
        result = process.run()
        self.native_cleanup_confirmed = result["cleanup"] == "observed-native-confirmed"
        return result

    def _reconcile_native_check(self, journal: Path) -> dict:
        from .delivery_native_process import reconcile_process

        self.native_cleanup_confirmed = False
        result = reconcile_process(journal)
        self.native_cleanup_confirmed = result["cleanup"] == "observed-native-confirmed"
        return result

    def _register_generated(self, checkout: Path, names: list[str]) -> list[Path]:
        from .delivery_resources import RunResources

        resources = RunResources(self.spec)
        roots = []
        for name in names:
            if (not isinstance(name, str) or Path(name).is_absolute()
                    or ".." in Path(name).parts):
                raise ValueError("generated directory left its owned checkout")
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
        process = self._run_native_check(NativeProcess(
            self.spec, folder / "process",
            argv=native_check_argv(self.spec, profile, staging, [
                "corepack", manager, "fetch", "--frozen-lockfile", "--ignore-scripts",
                "--ignore-pnpmfile", "--store-dir", str(dependencies),
            ]),
            cwd=staging, environment=environment, timeout=1800, cancelled=self._native_cancelled,
        ))
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


    def run_implementation_preparation(self, iteration: int, candidate: dict) -> dict:
        """Prepare only accepted locked dependencies before a role executes probes."""
        if self.candidate() != candidate:
            raise ValueError("implementation preparation candidate is stale")
        from .delivery_plan_checks import planned_checks

        folder = self.evidence_dir / "implementation-preparation" / str(iteration)
        planned = planned_checks(self.spec, self.checkout, folder, preparation=True)
        dependencies = [c for c in planned if c['id'].startswith('planned-python-dependencies-')]
        if any('/store' in c['argv']
               for c in self.spec['policy'].get('prepublish_checks', [])):
            dependencies = [c for c in self.spec['policy'].get('prepublish_checks', [])
                            if '/store' in c['argv']] + dependencies
        from .delivery_resources import RunResources

        resources = RunResources(self.spec)
        with resources.locked() as manifest:
            owned = set(manifest['roots'])
        isolated = {}
        interpreters = []
        for check in dependencies:
            if not check['id'].startswith('planned-python-dependencies-'):
                continue
            retained = self.checkout / check['cwd'] / '.venv'
            if not os.path.lexists(retained) or str(retained) in owned:
                continue
            # Historical ignored environments are not ours to adopt or overwrite.
            key = folder.relative_to(self.state_dir).as_posix() + '/' + check['id']
            environment = resources.scratch('implementation-python', key) / 'environment'
            isolated[check['id']] = environment
            interpreters.append({'project': check['cwd'],
                                 'interpreter': str(environment / 'bin/python')})
        options = {'python_environments': isolated} if isolated else {}
        options['native_projects'] = [
            c['cwd'] for c in planned if c['id'].startswith('planned-vitest-')
        ]
        result = (self._run_check_list(self.checkout, dependencies, folder, candidate, **options)
                  if dependencies else {'state': 'passed', 'results': [],
                                        'candidate_id': candidate['id'],
                                        'source_unchanged': True})
        if interpreters:
            result['python_interpreters'] = interpreters
            result['diagnostic'] = 'Use these controller-owned locked Python interpreters; ' \
                'the historical project .venv is preserved untouched: ' + json.dumps(interpreters)
        result['cleanup'] = ('unknown' if result.get('cleanup') == 'unknown'
                             or any(r.get('cleanup') != 'confirmed'
                                    for r in result.get('results', [])) else 'confirmed')
        if self.candidate() != candidate:
            raise ValueError("locked dependency preparation changed feature source")
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
        checks = list(self.spec["policy"].get("checks", []))
        if self.spec["provider"] == "codex" and (
                self.spec["policy"].get("host_sandbox") == "trusted-local"):
            from .delivery_plan_checks import planned_checks

            try:
                planned = planned_checks(
                    self.spec, checkout, self.evidence_dir / "checks" / str(iteration)
                )
                dependencies = [c for c in planned
                                if c['id'].startswith('planned-python-dependencies-')]
                checks = dependencies + checks + [c for c in planned if c not in dependencies]
            except (ValueError, OSError) as exc:
                raise CheckPreparationFailure('accepted-plan-recipes', exc) from exc
        return self._run_check_list(
            checkout,
            checks,
            self.evidence_dir / "checks" / str(iteration),
            candidate,
            native_projects=[c['cwd'] for c in checks if c['id'].startswith('planned-vitest-')],
        )

    def run_browser_qa(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        return execute_browser_qa(self, iteration, candidate)

    def _legacy_published_base(self, found: dict[str, Any]) -> str:
        """Authenticate an unchanged published target independently of its tip."""
        with self.store._connect() as db:
            receipts = db.execute(
                "SELECT observed_json FROM delivery_effects "
                "WHERE run_id=? AND kind='publish' AND state='complete'",
                (self.spec["run_id"],),
            ).fetchall()
        published = [json.loads(row[0]) for row in receipts if row[0]]
        if not any(
            isinstance(receipt, dict)
            and receipt.get("number") == found.get("number")
            and receipt.get("url") == found.get("url")
            and receipt.get("base") == self.spec["base_sha"]
            and receipt.get("state") == "OPEN"
            and re.fullmatch(r"[0-9a-f]{40}", receipt.get("head", ""))
            for receipt in published
        ):
            raise ValueError("legacy target has no completed owned publication")
        owner, name = self.spec["github_repo"].split("/", 1)
        query = (
            "query($owner:String!,$name:String!,$number:Int!,$after:String){"
            "repository(owner:$owner,name:$name){pullRequest(number:$number){"
            "number url state isDraft baseRefName headRefName headRefOid "
            "baseRef{name target{oid}} "
            "timelineItems(first:100,after:$after){totalCount pageInfo{hasNextPage endCursor} "
            "nodes{__typename}}}}}"
        )
        deadline = time.monotonic() + 60
        cursor = None
        cursors = set()
        count = 0
        total = None
        live_ref = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BrokerReadbackUnavailable("legacy published target history timed out")
            try:
                response = json.loads(_run([
                    "gh", "api", "graphql", "-f", "query=" + query,
                    "-F", "owner=" + owner, "-F", "name=" + name,
                    "-F", "number=" + str(found["number"]),
                    *(["-f", "after=" + cursor] if cursor else []),
                ], timeout=remaining))
            except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
                raise BrokerReadbackUnavailable(
                    "legacy published target history unavailable"
                ) from exc
            try:
                observed = response["data"]["repository"]["pullRequest"]
                timeline = observed["timelineItems"]
                nodes = timeline["nodes"]
                if total is None:
                    total = timeline["totalCount"]
                    live_ref = observed['baseRef']
                if (
                    response.get("errors")
                    or any(observed[key] != found[key] for key in (
                        "number", "url", "state", "isDraft", "baseRefName",
                        "headRefName", "headRefOid",
                    ))
                    or not isinstance(nodes, list)
                    or type(total) is not int or total < 0
                    or timeline["totalCount"] != total
                    or observed['baseRef'] != live_ref
                    or any(not isinstance(node, dict) or not node.get("__typename")
                           or node["__typename"] == "BaseRefChangedEvent" for node in nodes)
                ):
                    raise ValueError("legacy published target history changed or is incomplete")
                count += len(nodes)
                if count > total:
                    raise ValueError("legacy published target history count changed")
                if timeline["pageInfo"]["hasNextPage"] is False:
                    if count != total:
                        raise ValueError("legacy published target history is incomplete")
                    break
                next_cursor = timeline['pageInfo'].get('endCursor')
                if (timeline['pageInfo']['hasNextPage'] is not True or not nodes
                        or not isinstance(next_cursor, str) or not next_cursor
                        or next_cursor in cursors):
                    raise ValueError("legacy published target history pagination is incomplete")
                cursors.add(next_cursor)
                cursor = next_cursor
            except (KeyError, TypeError, AttributeError) as exc:
                raise ValueError("legacy published target history unavailable") from exc
        branch = found["baseRefName"]
        try:
            live_tip = live_ref["target"]["oid"]
            if (live_ref["name"] != branch
                    or not re.fullmatch(r"[0-9a-f]{40}", live_tip)):
                raise ValueError("legacy published target branch identity changed")
            _git(self.source, "check-ref-format", "refs/heads/" + branch)
            try:
                _git(self.source, "cat-file", "-e", live_tip + "^{commit}")
            except RuntimeError:
                if _git(self.source, "remote", "get-url", "origin") != self.spec["origin_url"]:
                    raise ValueError("legacy published source origin changed") from None
                # Fetch objects for the authenticated live SHA without moving refs or FETCH_HEAD.
                _run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(self.source),
                      "fetch", "--no-write-fetch-head", "--no-tags", "--refmap=",
                      "origin", live_tip], timeout=max(1, deadline - time.monotonic()))
            _git(self.source, "merge-base", "--is-ancestor", self.spec["base_sha"], live_tip)
        except (KeyError, TypeError, RuntimeError) as exc:
            raise ValueError("legacy published target ancestry unavailable") from exc
        return branch

    def _publication_base_ref(self, *, found: dict[str, Any] | None = None) -> str:
        frozen = self.spec.get("publication_base_ref")
        if frozen:
            return frozen
        # First publication stays exact-match. An authenticated completed
        # publication may retain its unchanged target while upstream advances.
        raw = self.spec["base_ref"]
        if re.fullmatch(r"[0-9a-fA-F]{40}", raw):
            with self.store._connect() as db:
                published = db.execute(
                    "SELECT 1 FROM delivery_effects WHERE run_id=? "
                    "AND kind='publish' AND state='complete' LIMIT 1",
                    (self.spec["run_id"],),
                ).fetchone()
            if published:
                found = found or self._read_owned_pr()
                if found is None:
                    raise ValueError("legacy published PR identity is unavailable")
                return self._legacy_published_base(found)
            return publication_base_ref(self.source, raw, self.spec["base_sha"])
        return raw.removeprefix("origin/")

    def _read_owned_pr(self) -> dict[str, Any] | None:
        if self.spec.get("feature_worker", {}).get("kind") == "chunk":
            from .delivery_feature_publication import owned_pr_number

            number = owned_pr_number(self.spec)
            if number is not None:
                found = json.loads(_run([
                    "gh", "pr", "view", str(number), "--repo", self.spec["github_repo"], "--json",
                    "number,url,state,isDraft,baseRefName,headRefName,headRefOid,title",
                ], timeout=60))
                if (found["number"] != number or found["headRefName"] != self.spec["branch"]
                        or found["isDraft"] or found["state"] != "OPEN"):
                    raise ValueError("recorded chunk PR is no longer the owned open publication")
                return found
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
            or found["isDraft"]
            or found["state"] != "OPEN"
        ):
            raise RuntimeError("owned branch PR is not the authorized open regular PR")
        return found

    def _existing_pr(self, *, validate_metadata: bool = True) -> dict[str, Any] | None:
        found = self._read_owned_pr()
        if found is None:
            return None
        if found["baseRefName"] != self._publication_base_ref(found=found):
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

    def _bind_pending_publication(
        self, key: str, head: str, number: int | None, *, remote_confirmed: bool = False,
    ) -> None:
        """Freeze observed identity in the existing effect before remote mutation."""
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            saved = db.execute(
                "SELECT state,observed_json FROM delivery_effects WHERE effect_key=?", (key,),
            ).fetchone()
            if saved is None or saved["state"] not in {"pending", "complete"}:
                raise ValueError("original publication effect is no longer recoverable")
            previous = json.loads(saved["observed_json"] or "null")
            if previous:
                if (previous["head"] != head
                        or number is not None and previous.get("number") not in {None, number}):
                    raise ValueError("original publication identity changed")
                number = previous.get("number") or number
                remote_confirmed |= previous.get("remote_confirmed", False)
            if saved["state"] == "complete":
                # The original activity may acknowledge while this read is in flight.
                # Preserve its completed result rather than replacing it with a partial observation.
                return
            db.execute(
                "UPDATE delivery_effects SET observed_json=? WHERE effect_key=?",
                (canonical_json({"head": head, "number": number,
                                 "remote_confirmed": remote_confirmed}), key),
            )

    def publish(self, iteration: int, input_candidate: dict[str, Any]) -> dict[str, Any]:
        # Only this invocation's confirmed pre-mutation failure can release uncertainty.
        # Any retained original effect may belong to an earlier lost completion.
        if self.spec.get("publication_readback_version") == 1:
            with self.store._connect() as db:
                prior = db.execute("SELECT 1 FROM delivery_effects WHERE effect_key=?",
                                   (f"publish:{self.spec['run_id']}:{iteration}",)).fetchone()
            self.publication_may_have_effect = prior is not None
        self.validate_candidate_scope()
        publication_branch = self._publication_base_ref()
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
        if before["id"] != input_candidate["id"] and self._changed_paths(include_index=True):
            raise RuntimeError("candidate changed during publication recovery")
        changed = self._changed_paths(include_index=True)
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
            indexed = set(_git(self.checkout, 'ls-files', '-z').split('\0'))
            stageable = sorted(path for path in changed
                               if path in indexed or os.path.lexists(self.checkout / path))
            # Already-staged removals are absent from both index and worktree;
            # preserve them without passing an unmatched pathspec to git add.
            if stageable:
                _git(self.checkout, '--literal-pathspecs', 'add', '--all', '--', *stageable)
            if _git(self.checkout, "diff", "--cached", "--name-only"):
                _git(
                    self.checkout,
                    "-c",
                    "core.hooksPath=/dev/null",
                    "commit",
                    "--signoff",
                    "-m",
                    publication_subject(self.spec),
                )
        self._validate_publication_commits()
        head = _git(self.checkout, "rev-parse", "HEAD")
        if head == self.spec["base_sha"]:
            raise ValueError("no meaningful commit is available for publication")
        if self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(key, head, existing["number"] if existing else None)
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
            self.publication_may_have_effect = True
            _git(self.checkout, "push", "origin", f"HEAD:refs/heads/{self.spec['branch']}")
        if self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(
                key, head, existing["number"] if existing else None, remote_confirmed=True,
            )
        if existing is None:
            title = publication_title(publication_subject(self.spec))
            body = self.state_dir / "pull-request.md"
            body.write_text(
                self.spec["policy"].get("pr_body")
                or (
                    f"{title}\n\n"
                    f"Addresses {self.spec['issue_url']} under the accepted local plan. "
                    "This PR is published for review and remains unmerged.\n"
                ),
                encoding="utf-8",
            )
            os.chmod(body, 0o600)
            self.publication_may_have_effect = True
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
                    publication_branch,
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
        if saved["state"] == "complete" and self.spec.get("publication_readback_version") == 1:
            done = json.loads(saved["observed_json"])
            if expected_head is not None and expected_head != done["head"]:
                raise ValueError("publication expected head differs from original effect")
            if expected_pr_number is not None and expected_pr_number != done["number"]:
                raise ValueError("publication expected PR differs from original effect")
            expected_head, expected_pr_number = done["head"], done["number"]
        elif self.spec.get("publication_readback_version") == 1:
            original = json.loads(saved["observed_json"] or "null")
            if not original or not original.get("head"):
                raise ValueError("original publication head has not been observed")
            if expected_head is not None and expected_head != original["head"]:
                raise ValueError("publication expected head differs from original effect")
            if (expected_pr_number is not None and original.get("number") is not None
                    and expected_pr_number != original["number"]):
                raise ValueError("publication expected PR differs from original effect")
            expected_head = original["head"]
            expected_pr_number = original.get("number") or expected_pr_number
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
        try:
            remote = _git(self.source, "ls-remote", "origin", f"refs/heads/{self.spec['branch']}")
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
            if self.spec.get("publication_readback_version") == 1:
                raise BrokerReadbackUnavailable("published branch readback unavailable") from exc
            raise
        if (self.spec.get("publication_readback_version") == 1 and saved["state"] == "pending"
                and not original.get("remote_confirmed")
                and (not remote or (remote.split()[0] != head
                                    and remote.split()[0] == input_candidate["head"]))):
            return {"state": "pending", "reason": "original_push_readback", "head": head}
        if not remote or remote.split()[0] != head:
            raise ValueError("remote feature branch differs from the published checkout")
        if saved["state"] == "pending" and self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(key, head, expected_pr_number, remote_confirmed=True)
        found = self._existing_pr()
        if found is not None:
            if (self.spec.get("publication_readback_version") == 1
                    and expected_pr_number is not None and found["number"] != expected_pr_number):
                raise ValueError("publication resolved to a different PR")
            if (self.spec.get("publication_readback_version") == 1
                    and found["headRefOid"] not in {head, input_candidate["head"]}):
                raise ValueError("owned PR head changed from the original publication")
        if (found is not None and saved["state"] == "pending"
                and self.spec.get("publication_readback_version") == 1):
            self._bind_pending_publication(key, head, found["number"], remote_confirmed=True)
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

    async def checks(
        self, pr: dict[str, Any], *, timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        required = set(self.spec["policy"].get("required_ci", []))
        if not required:
            return {"state": "unverified", "reason": "no required CI checks configured"}
        patient = "ci_wait_seconds" in self.spec["policy"]
        if timeout_seconds is None:
            timeout_seconds = self.spec["policy"].get("ci_wait_seconds", 1200)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        delay, checks, diagnostic = 15, {}, None
        argv = ["gh", "pr", "view", str(pr["number"]), "--repo", self.spec["github_repo"],
                "--json", "headRefOid,statusCheckRollup"]
        while True:
            try:
                if patient:
                    remaining = deadline - asyncio.get_running_loop().time()
                    found = json.loads(await asyncio.to_thread(
                        _run, argv, timeout=max(1, min(60, remaining))))
                else:
                    found = json.loads(_run(argv, timeout=60))
            except (RuntimeError, subprocess.TimeoutExpired, OSError, json.JSONDecodeError) as exc:
                if not patient:
                    raise
                diagnostic = str(exc)[:500]
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return {"state": "pending", "checks": checks, "reason": "CI deadline reached",
                            "diagnostic": diagnostic}
                await asyncio.sleep(min(delay, remaining))
                delay = min(delay * 2, 60)
                continue
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
            remaining = deadline - asyncio.get_running_loop().time()
            await asyncio.sleep(min(delay, remaining) if patient else 15)
            if patient:
                delay = min(delay * 2, 60)
