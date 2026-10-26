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
from .delivery_source_scope import outside_scope, require_authorized
from .delivery_store import DeliveryStore, _now


def _run(argv: list[str], *, cwd: Path | None = None, timeout: int = 120) -> str:
    from .delivery_publication_execution import inherited_descriptors

    nul_output = argv[0] == 'git' and '-z' in argv
    result = subprocess.run(
        argv, cwd=cwd, text=not nul_output, capture_output=True, timeout=timeout, check=False,
        pass_fds=inherited_descriptors(),
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


class CheckSuperseded(RuntimeError):
    """Temporal replaced this attempt; its retry owns every further launch and score."""


class DeliveryBroker:
    # Spec-only resource helpers also retain the ordinary, unscoped default.
    diagnostic_context = None

    # Journals of the native gates a broker ran or joined, so an unknown
    # teardown can be re-proven against the processes they recorded.
    native_journals: tuple[Path, ...] = ()
    # Set by the activity once Temporal replaced its waiter: the body may finish
    # the native process it supervises but must not launch or score another.
    check_superseded = staticmethod(lambda: False)

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
        self.diagnostic_context = None
        self.publication_may_have_effect = True
        # (effect key, insertion stamp) of the row this broker inserted, if any.
        self.inserted_effect: tuple[str, str] | None = None

    def run_resources(self, *, read_only=False):
        from .delivery_resources import RunResources

        context = getattr(self, 'diagnostic_context', None)
        return (context.resources(read_only=read_only)
                if context is not None else RunResources(
                    self.spec, read_only=read_only))

    def _native_process(self, folder, **options):
        from .delivery_native_process import NativeProcess

        if getattr(self, 'diagnostic_context', None) is not None:
            options['register_resources'] = False
        return NativeProcess(self.spec, folder, **options)

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
            stamp = _now()
            db.execute(
                """INSERT INTO delivery_effects
                   (effect_key,run_id,kind,request_json,state,updated_at)
                   VALUES (?,?,?,?,'pending',?)""",
                (key, self.spec["run_id"], kind, serialized, stamp),
            )
            self.inserted_effect = (key, stamp)
        return None

    def _finish_effect(self, key: str, result: dict[str, Any]) -> None:
        from .delivery_publication_execution import current

        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if owner := current(key):
                owner.check(db)
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
            resources = self.run_resources()
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
        selected = set(recovery["paths"])
        if not selected or outside_scope(self.spec["policy"], selected):
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
        escaped = outside_scope(self.spec['policy'], changed, checkout=self.checkout)
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
            resources = self.run_resources()
            resources.register(path, "gate")
        if path.exists():
            if _git(path, "rev-parse", "HEAD") != candidate["head"]:
                raise RuntimeError("gate checkout head changed")
        else:
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            _git(self.source, "worktree", "add", "--detach", str(path), candidate["head"])
        if resources:
            resources.created(path, head=candidate['head'])
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
        preserve_workspace_dependencies: bool = False,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        checkout = checkout.resolve(strict=True)

        def index_description() -> str | None:
            try:
                return _git(checkout, "ls-files", "--stage", "-z")
            except RuntimeError as exc:
                if (self.spec["provider"] != "fake"
                        or os.path.lexists(checkout / ".git")
                        or "not a git repository" not in str(exc)):
                    raise
                return None  # Explicit fake fixtures may have no Git repository.

        evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec)
        from .delivery_feature_gates import (
            GateAdmissionError,
            bind_selector_report,
            resolve_required_selectors,
            retain_admission_failure,
            retained_junit,
            selector_evidence,
        )

        selected_sources = {}
        for recipe in checks:
            try:
                selected_sources[recipe["id"]] = resolve_required_selectors(
                    recipe, checkout, spec=self.spec)
            except GateAdmissionError as exc:
                return retain_admission_failure(
                    self.spec, recipe, evidence_dir / recipe["id"], candidate, exc)
        checks = [bind_selector_report(recipe, evidence_dir / recipe["id"])
                  for recipe in checks]
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
        initial_index = index_description()
        native_dependencies = (
            self._ensure_native_dependency_store(checkout)
            if self.spec["provider"] == "codex"
            and any("/store" in check["argv"] for check in checks)
            else None
        )
        node_toolchain = None
        native_addon = None

        def require_prepared_source(check_id):
            if (candidate_for(checkout)["id"] != candidate["id"]
                    or index_description() != initial_index):
                raise CheckPreparationFailure(
                    check_id, ValueError("check source or Git index changed during preparation"),
                    results)

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
            independent_test = (check.get("kind") == "test" and "/store" not in argv
                                and not check.get("native_addon_prerequisite")
                                and not check["id"].startswith("planned-python-dependencies-")
                                and argv[:3] != ["corepack", "pnpm", "install"])
            if results and any(not item["passed"] for item in results):
                if (not independent_test or candidate_for(checkout)["id"] != candidate["id"]
                        or index_description() != initial_index):
                    break
                if self.spec["provider"] == "codex" and self._native_cancelled():
                    raise CheckCancelledBeforeLaunch("native check cancelled before launch")
            range_binding = None
            if check.get('plan_provenance', {}).get('recipe') == 'checks.diff':
                if argv != ['git', 'diff', '--check', 'origin/main...HEAD']:
                    raise ValueError(
                        'tracked checks.diff must retain its exact committed-range command'
                    )
                range_binding = self._diff_range_binding(checkout, candidate)
            native_result = None
            interrupted = []
            check_evidence = None
            preserved_modules = []
            if self.spec["provider"] == "codex":
                from .delivery_preparation import verify_prepared_spec
                from .delivery_sandbox import native_check_argv, prepare_native_check

                try:
                    verify_prepared_spec(self.spec)
                except Exception as exc:
                    # Nothing launched: report the unverifiable native authority with
                    # confirmed cleanup instead of an unknown process teardown.
                    raise CheckPreparationFailure(
                        "native-execution-authority", exc, results) from exc
                try:
                    profile, environment = prepare_native_check(
                        self.spec, checkout, evidence_dir, check,
                        dependency_store=(
                            Path(native_dependencies["store"]) if native_dependencies else None
                        ),
                        resources=self.run_resources(), diagnostic_context=self.diagnostic_context,
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
                    if preserve_workspace_dependencies and '/store' in argv:
                        require_prepared_source(check['id'])
                        if self.candidate() != candidate:
                            raise ValueError('implementation dependency source changed')
                        preserved_modules = self.run_resources().preserve_workspace_dependencies(
                            checkout, candidate, check)
                    generated = self._register_generated(checkout,
                        list(dict.fromkeys([
                            *([] if isolated_python is not None else
                              ["node_modules", *check.get("generated_directories", [])]),
                            *(self.spec["policy"].get("browser_qa") or {}).get(
                                "artifact_paths", []),
                        ])))
                except (ValueError, OSError) as exc:
                    raise CheckPreparationFailure(check['id'], exc, results) from exc
                # The same frozen lock populates an owned store before offline
                # installation. Candidate commands may read, never mutate it.
                command = [
                    native_dependencies["store"] if item == "/store" else item for item in argv
                ]

                options = {
                    "argv": native_check_argv(self.spec, profile, cwd, command),
                    "cwd": cwd,
                    "environment": environment,
                    "timeout": int(check.get("timeout_seconds", 600)),
                    "cancelled": self._native_cancelled,
                }
                require_prepared_source(check["id"])
                native_result = self._run_native_check(self._native_process(
                    evidence_dir / check["id"] / "native", **options,
                ))
                if native_result.get("interrupted_monitor") is True:
                    if self.diagnostic_context is not None:
                        return {'state': 'unknown', 'cleanup': native_result['cleanup'],
                                'candidate_id': candidate['id'], 'native_process': native_result,
                                'reason': ('diagnostic monitor was lost; '
                                           'invocation is never retried')}
                    # A lost monitor yields no verdict. One fresh invocation
                    # re-runs the gate; the interrupted journal stays as evidence.
                    interrupted.append(native_result)
                    require_prepared_source(check["id"])
                    native_result = self._run_native_check(self._native_process(
                        evidence_dir / check["id"] / "native-retry", **options,
                    ))
                self._record_generated(generated)
                if native_result["cleanup"] == "unknown":
                    return {
                        "state": "unknown",
                        "cleanup": "unknown",
                        "candidate_id": candidate["id"],
                        "native_process": native_result,
                    }
                if native_result.get("interrupted_monitor") is True:
                    interrupted.append(native_result)
                    if (self.spec['policy'].get('host_sandbox') == 'trusted-local'
                            and check.get('kind') == 'test'):
                        disposal = self.run_resources().cleanup_check_artifacts(
                            evidence_dir / check['id'] / 'pytest-artifacts',
                            [Path(item['journal']) for item in interrupted])
                        if disposal['state'] == 'failed_unknown':
                            return {'state': 'unknown', 'cleanup': 'unknown',
                                    'candidate_id': candidate['id'], 'artifact_cleanup': disposal}
                    return {
                        "state": "unknown",
                        "cleanup": "confirmed",
                        "process_cleanup": native_result["cleanup"],
                        "candidate_id": candidate["id"],
                        "check_id": check["id"],
                        "reason": "native check monitor was lost twice; "
                                  "the gate produced no verdict",
                        "native_process": native_result,
                        "interrupted_native_processes": interrupted,
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
                command_started_at, command_started = _now(), time.monotonic()
                checked = subprocess.run(
                    command,
                    cwd=cwd,
                    env=check_env,
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=int(check.get("timeout_seconds", 600)),
                )
                command_timing = {
                    "started_at": command_started_at, "finished_at": _now(),
                    "duration_seconds": time.monotonic() - command_started,
                }
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
            artifact_cleanup = None
            if (self.spec["policy"].get("host_sandbox") == "trusted-local"
                    and check.get("kind") == "test"):
                from .delivery_check_evidence import retain_artifacts

                try:
                    check_evidence = retain_artifacts(evidence_dir / check["id"], candidate)
                except (ValueError, OSError) as exc:
                    evidence_failure = str(exc)
                finally:
                    if self.spec['provider'] == 'codex':
                        artifact_cleanup = self.run_resources().cleanup_check_artifacts(
                            evidence_dir / check['id'] / 'pytest-artifacts',
                            [Path(item['journal']) for item in [*interrupted, native_result]])
                        if artifact_cleanup['state'] == 'failed_unknown':
                            evidence_failure = artifact_cleanup['reason']
            parsed_output = visible_output(output)
            count = None
            if check.get("test_count_regex"):
                count = observed_test_count(output, check["test_count_regex"])
            junit = None
            test_verdict_failure = None
            if check.get("junit_required"):
                from .delivery_check_evidence import junit_counts

                try:
                    if not check_evidence:
                        raise ValueError('required owned JUnit report was not retained')
                    junit = junit_counts(check_evidence, candidate['id'], self.state_dir)
                    count = junit['passed']
                    if junit['failures'] or junit['errors']:
                        test_verdict_failure = 'required JUnit report records failed test cases'
                        raise ValueError(test_verdict_failure)
                except (ValueError, OSError) as exc:
                    evidence_failure = str(exc)
            selector_receipts = None
            if selected_sources.get(check["id"]):
                try:
                    if not check_evidence:
                        raise ValueError("required per-selector JUnit report was not retained")
                    selector_receipts = selector_evidence(
                        check, checkout, selected_sources[check["id"]],
                        junit=retained_junit(check_evidence, candidate["id"], self.state_dir),
                        spec=self.spec)
                except (ValueError, OSError) as exc:
                    evidence_failure = str(exc)
                    selector_receipts = getattr(exc, "_failed_selector_receipts", None)
                    test_verdict_failure = (
                        evidence_failure if selector_receipts is not None else None
                    )
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
                    **({key: native_result.get(key)
                        for key in ("started_at", "finished_at", "duration_seconds")}
                       if native_result else command_timing),
                    "test_count": count,
                    **({"junit": junit} if junit is not None else {}),
                    **({"selectors": selector_receipts} if selector_receipts is not None else {}),
                    "rejected_output": rejected_output,
                    "rejection_causes": rejected_causes,
                    "passed": passed,
                    "diagnostic": parsed_output[-2000:] if not passed else None,
                    "log": str(artifact),
                    "log_sha256": _sha256(artifact),
                    **({'temporal_test_prerequisites': {
                        'receipt': str(evidence_dir / check['id'] / 'temporal-prerequisites.json'),
                        'receipt_sha256': _sha256(
                            evidence_dir / check['id'] / 'temporal-prerequisites.json')}}
                       if (evidence_dir / check['id'] / 'temporal-prerequisites.json').exists()
                       else {}),
                    **({"plan_provenance": check["plan_provenance"]}
                       if check.get("plan_provenance") else {}),
                    **({"satisfies": check["satisfies"]} if check.get("satisfies") else {}),
                    **({"artifacts": check_evidence} if check_evidence else {}),
                    **({"evidence_failure": evidence_failure} if evidence_failure else {}),
                    "cleanup": ('unknown' if artifact_cleanup and artifact_cleanup['state']
                                == 'failed_unknown' else 'confirmed'),
                    **({'artifact_cleanup': artifact_cleanup} if artifact_cleanup else {}),
                    **({'range_binding': range_binding} if range_binding else {}),
                    **({'preserved_workspace_dependencies': preserved_modules}
                       if preserved_modules else {}),
                    **(
                        {
                            "process_cleanup": native_result["cleanup"],
                            "native_process": native_result,
                            "dependency_preparation": native_dependencies,
                        }
                        if native_result
                        else {}
                    ),
                    **({"interrupted_native_processes": interrupted} if interrupted else {}),
                }
            )
            # Every check in a failed batch needs clean custody, even a passing verdict.
            if any(not item["passed"] for item in results) and (
                not independent_test or results[-1]["cleanup"] != "confirmed"
                or (evidence_failure is not None and evidence_failure != test_verdict_failure)
                or (artifact_cleanup is not None and artifact_cleanup.get("state")
                    not in {"removed", "already_absent"})
                or (native_result is not None and (
                    native_result.get("state") != "finished"
                    or native_result.get("cleanup") != "observed-native-confirmed"
                    or native_result.get("monitoring_complete") is not True
                    or native_result.get("stdio_drained") is not True
                    or any(native_result.get(flag) is not False
                           for flag in ("cancelled", "timed_out", "listener_conflict"))
                    or native_result.get("interrupted_monitor", False) is not False
                    or interrupted
                ))
            ):
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
        source_unchanged = (after["id"] == candidate["id"]
                            and index_description() == initial_index)
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
        from .delivery_resources import read_private, write_private
        from .delivery_sandbox import _native_addon_environment_version, _native_path

        authority = native_addon_authority(self.spec, checkout, projects)
        if authority is None:
            return None
        resources = self.run_resources()
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
        environment_version = _native_addon_environment_version(
            self.diagnostic_context.resource_spec if self.diagnostic_context is not None
            else self.spec, evidence)
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
        if self.check_superseded():
            raise CheckSuperseded("native check superseded before launch")
        from .delivery_temporal_prerequisites import check_launch as temporal_check_launch

        try:
            if self.diagnostic_context is not None:
                self.diagnostic_context.process(process)
            temporal_check_launch(process)
        except (ValueError, OSError) as exc:
            raise CheckPreparationFailure('temporal-test-prerequisites', exc) from exc
        self.native_cleanup_confirmed = False
        self.native_journals += (process.journal,)
        try:
            result = process.run()
        finally:
            if self.diagnostic_context is not None:
                self.diagnostic_context.observed_process(process)
        self.native_cleanup_confirmed = result["cleanup"] == "observed-native-confirmed"
        if self.check_superseded():
            # The retry scores this invocation from its journal; a replaced body
            # neither post-processes nor launches alongside it.
            raise CheckSuperseded("native check superseded after its monitor")
        return result

    def _reconcile_native_check(self, journal: Path) -> dict:
        from .delivery_native_process import reconcile_process

        if self.check_superseded():
            raise CheckSuperseded("native check superseded before reconciliation")
        self.native_cleanup_confirmed = False
        self.native_journals += (journal,)
        result = reconcile_process(journal, spec=self.spec)
        self.native_cleanup_confirmed = result["cleanup"] == "observed-native-confirmed"
        return result

    def _register_generated(self, checkout: Path, names: list[str]) -> list[Path]:
        from .delivery_configured_resources import (
            declared_generated_directories,
            workspace_dependency_directories,
        )
        from .delivery_resources import private_directory

        resources = self.run_resources()
        declared = declared_generated_directories(self.spec)
        browser = (self.spec['policy'].get('browser_qa') or {}).get('artifact_paths', [])
        if 'node_modules' in names:
            names = list(dict.fromkeys([*names, *workspace_dependency_directories(
                checkout, policy=self.spec['policy']),
                                       *sorted(declared)]))
        roots = []
        for name in names:
            if (not isinstance(name, str) or not Path(name).parts
                    or Path(name).as_posix() != name or Path(name).is_absolute()
                    or any(part in {"..", ".git", ".codex", ".agents"}
                           for part in Path(name).parts)):
                raise ValueError("generated directory left its owned checkout")
            root = checkout / name
            if _git(checkout, "ls-files", "--", name):
                raise ValueError("configured generated directory contains tracked source")
            configured_output = name in browser or name in declared
            if configured_output:
                resources._allowed(root, "generated", finalizing=True)
                private_directory(root.parent)
            if not root.parent.is_dir() or root.parent.resolve() != root.parent:
                raise ValueError("generated directory parent is not a fixed candidate directory")
            if configured_output and not {'.venv', 'node_modules'} & set(Path(name).parts):
                resources.generated_directory(root)
            else:
                resources.register(root, "generated")
            roots.append(root)
        return roots

    def _record_generated(self, roots: list[Path]) -> None:
        resources = self.run_resources()
        for root in roots:
            if os.path.lexists(root):
                resources.created(root)

    def _ensure_native_dependency_store(self, checkout: Path) -> dict[str, Any]:
        """Fetch frozen registry data without exposing candidate setup or credentials."""
        from .delivery_native_dependencies import REGISTRY, frozen_pnpm_inputs, write_frozen_inputs
        from .delivery_preparation import verify_prepared_spec
        from .delivery_resources import private_directory, read_private, write_private
        from .delivery_sandbox import native_check_argv, prepare_native_check

        try:
            verify_prepared_spec(self.spec)
        except Exception as exc:
            raise CheckPreparationFailure("native-execution-authority", exc) from exc
        provenance = {}
        manager, inputs = frozen_pnpm_inputs(self.spec, checkout, provenance=provenance)
        resources = self.run_resources()
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
            resources=self.run_resources(), diagnostic_context=self.diagnostic_context,
        )
        environment.update({
            "COREPACK_ENABLE_NETWORK": "0", "npm_config_registry": "https://" + REGISTRY + "/",
        })
        def launch(name):
            return self._run_native_check(self._native_process(
                folder / name,
                argv=native_check_argv(self.spec, profile, staging, [
                    "corepack", manager, "fetch", "--frozen-lockfile", "--ignore-scripts",
                    "--ignore-pnpmfile", "--store-dir", str(dependencies),
                ]),
                cwd=staging, environment=environment, timeout=1800,
                cancelled=self._native_cancelled,
            ))

        process = launch("process")
        if process.get("interrupted_monitor") is True and self.diagnostic_context is not None:
            raise ValueError('diagnostic dependency monitor was lost; invocation is never retried')
        if process.get("interrupted_monitor") is True:
            # A lost monitor leaves no fetch outcome; one fresh invocation repeats it.
            process = launch("process-retry")
            if process.get("interrupted_monitor") is True:
                raise ValueError("native dependency preparation monitor was lost twice")
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


    def _prepare_generated_outputs(self, checkout: Path) -> None:
        from .delivery_configured_resources import declared_generated_directories

        outputs = sorted(declared_generated_directories(self.spec) | set(
            (self.spec['policy'].get('browser_qa') or {}).get('artifact_paths', [])))
        try:
            self._register_generated(checkout, [
                name for name in outputs
                if not {'.venv', 'node_modules'} & set(Path(name).parts)])
        except (ValueError, OSError) as exc:
            raise CheckPreparationFailure('native-generated-output-custody', exc) from exc

    def run_implementation_preparation(self, iteration: int, candidate: dict) -> dict:
        """Pin configured output and prepare accepted locked dependencies before role probes."""
        if self.candidate() != candidate:
            raise ValueError("implementation preparation candidate is stale")
        from .delivery_configured_resources import implementation_prerequisites
        from .delivery_plan_checks import planned_checks

        folder = self.evidence_dir / "implementation-preparation" / str(iteration)
        planned = planned_checks(self.spec, self.checkout, folder, preparation=True)
        dependencies = [c for c in planned if c['id'].startswith('planned-python-dependencies-')]
        dependencies = implementation_prerequisites(self.spec) + dependencies

        resources = self.run_resources()
        self._prepare_generated_outputs(self.checkout)
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
        options['preserve_workspace_dependencies'] = True
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
        # Prepublication source/index bytes are committed only during publication.
        # A HEAD-only gate can represent the candidate once Git-visible work is clean.
        checkout = self.checkout
        if self.spec["provider"] == "codex" and not _git(
                self.checkout, "status", "--porcelain=v1", "--untracked-files=all"):
            checkout = self.gate_checkout("verify", iteration, candidate)
        from .delivery_validation_schedule import prepublication

        checks, deferred = prepublication(self.spec)
        result = (self._run_check_list(
            checkout, checks, self.evidence_dir / "prechecks" / str(iteration), candidate,
        ) if checks else {"state": "passed", "results": [],
                          "candidate_id": candidate["id"], "source_unchanged": True})
        if deferred:
            result["deferred_to_checks"] = deferred
        return result

    def run_checks(self, iteration: int, candidate: dict[str, Any]) -> dict[str, Any]:
        checkout = self.gate_checkout("verify", iteration, candidate)
        checks = list(self.spec["policy"].get("checks", []))
        if self.spec["provider"] == "codex" and (
                self.spec["policy"].get("host_sandbox") == "trusted-local"):
            from .delivery_plan_checks import planned_checks
            from .delivery_validation_schedule import local_checks

            try:
                planned = planned_checks(
                    self.spec, checkout, self.evidence_dir / "checks" / str(iteration)
                )
                checks = local_checks(self.spec, checks, planned)
            except (ValueError, OSError) as exc:
                raise CheckPreparationFailure('accepted-plan-recipes', exc) from exc
        return self._run_check_list(
            checkout,
            checks,
            self.evidence_dir / "checks" / str(iteration),
            candidate,
            native_projects=[c['cwd'] for c in checks if c['id'].startswith('planned-vitest-')],
        )

    def run_browser_qa(self, iteration: int, candidate: dict[str, Any], *,
                       reconcile_only: bool = False) -> dict[str, Any]:
        return execute_browser_qa(self, iteration, candidate, reconcile_only=reconcile_only)

    def _legacy_published_base(self, found: dict[str, Any], *, read_only: bool = False) -> str:
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
                if read_only:
                    raise ValueError(
                        "legacy published target requires an existing live object"
                    ) from None
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
        from .delivery_publication_execution import current

        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if owner := current(key):
                owner.check(db)
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

    def publication_effect_key(self, iteration: int) -> str:
        key = f"publish:{self.spec['run_id']}:{iteration}"
        head = self.spec.get("publication_validation_head")
        return key + ":validate:" + head if head else key

    def publish(self, iteration: int, input_candidate: dict[str, Any]) -> dict[str, Any]:
        from .delivery_publication_execution import PublicationUncertain, execution

        with execution(self, iteration, input_candidate) as owner:
            if owner is None:
                return {"state": "pending", "reason": "publication_execution_active"}
            try:
                return self._publish_owned(iteration, input_candidate, owner)
            except PublicationUncertain as exc:
                return {"state": "pending", "reason": "original_" + exc.operation + "_readback"}

    def _publish_owned(self, iteration, input_candidate, owner):
        # Only this invocation's confirmed pre-mutation failure can release uncertainty.
        # Any retained original effect may belong to an earlier lost completion.
        if self.spec.get("publication_readback_version") == 1:
            with self.store._connect() as db:
                prior = db.execute("SELECT 1 FROM delivery_effects WHERE effect_key=?",
                                   (self.publication_effect_key(iteration),)).fetchone()
                if prior is not None:
                    # Adopting a retained row re-stamps it, so the invocation that
                    # inserted it can no longer discard it from a timed-out thread.
                    db.execute(
                        "UPDATE delivery_effects SET updated_at=? "
                        "WHERE effect_key=? AND state='pending'",
                        (_now(), self.publication_effect_key(iteration)),
                    )
            self.publication_may_have_effect = prior is not None
        self.validate_candidate_scope()
        publication_branch = self._publication_base_ref()
        self._validate_publication_commits()
        key = self.publication_effect_key(iteration)
        before = self.candidate()
        request = {"iteration": iteration, "input_candidate_id": input_candidate["id"]}
        done = self._effect(key, "publish", request)
        if (self.spec.get("publication_readback_version") == 1
                and not self.publication_effect_inserted(iteration)):
            self.publication_may_have_effect = True
        if done:
            found = self._existing_pr()
            if found is None or found["headRefOid"] != done["head"]:
                return {"state": "pending", "reason": "pr_head_readback", "head": done["head"]}
            return done
        if before["id"] != input_candidate["id"] and self._changed_paths(include_index=True):
            raise RuntimeError("candidate changed during publication recovery")
        if before["id"] != input_candidate["id"]:
            if (any(before.get(field) != input_candidate.get(field) for field in (
                    "content_sha256", "base_sha", "policy_digest", "environment_digest"))
                    or before["head"] != input_candidate["head"]
                    and _git(self.checkout, "rev-parse", "HEAD^") != input_candidate["head"]):
                raise ValueError("published candidate content or authority changed")
            owner.observed("commit")
        changed = self._changed_paths(include_index=True)
        require_authorized(self.spec["policy"], changed, checkout=self.checkout)
        existing = self._existing_pr()
        if changed:
            owner.require("commit")
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
                owner.mutate("commit", lambda: _git(
                    self.checkout,
                    "-c",
                    "core.hooksPath=/dev/null",
                    "commit",
                    "--signoff",
                    "-m",
                    publication_subject(self.spec),
                ))
        self._validate_publication_commits()
        head = _git(self.checkout, "rev-parse", "HEAD")
        if head == self.spec["base_sha"]:
            raise ValueError("no meaningful commit is available for publication")
        if self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(key, head, existing["number"] if existing else None)
        try:
            remote = _git(self.source, "ls-remote", "origin",
                          f"refs/heads/{self.spec['branch']}")
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            raise BrokerReadbackUnavailable(
                "remote branch readback unavailable before push") from exc
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
            owner.mutate("push", lambda: _git(
                self.checkout, "push", "origin", f"HEAD:refs/heads/{self.spec['branch']}"))
        else:
            owner.observed("push")
        if self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(
                key, head, existing["number"] if existing else None, remote_confirmed=True,
            )
        if existing is None:
            self.publication_may_have_effect = True
            self._create_owned_pr(publication_branch)
        else:
            owner.observed("create")
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
        owner.observed("create")
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

    def _create_owned_pr(self, publication_branch: str) -> None:
        """Open the regular PR for the pushed head; an existing one is read back."""
        from .delivery_publication_execution import current

        owner = current()
        if owner is None:
            raise ValueError("PR creation requires its publication executor")
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
        try:
            def create():
                return _run(
                    [
                        "gh", "pr", "create", "--repo", self.spec["github_repo"],
                        "--head", self.spec["branch"], "--base", publication_branch,
                        "--title", title, "--body-file", str(body),
                    ],
                    cwd=self.checkout,
                    timeout=90,
                )
            owner.mutate("create", create)
        except RuntimeError as exc:
            # GitHub refuses a second open PR for one head and base: the
            # existing PR is this publication, and the caller reads it back.
            if "already exists" not in str(exc).casefold():
                raise

    def publication_effect_inserted(self, iteration: int) -> bool:
        """Whether this broker inserted the publish effect row of ``iteration``."""
        return (self.inserted_effect is not None
                and self.inserted_effect[0] == self.publication_effect_key(iteration))

    def discard_unused_publication(self, iteration: int) -> None:
        """Forget the pending effect of an invocation confirmed to precede any mutation.

        A retained row would make the next attempt treat the same transport
        failure as a possible lost mutation. Only the invocation that inserted
        the row and never reached push or PR creation may remove it, and only
        while the row still carries its insertion stamp: a later attempt that
        adopted the row re-stamped it and owns it from then on.
        """
        if (self.publication_may_have_effect is not False
                or not self.publication_effect_inserted(iteration)):
            raise ValueError("publication effect may have taken effect; it cannot be discarded")
        key, stamp = self.inserted_effect
        with self.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "DELETE FROM delivery_effects WHERE effect_key=? AND kind='publish' "
                "AND state='pending' AND updated_at=?",
                (key, stamp),
            )

    def reconcile_publish(
        self,
        iteration: int,
        input_candidate: dict[str, Any],
        *,
        expected_head: str | None = None,
        expected_pr_number: int | None = None,
        complete: bool = True,
        create_missing: bool = False,
    ) -> dict[str, Any]:
        from .delivery_publication_execution import PublicationUncertain, execution

        with execution(self, iteration, input_candidate) as owner:
            if owner is None:
                return {"state": "pending", "reason": "publication_execution_active"}
            try:
                # The lost body may have ended in a confirmed pre-mutation
                # query failure and discarded its unused row. Its durable
                # journal, rather than absence of a remote projection, permits
                # continuation of the same checked request. Admission readback
                # (complete=False) cannot originate any publication operation.
                if (create_missing and complete and owner.replacing
                        and owner.permitted("push") and owner.permitted("create")):
                    with self.store._connect() as db:
                        saved = db.execute(
                            "SELECT state FROM delivery_effects WHERE effect_key=?",
                            (owner.key,),
                        ).fetchone()
                    if saved is None or saved["state"] == "pending":
                        return self._publish_owned(iteration, input_candidate, owner)
                return self._reconcile_publish_owned(
                    iteration, input_candidate, expected_head=expected_head,
                    expected_pr_number=expected_pr_number, complete=complete,
                    create_missing=create_missing, owner=owner,
                )
            except PublicationUncertain as exc:
                return {"state": "pending", "reason": "original_" + exc.operation + "_readback"}

    def _reconcile_publish_owned(
        self, iteration, input_candidate, *, expected_head=None, expected_pr_number=None,
        complete=True, create_missing=False, owner,
    ):
        """Complete an existing publish effect after external readback.

        This path cannot commit or push. It is safe after a successful push
        followed by a stale GitHub PR projection. With ``create_missing`` it
        may finish a creation that was never launched; an uncertain earlier
        creation must first be observed, even after its local process exits.
        """
        from .delivery_publication_execution import PublicationUncertain

        key = self.publication_effect_key(iteration)
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
            # Reading a pending effect adopts it: the invocation that inserted it
            # can no longer discard the row this reconciliation depends on.
            with self.store._connect() as db:
                db.execute(
                    "UPDATE delivery_effects SET updated_at=? WHERE effect_key=? "
                    "AND state='pending'",
                    (_now(), key),
                )
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
        owner.observed("push")
        if saved["state"] == "pending" and self.spec.get("publication_readback_version") == 1:
            self._bind_pending_publication(key, head, expected_pr_number, remote_confirmed=True)
        found = self._existing_pr()
        if found is not None:
            owner.observed("create")
        if (found is None and create_missing and saved["state"] == "pending"
                and self.spec.get("publication_readback_version") == 1
                and expected_pr_number is None):
            # The push is confirmed at this exact head and no PR uses the
            # branch; finish the original mutation instead of waiting for it.
            try:
                self._create_owned_pr(self._publication_base_ref())
            except PublicationUncertain:
                raise
            except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
                raise BrokerReadbackUnavailable(
                    "owned PR creation is unconfirmed; remote readback required") from exc
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
        from .delivery_ci_inventory_io import GitHubRead, PRHeadChanged
        from .delivery_ci_inventory_state import assessment

        required = set(self.spec["policy"].get("required_ci", []))
        if not required:
            return {"state": "unverified", "reason": "no required CI checks configured",
                    "ci_assessment": assessment(
                        "unverified", pr.get("head"), source="authenticated_PR_check_contexts",
                        complete=False, required=required)}
        if getattr(self, "store", None) is not None:
            from .delivery_ci_inventory import result_for

            inventory = await asyncio.to_thread(
                result_for, self, pr, timeout_seconds=timeout_seconds)
            if inventory is not None:
                return inventory
        patient = "ci_wait_seconds" in self.spec["policy"]
        if timeout_seconds is None:
            timeout_seconds = self.spec["policy"].get("ci_wait_seconds", 1200)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        delay, checks, diagnostic, observations = 15, {}, None, []

        def result(state, *, complete=False, **details):
            return {"state": state, "checks": checks, "observations": observations,
                    "ci_assessment": assessment(
                        state, pr["head"], source="authenticated_PR_check_contexts",
                        complete=complete, required=required,
                        present=(any(item.get("__typename") == "CheckRun"
                                     and item.get("name") in required for item in observations)
                                 if complete else None)), **details}

        def readback(remaining):
            reader = GitHubRead(repositories={self.spec["github_repo"]},
                                timeout=max(1, min(60, remaining)))
            return reader.rollup(self.spec["github_repo"], pr["number"], pr["head"])

        while True:
            try:
                remaining = deadline - asyncio.get_running_loop().time()
                found = await asyncio.to_thread(readback, remaining)
            except PRHeadChanged:
                return result("stale", reason="PR head changed while checks were pending")
            except (RuntimeError, subprocess.TimeoutExpired, OSError, ValueError) as exc:
                diagnostic = str(exc)[:500]
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return result("pending", reason="CI deadline reached", diagnostic=diagnostic)
                await asyncio.sleep(min(delay, remaining))
                if patient:
                    delay = min(delay * 2, 60)
                continue
            observations = found["contexts"]
            matches = {}
            for item in observations:
                if item.get("__typename") == "CheckRun":
                    matches.setdefault(item.get("name"), []).append(item)
                elif item.get("__typename") == "StatusContext":
                    matches.setdefault(item.get("context"), []).append(item)
            # Preserve every identity. A name cannot choose between two jobs,
            # regardless of response order, conclusion, app or workflow.
            checks = {name: items[0] for name, items in matches.items()
                      if len(items) == 1 and items[0].get("__typename") == "CheckRun"}
            ambiguous = sorted(name for name in required if len(matches.get(name, [])) > 1)
            if ambiguous:
                return result("failed", complete=True, ambiguous=ambiguous,
                              reason="required CI check identity is ambiguous",
                              next_action="resolve_ambiguous_required_check_identity")
            failed = [
                name
                for name in sorted(required)
                if (checks.get(name, {}).get("status") == "COMPLETED"
                    and checks[name].get("conclusion") in {
                        "FAILURE", "CANCELLED", "TIMED_OUT", "SKIPPED", "NEUTRAL",
                        "ACTION_REQUIRED", "STALE", "STARTUP_FAILURE"})
            ]
            if failed:
                return result("failed", complete=True, failed=failed,
                              next_action="inspect_required_job_failure")
            if all(checks.get(name, {}).get("status") == "COMPLETED"
                   and checks[name].get("conclusion") == "SUCCESS" for name in required):
                return result("passed", complete=True, head=pr["head"],
                              next_action="verify_PR_eligibility_and_merge_gates_separately")
            if asyncio.get_running_loop().time() >= deadline:
                return result("pending", complete=True, reason="CI deadline reached",
                              missing=sorted(required - checks.keys()),
                              next_action="observe_required_CI_on_the_same_candidate")
            remaining = deadline - asyncio.get_running_loop().time()
            await asyncio.sleep(min(delay, remaining))
            if patient:
                delay = min(delay * 2, 60)
