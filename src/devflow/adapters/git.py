"""Git argv adapter. Ownership is recorded in Git metadata, never user files.

Runners use subprocess.run's keyword contract and return CompletedProcess. No
cleanup/reset operation is exposed. Callers persist their action before invoking
create_worktree, and retain its ownership token in the private execution store.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from devflow.errors import WorkflowError


class GitRepository:
    def __init__(self, path: str | Path, runner: Callable | None = None):
        self.path = Path(path).resolve()
        self.runner = runner or subprocess.run
        self._mutation_may_have_applied = False

    def _run(self, *args: str, allowed: tuple[int, ...] = (0,)) -> subprocess.CompletedProcess:
        try:
            result = self.runner(
                ["git", "-C", str(self.path), *args],
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise WorkflowError(
                "git_transport", "Git execution failed; reconcile before retry"
            ) from exc
        if result.returncode not in allowed:
            # stderr can contain remote URLs with embedded credentials.
            raise WorkflowError("git_failed", "Git operation failed", {"operation": args[0]})
        return result

    def resolve(self, ref: str, kind: str = "commit") -> str:
        if kind not in {"commit", "tree"} or not ref or ref.startswith("-"):
            raise WorkflowError("invalid_ref", "Invalid Git revision")
        value = self._run(
            "rev-parse", "--verify", "--end-of-options", f"{ref}^{{{kind}}}"
        ).stdout.strip()
        if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", value):
            raise WorkflowError("invalid_git_output", "Git returned an invalid object ID")
        return value

    @staticmethod
    def _remote_identity(remote: str) -> str | None:
        if remote:
            if "://" in remote:
                parsed = urlsplit(remote)
                if parsed.password or (
                    parsed.username and not (parsed.scheme == "ssh" and parsed.username == "git")
                ):
                    raise WorkflowError(
                        "credential_remote", "Remote URLs containing credentials are unsupported"
                    )
                host, name = parsed.hostname, parsed.path.lstrip("/")
            else:
                match = re.fullmatch(r"(?:git@)?([A-Za-z0-9.-]+):([^?#]+)", remote)
                if not match:
                    # A local transport has no public repository identity.
                    host, name = None, None
                else:
                    host, name = match.groups()
            if host:
                name = name.removesuffix(".git").rstrip("/")
                if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", name):
                    raise WorkflowError(
                        "invalid_remote", "Remote repository identity is unsupported"
                    )
                return (
                    "github:" if host.lower() == "github.com" else f"git:{host.lower()}/"
                ) + name.lower()
        return None

    def identity(self) -> str:
        """Stable remote or common-Git-directory identity across linked worktrees."""
        remote = self._run("config", "--get", "remote.origin.url", allowed=(0, 1)).stdout.strip()
        identity = self._remote_identity(remote)
        if identity is not None:
            return identity
        common = self._run("rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
        return "local:" + str(Path(common).resolve())

    def _transport_identity(self, remote: str) -> str:
        identity = self._remote_identity(remote)
        if identity is not None:
            return identity
        parsed = urlsplit(remote)
        if parsed.scheme == "file" and not parsed.netloc and not parsed.query and not parsed.fragment:
            from urllib.parse import unquote

            path = Path(unquote(parsed.path))
        elif not parsed.scheme:
            path = Path(remote)
        else:
            raise WorkflowError("push_remote_conflict", "Unsupported origin transport identity")
        return "file:" + str((self.path / path).resolve())

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        a, b = self.resolve(ancestor), self.resolve(descendant)
        return self._run("merge-base", "--is-ancestor", a, b, allowed=(0, 1)).returncode == 0

    def observe(self) -> dict:
        head = self.resolve("HEAD")
        status = self._run("status", "--porcelain=v1", "--untracked-files=all").stdout
        return {
            "path": str(self.path),
            "head_sha": head,
            "tree_sha": self.resolve(head, "tree"),
            "clean": not bool(status),
        }

    def _push_target(self, head_ref: str) -> tuple[str, str]:
        from devflow.domain.endpoints import validate_endpoint

        validate_endpoint({"kind": "pr", "target": head_ref})
        ref = "refs/heads/" + head_ref
        self._run("check-ref-format", ref)
        configured = self._run("config", "--get-all", "remote.origin.url").stdout.splitlines()
        pushurl = self._run("config", "--get-all", "remote.origin.pushurl",
                            allowed=(0, 1)).stdout.splitlines()
        fetch = self._run("remote", "get-url", "--all", "origin").stdout.splitlines()
        push = self._run("remote", "get-url", "--push", "--all", "origin").stdout.splitlines()
        if (len(configured) != 1 or pushurl or len(fetch) != 1 or push != fetch
                or not configured[0] or configured[0].startswith("-")
                or self._transport_identity(configured[0]) != self._transport_identity(fetch[0])):
            raise WorkflowError("push_remote_conflict", "Origin must have one identical fetch/push URL")
        # Validate Git's effective route against the admitted repository, then
        # submit the original URL so Git applies insteadOf exactly once. An
        # explicit pushurl is unsupported: it can mask pushInsteadOf during the
        # named-remote readback while a literal-URL push follows that rewrite.
        return ref, configured[0]

    def _read_remote_head(self, url: str, ref: str) -> str | None:
        output = self._run("ls-remote", "--refs", "--exit-code", url, ref,
                           allowed=(0, 2)).stdout.splitlines()
        if not output:
            return None
        matches = [line.split("\t") for line in output]
        if (len(matches) != 1 or len(matches[0]) != 2 or matches[0][1] != ref
                or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", matches[0][0])):
            raise WorkflowError("invalid_git_output", "Remote ref readback was not unique and exact")
        return matches[0][0]

    def reconcile_push(self, *, head_ref: str, expected_head: str,
                       remote_head_sha: str | None) -> dict | None:
        """Read-only recovery: an absent/divergent ref never authorizes another push."""
        ref, url = self._push_target(head_ref)
        actual = self._read_remote_head(url, ref)
        if actual != expected_head:
            return None
        return {"status": "pushed", "head_ref": head_ref, "remote_head_sha": actual,
                "external_id": ref, "independent_readback": True}

    def push_branch(self, *, head_ref: str, expected_head: str,
                    remote_head_sha: str | None) -> dict:
        """Publish one exact source SHA with an exact old-ref lease, never rewrite history."""
        ref, url = self._push_target(head_ref)
        if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", expected_head):
            raise WorkflowError("invalid_sha", "Push requires an exact source object ID")
        if remote_head_sha is not None and not re.fullmatch(
                r"[a-f0-9]{40}|[a-f0-9]{64}", remote_head_sha):
            raise WorkflowError("invalid_sha", "Push requires an exact old remote object ID or null")
        observed = self.observe()
        if (self._run("symbolic-ref", "--quiet", "HEAD").stdout.strip() != ref
                or self.resolve(ref) != expected_head or observed["head_sha"] != expected_head
                or not observed["clean"]):
            raise WorkflowError("candidate_drift", "Push source differs from the clean candidate branch")
        actual = self._read_remote_head(url, ref)
        if actual == expected_head:
            return {"status": "pushed", "head_ref": head_ref, "remote_head_sha": actual,
                    "external_id": ref, "independent_readback": True}
        if actual != remote_head_sha:
            raise WorkflowError("stale_remote", "Remote branch differs from its expected old head")
        if actual is not None and not self.is_ancestor(actual, expected_head):
            raise WorkflowError("non_fast_forward", "Push cannot replace unrelated remote history")
        self._mutation_may_have_applied = True
        try:
            result = self._run("-c", "push.followTags=false", "push", "--porcelain",
                               "--no-follow-tags", "--recurse-submodules=no",
                               f"--force-with-lease={ref}:{remote_head_sha or ''}",
                               url, f"{expected_head}:{ref}", allowed=(0, 1))
        except WorkflowError:
            # The server may have accepted the update before transport failed.
            result = None
        if result is not None and result.returncode:
            # Only a complete per-ref porcelain rejection proves nonexecution.
            rejected = [line.split("\t") for line in result.stdout.splitlines()
                        if line.startswith("!\t")]
            if (len(rejected) == 1 and len(rejected[0]) == 3
                    and rejected[0][1] == f"{expected_head}:{ref}"
                    and rejected[0][2].startswith(("[rejected]", "[remote rejected]"))):
                self._mutation_may_have_applied = False
                raise WorkflowError("push_rejected", "Remote rejected the exact branch update",
                                    {"no_mutation": True})
        # Independent read even after a successful process or a lost response.
        actual = self._read_remote_head(url, ref)
        if actual != expected_head:
            raise WorkflowError("ambiguous_git_action", "Push outcome unresolved; reconcile this action")
        return {"status": "pushed", "head_ref": head_ref, "remote_head_sha": actual,
                "external_id": ref, "independent_readback": True}

    def _ownership_file(self, action_id: str) -> Path:
        common = self._run("rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
        directory = Path(common) / "devflow-workspaces"
        directory.mkdir(mode=0o700, exist_ok=True)
        return directory / (hashlib.sha256(action_id.encode()).hexdigest() + ".json")

    @contextmanager
    def _workspace_lock(self):
        directory = self._ownership_file("lock").parent
        with (directory / ".lock").open("a") as stream:
            (directory / ".lock").chmod(0o600)
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def create_worktree(
        self, path: str | Path, branch: str, base_ref: str, *, action_id: str, ownership_token: str
    ) -> dict:
        with self._workspace_lock():
            return self._create_worktree(
                path, branch, base_ref, action_id=action_id, ownership_token=ownership_token
            )

    def _create_worktree(
        self, path: str | Path, branch: str, base_ref: str, *, action_id: str, ownership_token: str
    ) -> dict:
        """Reserve and create an owned worktree, or reconcile the same reservation.

        Existing paths are never adopted. A failed command retains the reservation
        so an interrupted successful creation can be independently identified.
        """
        if not action_id or not ownership_token or not branch or branch.startswith("-"):
            raise WorkflowError(
                "invalid_workspace", "Action, ownership token and branch are required"
            )
        self._run("check-ref-format", "--branch", branch)
        target = Path(path).absolute()
        if target.is_symlink() or target.resolve() != target:
            raise WorkflowError(
                "workspace_conflict", "Workspace path must have no symlink components"
            )
        base = self.resolve(base_ref)
        reservation = {
            "action_id": action_id,
            "path": str(target),
            "branch": branch,
            "base_sha": base,
            "token_hash": hashlib.sha256(ownership_token.encode()).hexdigest(),
        }
        marker = self._ownership_file(action_id)
        if marker.exists():
            if json.loads(marker.read_text()) != reservation:
                raise WorkflowError("workspace_conflict", "Workspace action identity was reused")
            if target.exists():
                self._verify_owned(target, reservation)
                return reservation | {"status": "confirmed"}
        else:
            if target.exists():
                raise WorkflowError(
                    "workspace_conflict", "Existing path is not owned by this action"
                )
            for existing in marker.parent.glob("*.json"):
                if json.loads(existing.read_text()).get("path") == str(target):
                    raise WorkflowError(
                        "workspace_conflict", "Workspace is reserved by another action"
                    )
            with marker.open("x") as stream:
                marker.chmod(0o600)
                json.dump(reservation, stream, sort_keys=True)
        self._run("worktree", "add", "-b", branch, str(target), base)
        self._verify_owned(target, reservation)
        return reservation | {"status": "confirmed"}

    def _verify_owned(self, target: Path, reservation: dict) -> None:
        entries = self._run("worktree", "list", "--porcelain", "-z").stdout
        matches = []
        for block in entries.split("\0\0"):
            fields = dict(part.split(" ", 1) for part in block.split("\0") if " " in part)
            if fields.get("worktree") == str(target):
                matches.append(fields)
        if len(matches) != 1 or matches[0].get("branch") != "refs/heads/" + reservation["branch"]:
            raise WorkflowError(
                "workspace_conflict", "Reserved path is not the expected Git worktree"
            )
        repository = GitRepository(target, self.runner)
        if not repository.is_ancestor(reservation["base_sha"], "HEAD"):
            raise WorkflowError(
                "workspace_conflict", "Owned worktree no longer contains its admitted base"
            )

    def register_checkout(
        self, *, action_id: str, ownership_token: str, expected_head: str, base_ref: str
    ) -> dict:
        with self._workspace_lock():
            return self._register_checkout(
                action_id=action_id,
                ownership_token=ownership_token,
                expected_head=expected_head,
                base_ref=base_ref,
            )

    def _register_checkout(
        self, *, action_id: str, ownership_token: str, expected_head: str, base_ref: str
    ) -> dict:
        """Explicitly enroll an existing dedicated checkout under caller authority."""
        if not action_id or not ownership_token:
            raise WorkflowError("invalid_workspace", "Action and ownership token are required")
        branch = self._run("symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip()
        if branch in {"main", "master"}:
            raise WorkflowError(
                "workspace_conflict", "Canonical branch cannot become an owned task checkout"
            )
        observed = self.observe()
        base = self.resolve(base_ref)
        if observed["head_sha"] != expected_head or not observed["clean"]:
            raise WorkflowError(
                "workspace_conflict", "Checkout differs from its admitted clean head"
            )
        if not self.is_ancestor(base, expected_head):
            raise WorkflowError("workspace_conflict", "Checkout must include the admitted base")
        reservation = {
            "action_id": action_id,
            "path": str(self.path),
            "branch": branch,
            "base_sha": base,
            "token_hash": hashlib.sha256(ownership_token.encode()).hexdigest(),
        }
        marker = self._ownership_file(action_id)
        if marker.exists():
            if json.loads(marker.read_text()) != reservation:
                raise WorkflowError(
                    "workspace_conflict", "Checkout action was already bound differently"
                )
        else:
            for existing in marker.parent.glob("*.json"):
                if json.loads(existing.read_text()).get("path") == str(self.path):
                    raise WorkflowError(
                        "workspace_conflict", "Checkout is already owned by another action"
                    )
            with marker.open("x") as stream:
                marker.chmod(0o600)
                json.dump(reservation, stream, sort_keys=True)
        self._verify_owned(self.path, reservation)
        return reservation | {"status": "confirmed"}

    def verify_ownership(self, ownership_token: str) -> dict:
        common = self._run("rev-parse", "--path-format=absolute", "--git-common-dir").stdout.strip()
        token_hash = hashlib.sha256(ownership_token.encode()).hexdigest()
        matches = []
        for marker in (Path(common) / "devflow-workspaces").glob("*.json"):
            record = json.loads(marker.read_text())
            if record.get("path") == str(self.path) and record.get("token_hash") == token_hash:
                matches.append(record)
        if len(matches) != 1:
            raise WorkflowError(
                "workspace_not_owned", "Checkout is not registered to this ownership token"
            )
        self._verify_owned(self.path, matches[0])
        return matches[0]

    def snapshot(
        self,
        *,
        candidate_id: str,
        attempt_id: str,
        scope_hash: str,
        base_ref: str,
        dependency_hash: str,
        environment_hash: str,
        ownership_token: str,
    ) -> dict:
        self.verify_ownership(ownership_token)
        base = self.resolve(base_ref)
        before = self.observe()
        if not before["clean"]:
            raise WorkflowError(
                "dirty_candidate", "Candidate checkout has uncommitted or untracked files"
            )
        if not self.is_ancestor(base, before["head_sha"]):
            raise WorkflowError("stale_candidate", "Candidate must contain its evaluated base")
        after = self.observe()
        if (
            before != after
            or self.resolve("HEAD") != before["head_sha"]
            or self.resolve(base_ref) != base
        ):
            raise WorkflowError("candidate_race", "Candidate or base changed during capture")
        return {
            "schema_version": 1,
            "record_type": "candidate",
            "candidate_id": candidate_id,
            "attempt_id": attempt_id,
            "scope_hash": scope_hash,
            "repository": self.identity(),
            "base_sha": base,
            "head_sha": before["head_sha"],
            "tree_sha": before["tree_sha"],
            "clean": True,
            "dependency_hash": dependency_hash,
            "environment_hash": environment_hash,
            "created_at": datetime.now(UTC).isoformat(),
        }
