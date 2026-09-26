"""Validated Git, GitHub and check effects for an admitted delivery."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .candidate import candidate_for
from .contracts import canonical_json
from .delivery_sandbox import prepare_native_check
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
        result = {
            "checkout": str(self.checkout),
            "candidate": candidate,
            "provenance": provenance,
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
                binary = Path(self.spec["policy"]["codex_bin"])
                if not binary.is_file() or _sha256(binary) != self.spec["policy"].get(
                    "codex_bin_sha256"
                ):
                    raise ValueError("Codex executable changed after check sandbox attestation")
                profile, check_env = prepare_native_check(self.spec, checkout, evidence_dir, check)
                # The CLI applies the selected OS sandbox before execing the
                # candidate-controlled command and its descendants. Its home
                # contains no provider or GitHub credentials.
                command = [str(binary), "sandbox", "-P", profile, "-C", str(cwd), *argv]
            elif self.spec["provider"] == "fake":
                # Explicit fixture provider only; real deliveries never take
                # this unsandboxed path.
                check_env = {"PATH": os.environ.get("PATH", ""), "CI": "1"}
                command = argv
            else:
                raise ValueError("unknown delivery provider")
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
            artifact = evidence_dir / f"{check['id']}.log"
            artifact.write_text(output, encoding="utf-8")
            os.chmod(artifact, 0o600)
            count = None
            if check.get("test_count_regex"):
                import re

                numbers = re.findall(check["test_count_regex"], output)
                count = sum(int(number) for number in numbers) if numbers else 0
            rejected_output = False
            if check.get("reject_regex"):
                import re

                rejected_output = re.search(check["reject_regex"], output) is not None
            passed = (
                checked.returncode == 0
                and (count is None or count >= int(check.get("min_tests", 1)))
                and not rejected_output
            )
            results.append(
                {
                    "id": check["id"],
                    "argv": argv,
                    "cwd": str(cwd),
                    "exit_code": checked.returncode,
                    "test_count": count,
                    "rejected_output": rejected_output,
                    "passed": passed,
                    "log": str(artifact),
                    "log_sha256": _sha256(artifact),
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

    def _existing_pr(self) -> dict[str, Any] | None:
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
                raise RuntimeError("published PR changed after its durable effect receipt")
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
        found = self._existing_pr()
        if found is None or found["headRefOid"] != head:
            raise RuntimeError("published PR head did not read back at the expected commit")
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
