"""Read-only custody of the current, possibly unpassed, retained worker subject.

An original stopped admission and a terminal checkpoint are historical facts.
Only the ordinary stopped-worker selector may establish the current tree. Native
progress receipts explain their differences; they do not establish a product pass.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from .contracts import digest
from .delivery_broker import DeliveryBroker, _git, _run
from .delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from .delivery_resources import _ancestors, _gate_evidence_root

CANDIDATE_FIELDS = ("id", "head", "content_sha256", "base_sha", "environment_digest",
                    "policy_digest")
ROLE_FIELDS = {"role", "iteration", "candidate", "attempt_id", "input_candidate_id", "provider"}


class _ReadOnlyRegistry(ExecutionRegistry):
    """An existing registry view: no schema, directory, lock or journal creation."""
    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def connect(self):
        _ancestors(self.path)
        if not self.path.is_absolute() or self.path.resolve(strict=True) != self.path:
            raise ValueError("retained observation requires the existing canonical registry")
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
                raise ValueError("retained observation registry is not a private owned file")
            header = os.read(fd, 100)
            if (len(header) != 100 or header[:16] != b"SQLite format 3\x00"
                    or header[18:20] != b"\x01\x01"
                    or any(os.path.lexists(str(self.path) + suffix)
                           for suffix in ("-wal", "-shm", "-journal"))):
                # A read-only SQLite connection can create WAL sidecars. Do not
                # use immutable=1: it can hide committed, uncheckpointed rows.
                raise ValueError("retained observation requires a settled rollback registry")
            db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=30)
            try:
                observed = self.path.lstat()
                keys = ("st_dev", "st_ino", "st_mode", "st_uid", "st_nlink")
                if any(getattr(observed, key) != getattr(info, key) for key in keys):
                    raise ValueError("retained observation registry was replaced")
                db.row_factory = sqlite3.Row
                db.execute("PRAGMA query_only=ON")
                db.execute("BEGIN")
                yield db
            finally:
                db.close()
        finally:
            os.close(fd)

    def checkpoints(self, issue_id):
        with self.connect() as db:
            values = {}
            for row in db.execute("SELECT checkpoint_key,content_digest,content_json "
                                  "FROM execution_checkpoints WHERE issue_id=?", (issue_id,)):
                value = json.loads(row["content_json"])
                if digest(value) != row["content_digest"]:
                    raise OwnershipConflict("retained observation checkpoint digest changed")
                values[row["checkpoint_key"]] = value
            return values


class ObservationBroker(DeliveryBroker):
    """Preserve broker PR fences without invoking feature reconciliation."""
    def __init__(self, store, spec, *, revision=None, resource_spec=None):
        if resource_spec is not None:
            from .delivery_preparation import require_native_execution
            from .delivery_resources import _gate_roots

            bindings = {}
            _gate_roots(resource_spec, execution_specs=bindings)
            if spec not in bindings.values():
                raise ValueError("historical observer lost its admitted execution")
            require_native_execution(spec)
        super().__init__(store, resource_spec or spec)
        self.spec = spec
        self.observed_revision = revision
        self.observed_record = None

    def _record(self):
        from .delivery_feature_publication import live_members, publication_child_binding
        from .delivery_feature_revisions import adopted_plan_identity
        from .delivery_github_contract import GitHubDelivery
        from .delivery_github_plans import verify_parent_binding

        if self.observed_record is not None:
            return self.observed_record
        feature = self.spec["feature_delivery"]
        issue = feature["snapshot"]["issue"]
        shared = _ReadOnlyRegistry(feature["registry"])
        values = shared.checkpoints(issue["id"])
        current = shared.current(issue["id"])
        if (not current or current["store_path"] != str(self.store.config.tracking_db)
                or current["issue_id"] != feature["owner"]["issue_id"]
                or current["repository_id"] != issue["repository_id"]
                or current["issue_url"] != issue["url"]
                or current["state"] not in {"active", "draining", "stopped"}):
            raise OwnershipConflict("retained observation lost canonical feature ownership")
        identity = adopted_plan_identity(self.spec, shared)
        if not identity.get("plan_digest"):
            raise OwnershipConflict("retained observation lacks accepted parent definition proof")
        explicit = self.observed_revision or self.spec.get("feature_plan_revision")
        legacy = (feature["snapshot"].get("delivery") or {}).get("manifest", {})
        if ((explicit is not None and any(explicit.get(key) != identity.get(key)
                                         for key in ("plan_revision", "plan_digest")))
                or (explicit is None
                    and legacy.get("plan_revision", 1) != identity["plan_revision"])):
            raise OwnershipConflict("retained observation carries a superseded plan identity")
        reference = {key: identity.get(key) for key in ("comment_id", "comment_node_id")}
        if reference["comment_id"] is None:
            reference = values.get("github-record") or feature["snapshot"].get("delivery")
        if not reference or reference.get("comment_id") is None:
            raise OwnershipConflict("retained observation lacks canonical GitHub plan ownership")
        gh = GitHubDelivery()
        record = gh.load_record(issue, reference)
        manifest = record["manifest"]
        if manifest["version"] == 2:
            verify_parent_binding(gh, issue, reference)
        if (manifest.get("plan_revision", 1) != identity["plan_revision"]
                or digest(manifest["plan"]) != identity["plan_digest"]
                or not identity.get("workstream_issues")
                or identity["workstream_issues"] != manifest["workstream_issues"]):
            raise OwnershipConflict("retained observation's canonical definition changed")
        worker = self.spec["feature_worker"]
        binding = publication_child_binding(record, worker["chunk_id"])
        if (worker["workstream_id"] != binding["workstream_id"]
                or self.spec["issue_url"] != binding["issue_url"]):
            raise OwnershipConflict("retained observation lost its original child issue")
        live_members(self.spec, record, gh)
        self.observed_record = record
        return record

    def _read_owned_pr(self):
        if self.spec.get("feature_worker", {}).get("kind") != "chunk":
            return super()._read_owned_pr()
        members = [member for member in self._record()["manifest"]["publication"]["members"]
                   if member["chunk_id"] == self.spec["feature_worker"]["chunk_id"]]
        if len(members) != 1 or members[0]["branch"] != self.spec["branch"]:
            raise OwnershipConflict("retained observation lacks its unique canonical chunk PR")
        number = members[0]["number"]
        found = json.loads(_run([
            "gh", "pr", "view", str(number), "--repo", self.spec["github_repo"], "--json",
            "number,url,state,isDraft,baseRefName,headRefName,headRefOid,title",
        ], timeout=60))
        if (found["number"] != number or found["headRefName"] != self.spec["branch"]
                or found["isDraft"] or found["state"] != "OPEN"):
            raise ValueError("recorded chunk PR is no longer the owned open publication")
        return found

    def _legacy_published_base(self, found, *, read_only=True):
        return super()._legacy_published_base(found, read_only=True)

    def observe_unpublished(self):
        from .delivery_feature_publication import live_members
        from .delivery_policy_recovery import _remote

        previous = self.spec.get("feature_worker", {}).get("previous_publication")
        if not previous:
            return _remote(self)
        record = self._record()
        if (previous not in record["manifest"]["publication"]["members"]
                or previous["branch"] != self.spec["branch"]
                or _git(self.checkout, "branch", "--show-current")
                != self.spec.get("local_branch", self.spec["branch"])
                or _git(self.source, "remote", "get-url", "origin") != self.spec["origin_url"]
                or _git(self.checkout, "remote", "get-url", "--push", "origin")
                != self.spec["origin_url"]):
            raise OwnershipConflict("retained integration publication identity changed")
        raw = next(item for item in live_members(self.spec, record)
                   if item["number"] == previous["number"])
        remote = _git(self.source, "ls-remote", "origin", "refs/heads/" + previous["branch"])
        if raw["merged"] or raw["state"] != "open" or remote.split() != [
            previous["head"], "refs/heads/" + previous["branch"],
        ]:
            raise OwnershipConflict("retained integration PR is closed or its branch moved")


def full_candidate(value):
    if (not isinstance(value, dict) or any(not isinstance(value.get(key), str)
                                         or not value[key] for key in CANDIDATE_FIELDS)):
        raise ValueError("retained worker lost full candidate and policy custody")
    return {key: value[key] for key in CANDIDATE_FIELDS}


def bounded_findings(value):
    if (not isinstance(value, list) or not value or len(value) > 32
            or any(not isinstance(text, str) or not text.strip() for text in value)
            or sum(len(text.encode()) for text in value) > 16 * 1024):
        raise ValueError("unresolved product findings exceed bounded fresh review context")
    return list(value)


def _time(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("retained worker native progress has an unbound completion time")
    return result


def _private_receipt(path, *, canonical=False):
    """Read a stable private receipt once; do not create or chmod its ancestry."""
    _ancestors(path)
    if path.resolve(strict=True) != path:
        raise ValueError("retained native receipt left its canonical owned path")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > 16 * 1024 * 1024):
            raise ValueError("retained native receipt is not a bounded owned private file")
        raw = stream.read(16 * 1024 * 1024 + 1)
        after = os.fstat(stream.fileno())
        keys = ("st_dev", "st_ino", "st_size", "st_mode", "st_uid", "st_nlink", "st_mtime_ns")
        if len(raw) != before.st_size or any(getattr(before, k) != getattr(after, k) for k in keys):
            raise ValueError("retained native receipt changed during read-only observation")
    value = json.loads(raw)
    if not isinstance(value, dict) or canonical and raw != (
            json.dumps(value, sort_keys=True, indent=2) + "\n").encode():
        raise ValueError("retained native launcher lost its exact canonical bytes")
    return value


def _runtime(identity):
    from .delivery_feature_review_context import _protected_root, _sources

    if (not isinstance(identity, dict) or identity.get("dirty") is not False
            or identity.get("local_digest") is not None
            or not isinstance(identity.get("runtime_payload_sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", identity["runtime_payload_sha256"])):
        raise ValueError("retained progress needs its clean actual finished native runtime")
    _, payload = _sources(_protected_root(), identity.get("revision"))
    if payload != identity["runtime_payload_sha256"]:
        raise ValueError("retained progress actual runtime differs from protected Git payload")
    return {"revision": identity["revision"], "runtime_payload_sha256": payload}


def _native_role(spec, role, attempt, session, lower, upper, admitted_candidate):
    from .delivery_sandbox import _native_role_home
    from .delivery_session_custody import _same_assessment
    from .supervisor import DeliverySupervisor

    job = attempt["job_key"]
    if not isinstance(job, str) or not re.fullmatch(r"[0-9a-f]{64}", job):
        raise ValueError("retained progress has an invalid supervised job identity")
    folder = Path(spec["state_dir"]) / "attempts" / job
    request_path = folder / "request.json"
    request = _private_receipt(request_path, canonical=True)
    result = json.loads(attempt["result_json"] or "null")
    expected = {key: value for key, value in role.items() if key not in ROLE_FIELDS}
    workspace = (Path(spec["checkout"]) if role["role"] == "implement" else
                 _gate_evidence_root(spec) / "gates" / str(role["iteration"]) / role["role"])
    candidate = full_candidate(request.get("candidate"))
    if (result != expected or role["role"] not in {"implement", "review", "verify"}
            or attempt["state"] != "finished" or attempt["cleanup"] != "confirmed"
            or attempt["run_id"] != spec["run_id"]
            or request.get("spec") != spec or request.get("role") != role["role"]
            or request.get("iteration") != role["iteration"]
            or attempt["iteration"] != role["iteration"] or attempt["role"] != role["role"]
            or candidate["id"] != role.get("input_candidate_id")
            or candidate["id"] != attempt["candidate_id"]
            or request.get("revision_context") is not None
            or request.get("native_authorized") is not True
            or request.get("role_evidence_key") != job
            or DeliverySupervisor._job_key(request) != job
            or request.get("workspace") != str(workspace)
            or request.get("result_path") != str(folder / "result.json")
            or attempt["result_path"] != request["result_path"]
            or request.get("start_path") != str(folder / "start.json")
            or "ready_path" in request
            or candidate["base_sha"] != spec["base_sha"]
            or candidate["environment_digest"] != admitted_candidate["environment_digest"]
            or candidate["policy_digest"] != spec["policy_digest"]
            or not lower <= _time(attempt["started_at"]) <= _time(attempt["finished_at"]) <= upper):
        raise ValueError("retained native progress changed its exact supervised launcher or timing")
    raw = _private_receipt(folder / "result.json")
    journal = _private_receipt(folder / "native-process.json")
    launch = _private_receipt(folder / "launch.json")
    native = result.get("native_process") or {}
    intent, metadata = journal.get("intent") or {}, journal.get("provider_session") or {}
    argv = intent.get("argv") or []
    resumed = session if role["role"] == "implement" else None
    output = full_candidate(role.get("candidate"))
    if (not _same_assessment(raw, result) or result.get("finish_reason") != "done"
            or result.get("cleanup") != "confirmed" or not attempt["pid"]
            or not attempt["process_identity"] or not attempt["session_id"]
            or attempt["session_id"] != result.get("session_id")
            or request.get("resume_session") != resumed
            or (resumed is not None and attempt["session_id"] != resumed)
            or (resumed is None and attempt["session_id"] == session)
            or native.get("state") != "finished" or native.get("exit_code") != 0
            or native.get("cleanup") != "observed-native-confirmed"
            or native.get("monitoring_complete") is not True
            or native.get("stdio_drained") is not True
            or any(native.get(key) is not False for key in ("cancelled", "timed_out"))
            or native.get("journal") != str(folder / "native-process.json")
            or journal.get("phase") != "finished" or journal.get("monitoring_complete") is not True
            or journal.get("result") != native
            or journal.get("runtime_identity") != native.get("runtime_identity")
            or intent.get("run_id") != spec["run_id"]
            or intent.get("policy_digest") != spec["policy_digest"]
            or intent.get("cwd") != str(workspace)
            or intent.get("environment_sha256") != digest(launch.get("environment"))
            or argv != launch.get("argv") or len(argv) != 5
            or argv[1:4] != ["-I", "-m", "devflow_temporal.role_runner"]
            or argv[4] != str(request_path) or not Path(argv[0]).is_absolute()
            or launch.get("environment", {}).get("CODEX_HOME")
            != str(_native_role_home(request) / "codex")
            or journal.get("owned", {}).get(str(attempt["pid"]), {}).get("identity")
            != attempt["process_identity"]
            or metadata.get("session_id") != attempt["session_id"]
            or metadata.get("resumed_from") != resumed
            or metadata.get("role") != role["role"]
            or metadata.get("iteration") != role["iteration"]
            or metadata.get("result_digest") != digest(result)
            or metadata.get("output_candidate") != {key: output[key]
                                                   for key in ("id", "head", "content_sha256")}
            or output["base_sha"] != spec["base_sha"]
            or output["environment_digest"] != admitted_candidate["environment_digest"]
            or output["policy_digest"] != spec["policy_digest"]):
        raise ValueError("retained progress lost its actual native result, session or monitor")
    return {"job_key": job, "request_sha256": digest(request), "result_sha256": digest(result),
            "native_journal_sha256": digest(journal), "launch_sha256": digest(launch),
            "runtime_identity": _runtime(native.get("runtime_identity"))}


def _progress(store, spec, row, attempts, recovery, seal):
    from .delivery_feature_revision_recovery import retained_recovery
    from .delivery_stopped_resume import KIND, custody

    transport = seal.get("original_recovery")
    if (not recovery or recovery.get("kind") != KIND
            or recovery.get("execution_spec") != spec
            or not transport or retained_recovery(transport) != recovery
            or spec.get("provider") != "codex"
            or spec.get("policy", {}).get("execution_backend") != "native-macos"):
        raise ValueError("retained worker progress lacks its original native stopped admission")
    with store._connect() as db:
        # The command, admission file and Temporal memo seal the exact compact
        # transport. Its separately authenticated local custody retains history
        # that was intentionally not sent to this workflow execution.
        if custody(db, transport) != spec:
            raise ValueError("retained worker progress changed original admission authority")
        command = db.execute("SELECT response_json FROM delivery_commands WHERE command_id=? "
                             "AND run_id=?", (recovery["command"]["command_id"], spec["run_id"])
                             ).fetchone()
    response = json.loads(command[0])
    state, closed = seal["state"], seal["closed"]
    prior = transport["state"]
    lower, upper = _time(recovery["closed"]["closed_at"]), _time(closed["closed_at"])
    roles, prefix = state.get("roles", []), prior.get("roles", [])
    maximum = recovery["maximum_iteration"]
    if (type(maximum) is not int or maximum > spec["policy"]["max_repairs"]
            or not prior["iteration"] < state["iteration"] <= maximum
            or state["revision"] != row["protocol_revision"]
            or response.get("workflow_id") != closed["workflow_id"]
            or response.get("authorized_through_iteration") != maximum
            or response.get("admission_sha256") != closed["recovery_digest"]
            or closed["recovery_digest"] != digest(transport)
            or not isinstance(closed.get("execution_run_id"), str)
            or not closed["execution_run_id"] or lower > upper
            or len(roles) > (maximum + 1) * 3 or not prefix
            or roles[:len(prefix)] != prefix or len(roles) <= len(prefix)
            or prior.get("run_id") != spec["run_id"]
            or recovery["command_digest"] != digest({"run_id": spec["run_id"],
                                                      **recovery["command"]})
            or full_candidate(recovery["execution_candidate"])["id"]
            != recovery["command"]["expected_candidate_id"]
            or recovery["execution_candidate"]["head"]
            != recovery["command"]["expected_candidate_head"]
            or recovery["command"]["expected_iteration"] != prior["iteration"]
            or not isinstance(recovery.get("session_id"), str) or not recovery["session_id"]
            or recovery["session_id"] != seal["session_id"]):
        raise ValueError("retained worker progress lost its closed admission prefix or ceiling")
    used, proofs = set(), []
    for role in roles[len(prefix):]:
        result = {key: value for key, value in role.items() if key not in ROLE_FIELDS}
        matches = [a for a in attempts if a["role"] == role.get("role")
                   and a["iteration"] == role.get("iteration")
                   and a["session_id"] == role.get("session_id")
                   and json.loads(a["result_json"] or "null") == result]
        if (len(matches) != 1 or matches[0]["job_key"] in used
                or type(role.get("iteration")) is not int
                or not prior["iteration"] < role["iteration"] <= maximum):
            raise ValueError("retained native progress is duplicated, missing or foreign")
        used.add(matches[0]["job_key"])
        proofs.append(_native_role(spec, role, matches[0], recovery["session_id"], lower, upper,
                                   full_candidate(recovery["execution_candidate"])))
    progressed = {a["job_key"] for a in attempts
                  if a["iteration"] > prior["iteration"] and _time(a["started_at"]) >= lower}
    if used != progressed:
        raise ValueError("retained closed progress omitted a supervised native attempt")
    return {"admission_sha256": digest(transport), "admission_custody_sha256": digest(recovery),
            "admission_candidate":
            full_candidate(recovery["execution_candidate"]), "prefix_sha256": digest(prefix),
            "closed_result_sha256": digest(state), "closure": {
                key: closed[key] for key in ("workflow_id", "execution_run_id", "closed_at",
                                             "request_digest", "recovery_digest")},
            "native_progress_sha256": digest(proofs)}


def retained_worker_subject(store, original, row, attempts, recovery):
    """Establish full ordinary continuation prerequisites before review reservation."""
    from .delivery_stopped_resume import review_snapshot

    seal = review_snapshot(store, original["run_id"])
    if (seal["predecessor_spec"] != original or dict(seal["row"]) != dict(row)
            or list(map(dict, seal["attempts"])) != list(map(dict, attempts))
            or recovery and recovery.get("execution_spec") != original):
        raise ValueError("retained worker changed during read-only subject observation")
    terminal = full_candidate(seal["state"]["candidate"])
    current = full_candidate(seal["candidate"])
    implementations = [role for role in seal["state"].get("roles", [])
                       if role.get("role") == "implement"]
    latest = implementations[-1] if implementations else None
    unpassed = bool(latest and latest.get("status") != "pass")
    mismatch = (recovery and full_candidate(recovery["execution_candidate"]) != terminal
                or current != terminal or unpassed)
    proof = _progress(store, original, row, attempts, recovery, seal) if mismatch else None
    disposition = None
    if current != terminal or unpassed:
        if (not latest or latest.get("status") != "findings"
                or latest.get("finish_reason") != "done" or latest.get("cleanup") != "confirmed"
                or latest.get("iteration") != seal["state"]["iteration"]
                or not latest.get("session_id") or latest["session_id"] != seal["session_id"]
                or full_candidate(latest.get("candidate")) != current):
            raise ValueError("current retained review subject is not an exact finished finding")
        findings = bounded_findings(latest.get("findings"))
        if seal["state"]["iteration"] >= original["policy"]["max_repairs"]:
            raise ValueError("unpassed retained worker has no ordinary implementation turn left")
        disposition = {"kind": "latest_finished_implementation_findings", "status": "findings",
                       "finish_reason": "done", "candidate": current,
                       "session_id": seal["session_id"], "unresolved_product_findings": findings,
                       "unresolved_product_findings_sha256": digest(findings)}
    prerequisites = {"row_sha256": digest(dict(row)),
                     "attempts_sha256": digest(list(map(dict, attempts))),
                     "effects_sha256": digest(seal["effects"]),
                     "cleanup_digest": seal["cleanup_digest"], "work_binding": seal["work_binding"],
                     "session_custody": seal.get("session_custody"),
                     "publication": seal.get("publication"), "closed_result_sha256":
                     digest(seal["state"])}
    return {"candidate": current, "terminal_candidate": terminal,
            "current_review_subject": disposition, "progress_custody": proof,
            "continuation_prerequisites_sha256": digest(prerequisites)}
