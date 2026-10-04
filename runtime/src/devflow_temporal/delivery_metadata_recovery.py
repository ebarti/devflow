"""One explicit stopped-run metadata reconciliation, followed only by fresh gates."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git, _run, conventional_subject
from .delivery_continuation import session_state_digest
from .delivery_metadata_contract import evidence_applicability as evidence_applicability
from .delivery_policy_recovery import _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock
from .delivery_resources import private_directory, read_private


def _immutable(path, value, *, raw=None):
    data = raw if raw is not None else (canonical_json(value) + "\n").encode()
    if canonical_json(json.loads(data)) != canonical_json(value):
        raise ValueError("immutable raw evidence differs from its typed value")
    stage = path.with_name(".stage-" + path.name + "-" + hashlib.sha256(data).hexdigest())

    def staging():
        info = stage.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink not in (1, 2)
        ):
            raise ValueError("metadata journal staging identity is unsafe")
        return info

    if path.exists():
        info = path.lstat()
        if info.st_nlink == 2 and stage.exists():
            staged = staging()
            if (staged.st_dev, staged.st_ino) != (info.st_dev, info.st_ino):
                raise ValueError("metadata journal has an unrelated hardlink")
            if path.read_bytes() != data:
                raise ValueError("metadata staged journal changed")
            stage.unlink()
        if canonical_json(read_private(path)) != canonical_json(value) or path.read_bytes() != data:
            raise ValueError("metadata immutable journal changed")
        return
    if stage.exists():
        info = staging()
        if info.st_nlink != 1 or not data.startswith(stage.read_bytes()):
            raise ValueError("metadata unfinished staging bytes are not attributable")
        stage.unlink()  # Only this hash-named unpublished staging prefix, never a receipt.
    fd = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(stage, path)
    stage.unlink()
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def preserve_resources(root, spec):
    """Before a new resource generation, preserve exact old bytes and hashes."""
    if spec.get("resource_cleanup_version") != 1:
        return
    target = root / "predecessor-resources"
    private_directory(target)
    for name in ("manifest.json", "finalization.json"):
        source = Path(spec["state_dir"]) / "resources" / name
        _immutable(target / name, read_private(source), raw=source.read_bytes())


def _authority(payload, spec):
    reference = Path(payload["authority_path"])
    info = reference.lstat()
    if (
        not reference.is_absolute()
        or reference.is_symlink()
        or not reference.is_file()
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or info.st_mode & 0o022
        or info.st_size > 128 * 1024
    ):
        raise ValueError("metadata authority reference is unsafe")
    raw = reference.read_bytes()
    if hashlib.sha256(raw).hexdigest() != payload["authority_sha256"]:
        raise ValueError("metadata authority reference changed")
    value = json.loads(raw)
    scope = value.get("scope", {})
    eligible = scope.get("eligible_original_run_ids", [])
    if (
        value.get("decision_owner") != "main task"
        or value.get("new_user_approval_required") is not False
        or not isinstance(value.get("authority_source"), str)
        or not value["authority_source"]
        or not isinstance(eligible, list)
        or not 1 <= len(eligible) <= 2
        or any(not isinstance(item, str) or not item for item in eligible)
        or len(set(eligible)) != len(eligible)
        or spec["run_id"] not in eligible
        or type(scope.get("max_reconciliation_commands_per_original_run")) is not int
        or scope["max_reconciliation_commands_per_original_run"] != 1
        or scope.get("exact_duplicate_and_known_interrupted_effect_resume_allowed") is not True
        or scope.get("known_base") != spec["base_sha"]
        or scope.get("repository", "").casefold() != "github.com/" + spec["github_repo"].casefold()
    ):
        raise ValueError("metadata authority does not bind this original run")
    return value


def _identity(broker):
    author = _git(broker.checkout, "var", "GIT_AUTHOR_IDENT")
    committer = _git(broker.checkout, "var", "GIT_COMMITTER_IDENT")
    name = author.rsplit(" ", 2)[0]
    if name != committer.rsplit(" ", 2)[0] or not re.fullmatch(r"[^<>\n]+ <[^<>\s]+>", name):
        raise ValueError("existing human publication signer is not recognized")
    return name, committer


def _range(broker, state, effects, signer, committer, recovery=None):
    base = broker.spec["base_sha"]
    commits = _git(broker.checkout, "rev-list", "--reverse", base + "..HEAD").splitlines()
    if not 1 <= len(commits) <= 16:
        raise ValueError("metadata recovery requires a bounded published commit range")
    published = {}
    for effect in effects:
        if effect["kind"] != "publish":
            continue
        request, receipt = json.loads(effect["request_json"]), json.loads(effect["observed_json"])
        role = next(
            (
                role
                for role in state["roles"]
                if role.get("role") == "implement"
                and canonical_json(role.get("iteration"))
                == canonical_json(request.get("iteration"))
            ),
            None,
        )
        if role is None and isinstance(recovery, dict) and (
            recovery.get("kind") == "execution_policy_recovery"
            and type(request.get("iteration")) is int
            and type(recovery.get("start_iteration")) is int
            and request["iteration"] == recovery.get("start_iteration")
            and type(recovery.get("state", {}).get("iteration")) is int
            and recovery["state"]["iteration"] + 1 == request["iteration"]
            and recovery.get("candidate", {}).get("id") == request.get("input_candidate_id")
        ):
            predecessor = next((r for r in reversed(recovery["state"]["roles"])
                                if r.get("role") == "implement"
                                and canonical_json(r.get("iteration"))
                                == canonical_json(recovery["state"]["iteration"])), None)
            if predecessor and any(canonical_json(r) == canonical_json(predecessor)
                                   for r in state["roles"]):
                frozen = predecessor.get("candidate", {})
                retained = recovery["candidate"]
                if all(canonical_json(frozen.get(k)) == canonical_json(retained.get(k))
                       for k in ("id", "head", "content_sha256", "base_sha",
                                 "environment_digest")):
                    role = predecessor
        candidate = receipt.get("candidate", {})
        if (
            not role
            or role.get("input_candidate_id") is None
            or role.get("candidate", {}).get("id") != request.get("input_candidate_id")
            or role["candidate"].get("content_sha256") != candidate.get("content_sha256")
            or receipt.get("base") != base
            or receipt.get("head") != candidate.get("head")
            or candidate.get("id")
            != hashlib.sha256(
                f"{candidate.get('head')}:{candidate.get('content_sha256')}".encode()
            ).hexdigest()
        ):
            raise ValueError("published range lacks its authentic controller candidate proof")
        published[receipt["head"]] = receipt
    if set(commits) != set(published):
        raise ValueError("published range contains a foreign or unproven commit")
    mapping, old_parent, new_parent = [], base, base
    for old in commits:
        raw = subprocess.run(
            ["git", "-C", str(broker.checkout), "cat-file", "commit", old],
            check=True,
            capture_output=True,
            timeout=30,
        ).stdout.decode("utf-8")
        header, message = raw.split("\n\n", 1)
        parents = re.findall(r"(?m)^parent ([0-9a-f]{40})$", header)
        author = re.search(r"(?m)^author (.+)$", header)
        tree = re.search(r"(?m)^tree ([0-9a-f]{40})$", header)
        if parents != [old_parent] or not author or not tree:
            raise ValueError("published metadata range is not linear")
        if author[1].rsplit(" ", 2)[0] != signer:
            raise ValueError("published commit author differs from recognized existing signer")
        paths = set(_git(broker.checkout, "diff", "--name-only", old_parent, old).splitlines())
        if not paths <= set(broker.spec["policy"]["allowed_paths"]):
            raise ValueError("published range changes source outside frozen scope")
        subject, _, body = message.partition("\n")
        subject = conventional_subject(subject.removeprefix("Implement "))
        new_message = subject + "\n" + body
        if (
            signer
            not in _git(
                broker.checkout,
                "show",
                "-s",
                "--format=%(trailers:key=Signed-off-by,valueonly)",
                old,
            ).splitlines()
        ):
            new_message += "\n" if new_message.endswith("\n") else "\n\n"
            new_message += "Signed-off-by: " + signer + "\n"
        # Old GPG signatures authenticate the retained old object only. Never copy
        # an invalid signature onto the rewritten metadata object.
        payload = (
            f"tree {tree[1]}\nparent {new_parent}\nauthor {author[1]}\n"
            f"committer {committer}\n\n{new_message}"
        )
        encoded = payload.encode()
        new = hashlib.sha1(b"commit " + str(len(encoded)).encode() + b"\0" + encoded).hexdigest()
        mapping.append(
            {
                "old": old,
                "new": new,
                "tree": tree[1],
                "author": author[1],
                "original_object": raw,
                "new_object": payload,
            }
        )
        old_parent, new_parent = old, new
    if old_parent == new_parent:
        raise ValueError("published range has no metadata change")
    return mapping


def _snapshot(store, run_id, payload):
    spec = store.effective_spec(run_id)
    authority = _authority(payload, spec)
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    closed = store._completed_temporal_result(
        run_id, workflow_id=row["workflow_id"] or f"delivery-{run_id}"
    )
    state = closed["result"]
    roles = state.get("roles", [])
    if (
        closed["request_digest"] != row["request_digest"]
        or closed["recovery_digest"] != (digest(previous) if previous else None)
        or state.get("run_id") != run_id
        or state.get("outcome") not in {"blocked", "delivered"}
        or row["outcome"] != state["outcome"]
        or state.get("revision") != payload["expected_revision"]
        or row["protocol_revision"] != state["revision"]
        or canonical_json(json.loads(row["candidate_json"] or "null"))
        != canonical_json(state.get("candidate"))
        or canonical_json(json.loads(row["pr_json"] or "null"))
        != canonical_json(state.get("pull_request"))
        or any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts)
        or any(e["state"] != "complete" or not e["observed_json"] for e in effects)
        or not isinstance(roles, list)
        or len(roles) != len(attempts)
        or sorted((a["role"], a["iteration"], a["session_id"]) for a in attempts)
        != sorted((r["role"], r["iteration"], r.get("session_id")) for r in roles)
    ):
        raise ValueError("metadata recovery requires an authentic stopped terminal checkpoint")
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    pr = state.get("pull_request", {})
    if (
        canonical_json(candidate) != canonical_json(state.get("candidate"))
        or candidate["id"] != payload["expected_candidate_id"]
        or candidate["head"] != payload["expected_head"]
        or pr.get("head") != candidate["head"]
        or pr.get("number") != payload["expected_pr_number"]
        or _git(broker.checkout, "status", "--porcelain", "--untracked-files=all")
        or _git(broker.checkout, "branch", "--show-current") != spec["branch"]
    ):
        raise ValueError("metadata candidate or clean owned checkout changed")
    signer, committer = _identity(broker)
    if signer != payload["expected_signer"]:
        raise ValueError("metadata signer differs from explicit request")
    cleanup = _stopped_cleanup(spec) if spec["provider"] == "codex" else {"provider": "fake"}
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    mapping = _range(broker, state, effects, signer, committer, previous)
    found = broker._existing_pr(validate_metadata=False)
    if found is None:
        raise ValueError("metadata recovery has no owned open PR")
    result = {
        "kind": "published_metadata_recovery",
        "command": payload,
        "spec": spec,
        "authority": authority,
        "original_row": row,
        "attempts": attempts,
        "effects": effects,
        "claim": claim,
        "closed": closed,
        "state": state,
        "original_recovery": previous,
        "cleanup": cleanup,
        "work_binding": binding,
        "mapping": mapping,
        "old_head": candidate["head"],
        "new_head": mapping[-1]["new"],
        "signer": signer,
        "old_title": found["title"],
        "new_title": conventional_subject(spec["goal"]),
    }
    if spec["provider"] == "codex":
        sessions = {r.get("session_id") for r in roles if r.get("role") == "implement"}
        if len(sessions) != 1 or None in sessions:
            raise ValueError("metadata original implementer session identity changed")
        result["session_id"] = sessions.pop()
        result["session_sha256"] = session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement",
            result["session_id"],
        )
    _, local, remote, found = _guard(store, result)
    if local != result["old_head"] or remote != local or found["headRefOid"] != local:
        raise ValueError("initial metadata admission requires exact original local/remote/PR head")
    return result


def _guard(store, grant):
    spec = grant["spec"]
    _authority(grant["command"], spec)
    broker = DeliveryBroker(store, spec)
    if spec['provider'] == 'codex':
        from .delivery_config import DeliveryConfig
        from .delivery_native_renewal import verify_generation

        if digest(DeliveryConfig.load(Path(spec['config_path'])).raw) != spec['config_digest']:
            raise ValueError('metadata effect frozen configuration changed')
        generation = Path(spec['state_dir']) / 'native-preparation-renewal/generation.json'
        if generation.exists():
            observed = read_private(generation)
            if observed.get('command_digest') != digest({'run_id': spec['run_id'],
                                                        **grant['command']}):
                raise ValueError('metadata effect native generation command changed')
            verify_generation(spec, {'path': str(generation),
                                     'sha256': hashlib.sha256(generation.read_bytes()).hexdigest()},
                              observed['effective_spec'])
    candidate = broker.candidate()
    if (
        candidate["head"] not in {grant["old_head"], grant["new_head"]}
        or any(
            candidate[k] != grant["state"]["candidate"][k]
            for k in ("content_sha256", "base_sha", "policy_digest", "environment_digest")
        )
        or _git(broker.checkout, "status", "--porcelain", "--untracked-files=all")
        or _git(broker.checkout, "branch", "--show-current") != spec["branch"]
        or _git(broker.checkout, "remote", "get-url", "--push", "origin") != spec["origin_url"]
        or _git(broker.source, "remote", "get-url", "origin") != spec["origin_url"]
        or _identity(broker)[0] != grant["signer"]
    ):
        raise ValueError("metadata effect lost exact source, branch, origin or signer authority")
    if spec["provider"] == "codex" and canonical_json(_stopped_cleanup(spec)) != canonical_json(
        grant["cleanup"]
    ):
        raise ValueError("metadata effect cleanup evidence changed")
    if (
        spec["provider"] == "codex"
        and session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement",
            grant["session_id"],
        )
        != grant["session_sha256"]
    ):
        raise ValueError("metadata effect original session evidence changed")
    row, attempts, effects, claim = _rows(store, spec["run_id"])
    if (
        canonical_json(attempts) != canonical_json(grant["attempts"])
        or canonical_json(effects) != canonical_json(grant["effects"])
        or canonical_json(row) != canonical_json(grant["original_row"])
    ):
        raise ValueError("metadata effect stopped run history changed")
    with store._connect() as db:
        if canonical_json(work_binding(store, spec, db)) != canonical_json(grant["work_binding"]):
            raise ValueError("metadata effect work binding changed")
        pending = db.execute(
            "SELECT 1 FROM delivery_metadata_recoveries WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if pending and (claim is None or claim["owner"] != f"external:devflow:{spec['run_id']}"):
            raise ValueError("metadata effect lost its owning claim")
    remote = _git(broker.source, "ls-remote", "origin", f"refs/heads/{spec['branch']}")
    found = broker._existing_pr(validate_metadata=False)
    if (
        not remote
        or remote.split()[0] not in {grant["old_head"], grant["new_head"]}
        or found is None
        or found["number"] != grant["command"]["expected_pr_number"]
        or found.get("state") != "OPEN"
        or found.get("isDraft") is not False
        or found.get("headRefName") != spec["branch"]
        or found.get("baseRefName") != spec["base_ref"].removeprefix("origin/")
        or found["url"] != grant["state"]["pull_request"]["url"]
        or found["headRefOid"] not in {grant["old_head"], grant["new_head"]}
        or found["title"] not in {grant["old_title"], grant["new_title"]}
    ):
        raise ValueError("metadata effect remote/PR authority changed")
    return broker, candidate["head"], remote.split()[0], found


def reconcile(store, run_id, payload, *, preflight=False):
    fields = {
        "command_id",
        "expected_revision",
        "expected_candidate_id",
        "expected_head",
        "expected_pr_number",
        "expected_signer",
        "authority_path",
        "authority_sha256",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) not in (fields, fields | {
            'preparation_authority_path', 'preparation_authority_sha256',
        })
        or any(
            not isinstance(payload[key], str)
            for key in fields - {"expected_revision", "expected_pr_number"}
        )
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", payload["command_id"])
        or type(payload["expected_revision"]) is not int
        or payload["expected_revision"] < 1
        or type(payload["expected_pr_number"]) is not int
        or payload["expected_pr_number"] < 1
        or not re.fullmatch(r"[0-9a-f]{64}", payload["expected_candidate_id"])
        or not re.fullmatch(r"[0-9a-f]{40}", payload["expected_head"])
        or not re.fullmatch(r"[0-9a-f]{64}", payload["authority_sha256"])
    ):
        raise ValueError("metadata request fields or identity are invalid")
    command_digest = digest({"run_id": run_id, **payload})
    if preflight:
        grant = _snapshot(store, run_id, payload)
        from .delivery_native_renewal import readiness

        ready = readiness(grant['spec'], payload)
        return {
            "run_id": run_id,
            "preflight": True,
            "precheck_sha256": digest(grant),
            "old_head": grant["old_head"],
            "new_head": grant["new_head"],
            "commits": len(grant["mapping"]),
            "provider_turns": 0,
            "native_preparation_required": bool(ready and ready['required']),
        }
    root = Path(store.effective_spec(run_id)["state_dir"]) / "metadata-reconciliation"
    private_directory(root)
    with _lock(root / "controller.lock"):
        with store._connect() as db:
            saved = db.execute(
                "SELECT * FROM delivery_metadata_recoveries WHERE run_id=?", (run_id,)
            ).fetchone()
            prior = db.execute(
                "SELECT * FROM delivery_commands WHERE command_id=?", (payload["command_id"],)
            ).fetchone()
        if prior and prior["request_digest"] != command_digest:
            raise ValueError("command ID already belongs to different inputs")
        if saved and saved["command_digest"] != command_digest:
            raise ValueError("this original run already received its metadata command")
        if saved and saved["state"] == "queued":
            return json.loads(prior["response_json"])
        if saved:
            grant = json.loads(saved["grant_json"])
        elif (root / "intent.json").exists():
            # Publication intent precedes admission. Retain its exact timestamps and
            # mapping after an interrupted transaction, never recapture authority.
            grant = read_private(root / "intent.json")
            if canonical_json(grant.get("command")) != canonical_json(payload) or digest(
                store.effective_spec(run_id)
            ) != digest(grant["spec"]):
                raise ValueError("metadata pending intent belongs to different authority")
            closed = store._completed_temporal_result(
                run_id,
                workflow_id=grant["closed"]["workflow_id"],
            )
            if canonical_json(closed) != canonical_json(grant["closed"]):
                raise ValueError("metadata pending closed execution changed")
            _guard(store, grant)
        else:
            grant = _snapshot(store, run_id, payload)
        from .delivery_native_renewal import readiness, renew

        readiness(grant['spec'], payload)
        _immutable(root / "intent.json", grant)
        preserve_resources(root, grant["spec"])

        execution_spec, renewal = renew(grant['spec'], payload, command_digest)
        _guard(store, grant)
        if saved is None:
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
                if canonical_json(dict(row)) != canonical_json(grant["original_row"]):
                    raise ValueError("metadata admission lost its frozen terminal projection")
                if canonical_json(work_binding(store, grant["spec"], db)) != canonical_json(
                    grant["work_binding"]
                ):
                    raise ValueError("metadata admission lost frozen issue authority")
                if store.state.claim_for(db, grant["spec"]["work_id"]) is None:
                    store.state.claim_work(
                        db,
                        grant["spec"]["work_id"],
                        f"external:devflow:{run_id}",
                        store.config.dashboard_url,
                    )
                db.execute(
                    "INSERT INTO delivery_metadata_recoveries VALUES (?,?,?,?,'pending')",
                    (run_id, payload["command_id"], command_digest, canonical_json(grant)),
                )
                pending = {
                    "run_id": run_id,
                    "phase": "metadata_reconciling",
                    "reconciliation_only": True,
                }
                db.execute(
                    "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                    (payload["command_id"], run_id, command_digest, canonical_json(pending)),
                )
        broker, local, remote, found = _guard(store, grant)
        for kind, head in (("original", grant["old_head"]), ("rewritten", grant["new_head"])):
            if kind == "rewritten":
                for item in grant["mapping"]:
                    _guard(store, grant)
                    result = subprocess.run(
                        [
                            "git",
                            "-C",
                            str(broker.checkout),
                            "hash-object",
                            "-t",
                            "commit",
                            "-w",
                            "--stdin",
                        ],
                        input=item["new_object"].encode(),
                        capture_output=True,
                        check=True,
                        timeout=30,
                    )
                    if result.stdout.decode().strip() != item["new"]:
                        raise ValueError("rewritten metadata object differs from sealed mapping")
            ref = f"refs/devflow/metadata/{run_id}/{kind}"
            old = _git(broker.checkout, "for-each-ref", "--format=%(objectname)", ref)
            if old and old != head:
                raise ValueError("metadata preservation ref was replaced")
            _guard(store, grant)
            _git(broker.checkout, "update-ref", ref, head, old or "0" * 40)
            _immutable(root / (kind + "-ref.json"), {"ref": ref, "head": head})
        broker, local, remote, found = _guard(store, grant)
        if local == grant["old_head"]:
            _git(
                broker.checkout,
                "update-ref",
                f"refs/heads/{broker.spec['branch']}",
                grant["new_head"],
                grant["old_head"],
            )
        broker, local, remote, found = _guard(store, grant)
        if remote == grant["old_head"]:
            _git(
                broker.checkout,
                "push",
                f"--force-with-lease=refs/heads/{broker.spec['branch']}:{grant['old_head']}",
                "origin",
                f"{grant['new_head']}:refs/heads/{broker.spec['branch']}",
            )
        broker, local, remote, found = _guard(store, grant)
        if found["title"] != grant["new_title"]:
            _run(
                [
                    "gh",
                    "pr",
                    "edit",
                    str(found["number"]),
                    "--repo",
                    broker.spec["github_repo"],
                    "--title",
                    grant["new_title"],
                ],
                timeout=45,
            )
        broker, local, remote, found = _guard(store, grant)
        if (
            local != grant["new_head"]
            or remote != grant["new_head"]
            or found["headRefOid"] != grant["new_head"]
            or found["title"] != grant["new_title"]
        ):
            raise ValueError("metadata publication readback remains pending; replay SAME request")
        broker._validate_publication_commits()
        execution_broker = DeliveryBroker(store, execution_spec)
        candidate = execution_broker.candidate()
        if (candidate['head'] != grant['new_head']
                or candidate['policy_digest'] != execution_spec['policy_digest']
                or any(canonical_json(candidate[key])
                       != canonical_json(grant['state']['candidate'][key])
                       for key in ('content_sha256', 'base_sha', 'environment_digest'))):
            raise ValueError('metadata successor changed identical feature source custody')
        publication = {
            "number": found["number"],
            "url": found["url"],
            "state": "OPEN",
            "head": grant["new_head"],
            "base": broker.spec["base_sha"],
            "candidate": candidate,
        }
        _immutable(
            root / "publication.json",
            {
                "candidate": candidate,
                "publication": publication,
                "mapping_digest": digest(grant["mapping"]),
            },
        )
        recovery = {
            **grant,
            "candidate": candidate,
            "publication": publication,
            "grant_digest": digest(grant),
            'execution_spec': execution_spec,
            'native_preparation_renewal': renewal,
            'source_lineage': {'before': grant['state']['candidate'], 'after': candidate,
                               'metadata_mapping_sha256': digest(grant['mapping']),
                               'native_preparation_renewal': renewal,
                               'same_feature_source': True},
        }
        workflow_id = f"delivery-{run_id}-metadata-1"
        response = {
            "run_id": run_id,
            "phase": "metadata_validation_queued",
            "workflow_id": workflow_id,
            "candidate_id": candidate["id"],
            "head": grant["new_head"],
            "reconciliation_only": True,
        }
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            work_binding(store, grant["spec"], db)
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if canonical_json(dict(current)) != canonical_json(grant["original_row"]):
                raise ValueError("metadata validation queue lost its exact stopped checkpoint")
            db.execute(
                "UPDATE delivery_metadata_recoveries SET state='queued' WHERE run_id=?", (run_id,)
            )
            db.execute(
                "UPDATE delivery_runs SET phase='metadata_validation_queued',"
                "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1,"
                "workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?",
                (workflow_id, canonical_json(recovery), store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                "WHERE run_id=?",
                (store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                (canonical_json(response), payload["command_id"]),
            )
            store._event(
                db,
                run_id,
                grant["original_row"]["revision"] + 1,
                "metadata_validation_queued",
                "Owned metadata reconciled; fresh gates only",
                {
                    "old_head": grant["old_head"],
                    "new_head": grant["new_head"],
                    "mapping_digest": digest(grant["mapping"]),
                },
            )
        return response


def validation_readback(store, spec, recovery):
    if recovery.get('native_preparation_renewal'):
        from .delivery_native_renewal import verify_generation

        verify_generation(recovery['spec'], recovery['native_preparation_renewal'], spec)
    broker = DeliveryBroker(store, spec)
    if canonical_json(broker.candidate()) != canonical_json(recovery["candidate"]):
        raise ValueError("metadata validation candidate changed")
    broker._validate_publication_commits()
    remote = _git(broker.source, "ls-remote", "origin", f"refs/heads/{spec['branch']}")
    found = broker._existing_pr()
    if (
        not remote
        or remote.split()[0] != recovery["new_head"]
        or found is None
        or found["number"] != recovery["publication"]["number"]
        or found["headRefOid"] != recovery["new_head"]
    ):
        raise ValueError("metadata validation remote publication changed")
    with store._connect() as db:
        work_binding(store, spec, db)
        grant = db.execute(
            "SELECT * FROM delivery_metadata_recoveries WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if not grant or digest(json.loads(grant["grant_json"])) != recovery["grant_digest"]:
            raise ValueError("metadata validation grant changed")
    root = Path(spec["state_dir"]) / "metadata-reconciliation"
    original = read_private(root / "intent.json")
    if digest(original) != recovery["grant_digest"]:
        raise ValueError("metadata immutable intent changed before validation")
    _authority(original["command"], spec)
    _row, attempts, effects, _claim = _rows(store, spec["run_id"])
    by_key = {item["effect_key"]: item for item in effects}
    if canonical_json(attempts) != canonical_json(original["attempts"]) or any(
        canonical_json(by_key.get(item["effect_key"])) != canonical_json(item)
        for item in original["effects"]
    ):
        raise ValueError("metadata predecessor attempt/publication history changed")
    if (
        spec["provider"] == "codex"
        and session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement",
            original["session_id"],
        )
        != original["session_sha256"]
    ):
        raise ValueError("metadata original session evidence changed")
    for kind, head in (("original", recovery["old_head"]), ("rewritten", recovery["new_head"])):
        ref = f"refs/devflow/metadata/{spec['run_id']}/{kind}"
        if _git(
            broker.checkout, "for-each-ref", "--format=%(objectname)", ref
        ) != head or canonical_json(read_private(root / (kind + "-ref.json"))) != canonical_json(
            {"ref": ref, "head": head}
        ):
            raise ValueError("metadata preserved object/ref custody changed")
    return deepcopy(recovery["publication"])
