"""Explicit ordinary execution inventory; never PR/ruleset or merge eligibility.

The acknowledgement writes only metadata. A normal, separately admitted CI
workflow remains the sole writer of delivery verdicts, tracker and finalization.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_ci_inventory_contract import APPROVED_PROFILE_SHA256, approved_profile
from .delivery_ci_inventory_io import MAX_JSON, MAX_LOG, GitHubRead, owned_bytes, publish
from .delivery_ci_inventory_observation import METHOD, collect

KIND = "ordinary_ci_execution_inventory"
ACK = "ordinary_execution_only_no_PR_ruleset_or_merge_eligibility"
FIELD = "ordinary_ci_inventory"
FIELDS = {
    "command_id",
    "expected_revision",
    "expected_projection_revision",
    "expected_iteration",
    "expected_candidate_id",
    "expected_candidate_head",
    "expected_content_sha256",
    "expected_pr_number",
    "profile_path",
    "expected_profile_sha256",
}


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_ci_inventories (
        run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
        command_id TEXT UNIQUE NOT NULL, request_digest TEXT NOT NULL,
        profile_sha256 TEXT NOT NULL, response_json TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_ci_inventory_audit (
        run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
        command_id TEXT UNIQUE NOT NULL, response_json TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_ci_inventory_results (
        run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
        gate_command_id TEXT UNIQUE NOT NULL, generation INTEGER NOT NULL,
        recovery_sha256 TEXT NOT NULL, response_json TEXT NOT NULL)""")


def _request(value, acknowledged=False):
    extra = {"preflight_key", "acknowledgement"} if acknowledged else set()
    if (
        not isinstance(value, dict)
        or set(value) != FIELDS | extra
        or not isinstance(value.get("command_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value["command_id"])
        or any(
            type(value.get(k)) is not int or value[k] < 0
            for k in ("expected_revision", "expected_projection_revision", "expected_iteration")
        )
        or type(value.get("expected_pr_number")) is not int
        or value["expected_pr_number"] < 1
        or value.get("expected_profile_sha256") != APPROVED_PROFILE_SHA256
        or not isinstance(value.get("profile_path"), str)
        or (
            acknowledged
            and (
                value.get("acknowledgement") != ACK
                or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("preflight_key")))
            )
        )
    ):
        raise ValueError("ordinary CI inventory requires its exact reviewed purpose and selection")
    for key, size in (
        ("expected_candidate_id", 64),
        ("expected_content_sha256", 64),
        ("expected_candidate_head", 40),
    ):
        if not re.fullmatch(r"[0-9a-f]{" + str(size) + "}", str(value.get(key))):
            raise ValueError("ordinary CI inventory has an invalid candidate binding")
    return {k: value[k] for k in FIELDS}


def _profile(path):
    with owned_bytes(path, 128 * 1024, expected=APPROVED_PROFILE_SHA256) as raw:
        profile = approved_profile(json.loads(raw))
        if raw != canonical_json(profile).encode():
            raise ValueError("ordinary CI profile must retain its canonical reviewed bytes")
        return profile, raw


def _saved(store, run_id, db=None):
    if db is None:
        with store._connect() as current:
            return _saved(store, run_id, current)
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_ci_inventories'"
    ).fetchone():
        return None
    row = db.execute("SELECT * FROM delivery_ci_inventories WHERE run_id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def _root(spec):
    return Path(spec["state_dir"]) / "ci-execution-inventory" / APPROVED_PROFILE_SHA256


def _compact(seal):
    return {
        "original_spec_sha256": digest(seal["original_spec"]),
        "original_policy_sha256": seal["original_spec"]["policy_digest"],
        "row_sha256": digest(seal["row"]),
        "closed_sha256": digest(seal["closed"]),
        "attempts_sha256": digest(seal["attempts"]),
        "effects_sha256": digest(seal["effects"]),
        "candidate": seal["candidate"],
        "publication": seal["publication"],
        "work_binding": seal["work_binding"],
        "previous_digest": seal["previous_digest"],
        "stage": seal["stage"],
        "generation": seal["generation"],
    }


def _subject(spec, candidate, publication, profile):
    s = profile["subject"]
    if (
        spec.get("provider") != "codex"
        or spec.get("feature_worker")
        or spec.get("feature_delivery")
        or spec.get("authorized_endpoint") != "published_unmerged"
        or spec["run_id"] != s["run_id"]
        or spec["github_repo"] != s["repository"]
        or spec["branch"] != s["branch"]
        or candidate.get("id") != s["candidate_id"]
        or candidate.get("head") != s["head"]
        or candidate.get("content_sha256") != s["content_sha256"]
        or spec["policy_digest"] != s["policy_digest"]
        or spec["policy"].get("required_ci") != profile["required_ci"]
        or publication.get("number") != s["pr"]
        or publication.get("head") != s["head"]
    ):
        raise ValueError("ordinary CI profile differs from its full frozen original subject")


def _physical(store, spec, previous, *, resource_spec=None):
    from .delivery_amendment_capacity import live_owner
    from .delivery_stopped_resume import observed_preparation_cleanup
    from .delivery_unpublished_recovery import quarantine_history, teardown_checkpoint

    history = quarantine_history(previous)
    resources = observed_preparation_cleanup(
        spec,
        resource_spec=resource_spec or spec,
        quarantined=teardown_checkpoint(history) if history else None,
    )
    with store._connect() as db:
        owner = live_owner(spec, db=db)[1]
    return {"resources_sha256": resources, "operational_owner": owner}


def _authority(store, run_id, command, profile):
    from .delivery_gate_retry import CI_KIND, snapshot
    from .delivery_runtime_location import require_not_pending
    from .delivery_store import DeliveryStore

    require_not_pending(store.config)

    readonly = store if store.read_only else DeliveryStore(store.config, read_only=True)
    seal = snapshot(readonly, run_id, CI_KIND)
    spec, row, candidate = seal["original_spec"], seal["row"], seal["candidate"]
    _subject(spec, candidate, seal["publication"], profile)
    if (
        readonly.intake_execution_spec(run_id)["policy_digest"]
        != profile["subject"]["intake_policy_digest"]
    ):
        raise ValueError("ordinary CI inventory changed its original intake policy")
    if (
        command["expected_revision"] != row["protocol_revision"]
        or command["expected_projection_revision"] != row["revision"]
        or command["expected_iteration"] != row["iteration"]
        or command["expected_candidate_id"] != candidate["id"]
        or command["expected_candidate_head"] != candidate["head"]
        or command["expected_content_sha256"] != candidate["content_sha256"]
        or command["expected_pr_number"] != seal["publication"]["number"]
    ):
        raise ValueError("ordinary CI inventory lost its stopped current checkpoint")
    from .delivery_gate_retry import _retained_recovery, _rows, _sealed_row
    from .delivery_policy_recovery import work_binding

    physical = _physical(readonly, spec, _retained_recovery(readonly, run_id, seal))
    with readonly._connect() as db:
        if (
            work_binding(readonly, spec, db) != seal["work_binding"]
            or readonly.state.claim_for(db, spec["work_id"]) is not None
            or db.execute(
                "SELECT 1 FROM delivery_mutations WHERE run_id=? "
                "AND (state!='complete' OR kind='cancel')",
                (run_id,),
            ).fetchone()
            or db.execute(
                "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state!='sent'", (run_id,)
            ).fetchone()
        ):
            raise ValueError("ordinary CI inventory lost its released issue/effect authority")
    actual, attempts, effects, claim = _rows(readonly, run_id)
    if (
        _sealed_row(actual) != row
        or attempts != seal["attempts"]
        or effects != seal["effects"]
        or claim is not None
    ):
        raise ValueError("ordinary CI original ledger changed during observation")
    require_not_pending(store.config)
    return seal, {**_compact(seal), **physical}


def _observe(profile, *, timeout=120):
    reader = GitHubRead(repositories={p["repository"] for p in profile["sources"]}, timeout=timeout)
    facts = collect(
        reader, profile, required=profile["required_ci"], expected_tree=profile["subject"]["tree"]
    )
    return facts, reader.observations


def _retain(root, facts, observations):
    refs = []
    for value in observations:
        reference = publish(root / "raw", value["raw"], limit=MAX_LOG)
        refs.append({"request": value["request"], "body": reference, "http": value["http"]})
    return {"facts": facts, "raw_observations": refs}


def preflight(store, run_id, payload):
    command = _request(payload)
    profile, _ = _profile(command["profile_path"])
    if _saved(store, run_id):
        raise ValueError("ordinary CI selection already has its one acknowledged inventory")
    with store._connect() as db:
        if db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (command["command_id"],)
        ).fetchone():
            raise ValueError("ordinary CI command ID already belongs to another operation")
    _, before = _authority(store, run_id, command, profile)
    facts, _ = _observe(profile)
    _, after = _authority(store, run_id, command, profile)
    if before != after:
        raise ValueError("ordinary CI source/owner/ledger changed during preflight")
    return {
        "run_id": run_id,
        "preflight": True,
        "kind": KIND,
        "preflight_key": digest({"command": command, "authority": before, "facts": facts}),
        "profile_sha256": APPROVED_PROFILE_SHA256,
        "PR_ruleset_eligibility": False,
        "merge_context_verified": False,
        "delivery_acceptance": False,
        "additional_iterations": 0,
    }


def acknowledge(store, run_id, payload):
    command = _request(payload, acknowledged=True)
    request_digest = digest({"run_id": run_id, **payload})
    if prior := _saved(store, run_id):
        if (
            prior["command_id"] != command["command_id"]
            or prior["request_digest"] != request_digest
        ):
            raise ValueError("ordinary CI selection cannot acquire another acknowledgement")
        response, receipt, profile = archive(store, run_id)
        _profile(command["profile_path"])
        with store._connect() as db:
            row = db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        recovery = json.loads(row[0]) if row and row[0] else None
        active = bool(recovery and recovery.get("seal", {}).get(FIELD))
        if active:
            from .delivery_broker import DeliveryBroker
            from .delivery_gate_retry import _rows

            spec = store.effective_spec(run_id)
            validate(store, spec, recovery)
            broker = DeliveryBroker(store, spec)
            if broker.candidate() != recovery["candidate"]:
                raise ValueError("ordinary CI replay lost its admitted current candidate")
            before = digest(_rows(store, run_id))
        else:
            _, before = _authority(store, run_id, command, profile)
            if before != receipt["authority"]:
                raise ValueError("ordinary CI replay lost its original authority")
        facts, _ = _observe(profile)
        if facts != receipt["observation"]["facts"]:
            raise ValueError("ordinary CI replay selected IDs/bytes changed")
        if active:
            validate(store, spec, recovery)
            if (
                broker.candidate() != recovery["candidate"]
                or digest(_rows(store, run_id)) != before
            ):
                raise ValueError("ordinary CI current authority changed during replay")
        else:
            _, after = _authority(store, run_id, command, profile)
            if after != before:
                raise ValueError("ordinary CI original authority changed during replay")
        return {**response, "replayed": True}
    profile, raw = _profile(command["profile_path"])
    with store._connect() as db:
        if db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (command["command_id"],)
        ).fetchone():
            raise ValueError("ordinary CI command ID already belongs to another operation")
    seal, before = _authority(store, run_id, command, profile)
    facts, observations = _observe(profile)
    key = digest({"command": command, "authority": before, "facts": facts})
    if key != payload["preflight_key"]:
        raise ValueError("ordinary CI preflight facts or original authority changed")
    root = _root(seal["original_spec"])
    profile_ref = publish(root / "raw", raw, limit=128 * 1024)
    retained = _retain(root, facts, observations)
    receipt = {
        "schema": KIND + "/v1",
        "run_id": run_id,
        "command": command,
        "request_digest": request_digest,
        "profile": profile_ref,
        "authority": before,
        "observation": retained,
        "PR_ruleset_eligibility": False,
        "merge_context_verified": False,
        "delivery_acceptance": False,
        "original_delivery_fields_unchanged": True,
    }
    reference = publish(root / "receipts", canonical_json(receipt).encode(), limit=MAX_JSON)
    _, after = _authority(store, run_id, command, profile)
    if before != after:
        raise ValueError("ordinary CI authority changed before acknowledgement")
    response = {
        "run_id": run_id,
        "kind": KIND,
        "state": "acknowledged",
        "receipt": reference,
        "profile_sha256": APPROVED_PROFILE_SHA256,
        "command_id": command["command_id"],
        "PR_ruleset_eligibility": False,
        "merge_context_verified": False,
        "delivery_acceptance": False,
        "additional_iterations": 0,
    }
    from .delivery_gate_retry import _sealed_row
    from .delivery_policy_recovery import work_binding

    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if (
            current is None
            or _sealed_row(current) != seal["row"]
            or _saved(store, run_id, db)
            or work_binding(store, seal["original_spec"], db) != seal["work_binding"]
            or store.state.claim_for(db, seal["original_spec"]["work_id"]) is not None
            or db.execute(
                "SELECT 1 FROM delivery_native_diagnostics WHERE run_id=? "
                "AND state IN ('queued','running','unknown')",
                (run_id,),
            ).fetchone()
        ):
            raise ValueError("ordinary CI acknowledgement lost its atomic original checkpoint")
        for table, key_name, expected in (
            ("delivery_attempts", "job_key", seal["attempts"]),
            ("delivery_effects", "effect_key", seal["effects"]),
        ):
            if [
                dict(r)
                for r in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY {key_name}", (run_id,)
                )
            ] != expected:
                raise ValueError("ordinary CI acknowledgement lost its atomic full ledger")
        db.execute(
            "INSERT INTO delivery_ci_inventories VALUES (?,?,?,?,?)",
            (
                run_id,
                command["command_id"],
                request_digest,
                APPROVED_PROFILE_SHA256,
                canonical_json(response),
            ),
        )
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (command["command_id"], run_id, request_digest, canonical_json(response)),
        )
        db.execute(
            "INSERT INTO delivery_ci_inventory_audit VALUES (?,?,?)",
            (run_id, command["command_id"], canonical_json(response)),
        )
    return response


def _document(store, reference, run_id, folder, *, original=None):
    original = original if original is not None else store.spec(run_id)
    _reference(reference, MAX_JSON)
    path = _root(original) / folder / reference["sha256"]
    if reference.get("path") != str(path) or reference.get("bytes", MAX_JSON + 1) > MAX_JSON:
        raise ValueError("ordinary CI receipt left its derived private namespace")
    with owned_bytes(path, MAX_JSON, expected=reference["sha256"]) as raw:
        if len(raw) != reference["bytes"]:
            raise ValueError("ordinary CI receipt size changed")
        return json.loads(raw)


def _reference(reference, limit):
    if (
        not isinstance(reference, dict)
        or set(reference) != {"path", "sha256", "bytes"}
        or not isinstance(reference["path"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", str(reference["sha256"]))
        or type(reference["bytes"]) is not int
        or not 0 < reference["bytes"] <= limit
    ):
        raise ValueError("ordinary CI reference has an invalid bounded immutable identity")


def archive(store, run_id):
    """Data-only immutable proof hydration: no GitHub/source/cwd observations."""
    # One existing connection/snapshot for this immutable proof. Reopening a
    # writable Store for each internal query would repeatedly hydrate the same
    # complete installation authority. No object is cached across operations.
    with store._connect() as db:
        if not db.in_transaction:
            db.execute("BEGIN")
        return _archive(store, db, run_id)


def _archive(store, db, run_id):
    saved = _saved(store, run_id, db)
    if not saved or saved["profile_sha256"] != APPROVED_PROFILE_SHA256:
        raise ValueError("ordinary CI inventory has no durable explicit acknowledgement")
    response = json.loads(saved["response_json"])
    original = store._spec(db, run_id)
    receipt = _document(store, response["receipt"], run_id, "receipts", original=original)
    root = _root(original)
    profile_ref = receipt["profile"]
    _reference(profile_ref, 128 * 1024)
    if profile_ref["path"] != str(root / "raw" / APPROVED_PROFILE_SHA256):
        raise ValueError("ordinary CI profile archive left its sealed namespace")
    profile, _ = _profile(profile_ref["path"])
    expected_key = digest(
        {
            "command": receipt["command"],
            "authority": receipt["authority"],
            "facts": receipt["observation"]["facts"],
        }
    )
    expected_request = digest(
        {
            "run_id": run_id,
            **receipt["command"],
            "preflight_key": expected_key,
            "acknowledgement": ACK,
        }
    )
    if (
        receipt.get("schema") != KIND + "/v1"
        or receipt.get("run_id") != run_id
        or receipt.get("request_digest") != saved["request_digest"]
        or expected_request != saved["request_digest"]
        or profile_ref["sha256"] != APPROVED_PROFILE_SHA256
        or profile_ref["bytes"] != len(canonical_json(profile).encode())
        or receipt["command"]["command_id"] != saved["command_id"]
        or response.get("command_id") != saved["command_id"]
        or response.get("profile_sha256") != APPROVED_PROFILE_SHA256
        or any(
            receipt.get(k) is not False or response.get(k) is not False
            for k in ("PR_ruleset_eligibility", "merge_context_verified", "delivery_acceptance")
        )
    ):
        raise ValueError("ordinary CI acknowledgement changed its original purpose")
    _proof(store, run_id, receipt["observation"], original=original)
    command = db.execute(
        "SELECT * FROM delivery_commands WHERE command_id=?", (saved["command_id"],)
    ).fetchone()
    event = db.execute(
        "SELECT response_json FROM delivery_ci_inventory_audit WHERE run_id=? AND command_id=?",
        (run_id, saved["command_id"]),
    ).fetchall()
    if (
        not command
        or command["request_digest"] != saved["request_digest"]
        or json.loads(command["response_json"]) != response
        or len(event) != 1
        or json.loads(event[0][0]) != response
    ):
        raise ValueError("ordinary CI acknowledgement lost its command/audit custody")
    return response, receipt, profile


def _proof(store, run_id, observation, *, original=None):
    facts = observation["facts"]
    if (
        facts.get("method") != METHOD
        or facts.get("state") != "passed"
        or facts.get("PR_ruleset_eligibility") is not False
        or facts.get("merge_context_verified") is not False
        or facts.get("profile_sha256") != APPROVED_PROFILE_SHA256
    ):
        raise ValueError("ordinary CI observation lost its separate execution purpose")
    refs = observation["raw_observations"]
    if not isinstance(refs, list) or not 1 <= len(refs) <= 256:
        raise ValueError("ordinary CI raw evidence inventory is incomplete or excessive")
    root = _root(original if original is not None else store.spec(run_id)) / "raw"
    total = 0
    for value in refs:
        ref = value["body"]
        _reference(ref, MAX_LOG)
        if ref["path"] != str(root / ref["sha256"]):
            raise ValueError("ordinary CI raw evidence left its exact acknowledged namespace")
        with owned_bytes(ref["path"], MAX_LOG, expected=ref["sha256"]) as raw:
            if len(raw) != ref["bytes"] or value["http"].get("status") != 200:
                raise ValueError("ordinary CI raw response lost its complete HTTP custody")
            total += len(raw)
        if total > 128 * 1024 * 1024:
            raise ValueError("ordinary CI raw evidence aggregate bound exceeded")


def for_seal(store, spec, seal):
    if not _saved(store, spec["run_id"]):
        return None
    response, receipt, profile = archive(store, spec["run_id"])
    _subject(spec, seal["candidate"], seal["publication"], profile)
    if any(receipt["authority"].get(k) != v for k, v in _compact(seal).items()):
        raise ValueError("ordinary CI acknowledgement lost its exact original closed checkpoint")
    return {
        "acknowledgement": response["receipt"],
        "profile_sha256": APPROVED_PROFILE_SHA256,
        "original_authority_sha256": digest(receipt["authority"]),
    }


def authority(store, recovery):
    """Historical CI retry hydration; never resolve a current source or remote ref."""
    from .delivery_gate_retry import CI_KIND

    binding = recovery.get("seal", {}).get(FIELD)
    if not binding:
        raise ValueError("ordinary CI execution has no admitted acknowledgement")
    before = recovery["seal"]["original_spec"]
    response, receipt, profile = archive(store, before["run_id"])
    if (
        recovery.get("kind") != CI_KIND
        or recovery.get("command", {}).get("additional_iterations") != 0
        or binding
        != {
            "acknowledgement": response["receipt"],
            "profile_sha256": APPROVED_PROFILE_SHA256,
            "original_authority_sha256": digest(receipt["authority"]),
        }
        or any(receipt["authority"].get(k) != v for k, v in _compact(recovery["seal"]).items())
    ):
        raise ValueError("ordinary CI execution changed its one finite original admission")
    _subject(before, recovery["seal"]["candidate"], recovery["seal"]["publication"], profile)
    _gate_acknowledgement(store, recovery)
    return response, receipt, profile


def _gate_acknowledgement(store, recovery):
    spec = recovery["execution_spec"]
    with store._connect() as db:
        command = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?",
            (recovery["command"]["command_id"],),
        ).fetchone()
        response = json.loads(command["response_json"]) if command else {}
        expected = {
            "run_id": spec["run_id"],
            "phase": "gates_retry_queued",
            "workflow_id": f"delivery-{spec['run_id']}-ci-retry-{spec['gate_retry_generation']}",
            "candidate_id": recovery["candidate"]["id"],
            "implementation_authority": False,
            "additional_iterations": 0,
            "claim_identity": response.get("claim_identity"),
        }
        events = db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? AND "
            "type='gates_retry_queued' AND run_revision=? LIMIT 2",
            (spec["run_id"], recovery["seal"]["row"]["revision"] + 1),
        ).fetchall()
    if (
        not command
        or command["run_id"] != spec["run_id"]
        or command["request_digest"] != digest({"run_id": spec["run_id"], **recovery["command"]})
        or response != expected
        or not isinstance(response.get("claim_identity"), dict)
        or len(events) != 1
        or json.loads(events[0][0]) != response
    ):
        raise ValueError("ordinary CI execution lost its exact normal admission ACK")
    return response


def validate(store, spec, recovery, *, resource_spec=None):
    from .delivery_amendment_capacity import live_owner
    from .delivery_gate_retry import _rows
    from .delivery_listener_verification import historical_records
    from .delivery_policy_recovery import work_binding
    from .delivery_prerequisite_gate_recovery import claim_identity

    if store.read_only:
        from .delivery_runtime_location import require_not_pending

        require_not_pending(store.config)

    _, receipt, _ = authority(store, recovery)
    before = recovery["seal"]["original_spec"]
    closed = store._completed_temporal_result(
        spec["run_id"], workflow_id=recovery["seal"]["closed"]["workflow_id"]
    )
    if closed != recovery["seal"]["closed"]:
        raise ValueError("ordinary CI original closed history changed")
    historical_records(store, recovery, resource_spec or spec)
    _physical(store, before, recovery["original_recovery"], resource_spec=resource_spec or spec)
    row, _, _, claim = _rows(store, spec["run_id"])
    expected_candidate = (
        recovery["seal"]["candidate"]
        if row["phase"] == "gates_retry_queued"
        and row["protocol_revision"] == recovery["seal"]["row"]["protocol_revision"]
        else recovery["candidate"]
    )
    with store._connect() as db:
        command = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?",
            (recovery["command"]["command_id"],),
        ).fetchone()
        response = json.loads(command["response_json"]) if command else {}
        expected = {
            "run_id": spec["run_id"],
            "phase": "gates_retry_queued",
            "workflow_id": f"delivery-{spec['run_id']}-ci-retry-{spec['gate_retry_generation']}",
            "candidate_id": recovery["candidate"]["id"],
            "implementation_authority": False,
            "additional_iterations": 0,
            "claim_identity": response.get("claim_identity"),
        }
        events = db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? AND "
            "type='gates_retry_queued' AND run_revision=? LIMIT 2",
            (spec["run_id"], recovery["seal"]["row"]["revision"] + 1),
        ).fetchall()
        if (
            row["work_id"] != spec["work_id"]
            or row["iteration"] != recovery["state"]["iteration"]
            or json.loads(row["candidate_json"]) != expected_candidate
            or not claim
            or claim["owner"] != "external:devflow:" + spec["run_id"]
            or response.get("claim_identity") != claim_identity(claim)
            or work_binding(store, spec, db) != receipt["authority"]["work_binding"]
            or live_owner(before, db=db)[1] != receipt["authority"]["operational_owner"]
            or not command
            or command["run_id"] != spec["run_id"]
            or command["request_digest"]
            != digest({"run_id": spec["run_id"], **recovery["command"]})
            or response != expected
            or len(events) != 1
            or json.loads(events[0][0]) != response
        ):
            raise ValueError("ordinary CI execution lost its current exact claim/owner generation")


def result_for(broker, pr, *, timeout_seconds=None):
    store, spec = broker.store, broker.spec
    if not _saved(store, spec["run_id"]):
        return None
    with store._connect() as db:
        row = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
    recovery = json.loads(row[0]) if row and row[0] else None
    if not recovery or not recovery.get("seal", {}).get(FIELD):
        # A file/ACK alone cannot replace normal CI. Only its separately
        # admitted original CI retry is eligible to consume this inventory.
        return None
    from .delivery_gate_retry import effective_spec

    if effective_spec(store, recovery["original_spec"], recovery) != spec:
        raise ValueError("ordinary CI reader differs from its admitted execution")
    response, _, profile = authority(store, recovery)
    if pr.get("head") != profile["subject"]["head"] or pr.get("number") != profile["subject"]["pr"]:
        raise ValueError("ordinary CI current publication changed")
    validate(store, spec, recovery)
    # Use the existing CI observation allowance; never extend its deadline.
    maximum = min(120, spec["policy"].get("ci_wait_seconds", 1200))
    if timeout_seconds is not None:
        maximum = min(maximum, timeout_seconds)
    if maximum <= 0:
        raise ValueError("ordinary CI inventory needs its finite normal observation window")
    facts, observations = _observe(profile, timeout=maximum)
    if broker.candidate() != recovery["candidate"]:
        raise ValueError("ordinary CI candidate changed during remote observation")
    validate(store, spec, recovery)
    root = _root(store.spec(spec["run_id"]))
    retained = _retain(root, facts, observations)
    document = {
        "schema": METHOD,
        "run_id": spec["run_id"],
        "gate_command_id": recovery["command"]["command_id"],
        "acknowledgement": response["receipt"],
        **retained,
    }
    reference = publish(root / "observations", canonical_json(document).encode(), limit=MAX_JSON)
    validate(store, spec, recovery)
    result = {
        "state": "passed",
        "head": profile["subject"]["head"],
        "checks": facts["checks"],
        "execution_inventory": {
            "method": METHOD,
            "receipt": reference,
            "acknowledgement": response["receipt"],
            "profile_sha256": APPROVED_PROFILE_SHA256,
            "PR_ruleset_eligibility": False,
            "merge_context_verified": False,
        },
    }
    from .delivery_prerequisite_gate_recovery import claim_identity

    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        claim = store.state.claim_for(db, spec["work_id"])
        admitted = db.execute(
            "SELECT response_json FROM delivery_commands WHERE command_id=?",
            (recovery["command"]["command_id"],),
        ).fetchone()
        if (
            not current
            or json.loads(current[0]) != recovery
            or not claim
            or not admitted
            or json.loads(admitted[0]).get("claim_identity") != claim_identity(claim)
        ):
            raise ValueError("ordinary CI result lost its current finite admission/claim")
        saved = db.execute(
            "SELECT * FROM delivery_ci_inventory_results WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if saved:
            prior = json.loads(saved["response_json"])
            prior_record = _document(
                store, prior["execution_inventory"]["receipt"], spec["run_id"], "observations"
            )
            if (
                saved["gate_command_id"] != recovery["command"]["command_id"]
                or saved["recovery_sha256"] != digest(recovery)
                or prior_record["facts"] != facts
            ):
                raise ValueError("ordinary CI execution cannot replace its one observed receipt")
            result = prior
        else:
            db.execute(
                "INSERT INTO delivery_ci_inventory_results VALUES (?,?,?,?,?)",
                (
                    spec["run_id"],
                    recovery["command"]["command_id"],
                    spec["gate_retry_generation"],
                    digest(recovery),
                    canonical_json(result),
                ),
            )
    validate_result(store, spec["run_id"], result)
    validate(store, spec, recovery)
    return result


def validate_result(store, run_id, result):
    proof = result.get("execution_inventory")
    if not proof:
        return
    with store._connect() as db:
        saved = db.execute(
            "SELECT * FROM delivery_ci_inventory_results WHERE run_id=?", (run_id,)
        ).fetchone()
    if not saved or json.loads(saved["response_json"]) != result:
        raise ValueError("file-only CI observations grant no normal result authority")
    from .delivery_gate_retry import gate_namespace
    from .delivery_resources import read_private

    generation = saved["generation"]
    if type(generation) is not int or not 1 <= generation <= 8:
        raise ValueError("ordinary CI result has an invalid finite generation")
    admitted = read_private(
        Path(store.spec(run_id)["state_dir"]) / gate_namespace(generation, "ci") / "admission.json"
    )
    if (
        digest(admitted) != saved["recovery_sha256"]
        or admitted["command"]["command_id"] != saved["gate_command_id"]
        or admitted["execution_spec"]["gate_retry_generation"] != generation
    ):
        raise ValueError("ordinary CI result lost its immutable normal admission")
    authority(store, admitted)
    response, _, profile = archive(store, run_id)
    record = _document(store, proof["receipt"], run_id, "observations")
    _proof(store, run_id, record)
    if (
        proof.get("method") != METHOD
        or proof.get("profile_sha256") != APPROVED_PROFILE_SHA256
        or proof.get("acknowledgement") != response["receipt"]
        or record.get("acknowledgement") != response["receipt"]
        or record.get("schema") != METHOD
        or record.get("run_id") != run_id
        or record.get("gate_command_id") != saved["gate_command_id"]
        or result.get("state") != "passed"
        or result.get("head") != profile["subject"]["head"]
        or result.get("checks") != record["facts"]["checks"]
        or proof.get("PR_ruleset_eligibility") is not False
        or proof.get("merge_context_verified") is not False
    ):
        raise ValueError("ordinary CI result lost its separate acknowledged evidence")


def info(store, run_id):
    from .delivery_store import DeliveryStore

    readonly = store if store.read_only else DeliveryStore(store.config, read_only=True)
    response, _, _ = archive(readonly, run_id)
    return response


def indexed_evidence(store, run_id):
    if not _saved(store, run_id):
        return []
    response, _, _ = archive(store, run_id)
    # Raw logs may contain terminal controls. Only JSON-escaped summaries and
    # hashes are exposed by the generic evidence UI; raw bodies stay private.
    indexed = [
        {
            "id": "ordinary-ci-inventory-acknowledgement",
            "label": "Ordinary execution inventory; PR/ruleset and merge eligibility false",
            "path": Path(response["receipt"]["path"]),
            "limit": MAX_JSON,
        }
    ]
    with store._connect() as db:
        result = db.execute(
            "SELECT response_json FROM delivery_ci_inventory_results WHERE run_id=?", (run_id,)
        ).fetchone()
    if result:
        value = json.loads(result[0])
        validate_result(store, run_id, value)
        indexed.append(
            {
                "id": "ordinary-ci-execution-observation",
                "label": "Ordinary executed checks; not PR/ruleset or merge eligibility",
                "path": Path(value["execution_inventory"]["receipt"]["path"]),
                "limit": MAX_JSON,
            }
        )
    return indexed
