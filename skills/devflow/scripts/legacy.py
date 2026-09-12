"""Optional, one-shot import of historical version-1 stores; never recovers work."""

from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from state import encode, history, insert, now, record, row


def import_legacy(db, source_path, destination_path):
    source_path = Path(source_path).expanduser().resolve(strict=True)
    if source_path == Path(destination_path).expanduser().resolve():
        raise ValueError("source and destination must differ")
    source_name = str(source_path)
    prior = db.execute("SELECT report FROM imports WHERE source=?", (source_name,)).fetchone()
    if prior:
        return {"replayed": True, "report": json.loads(prior[0]),
                "note": "This source was already imported; later source changes are not resynchronized."}
    imported, skipped = Counter(), Counter()
    notes, skipped_records = [], []
    # Keep a consistent read transaction, including committed WAL contents.
    # No immutable=1: it would ignore a source's WAL. No writes or checkpoints.
    with closing(sqlite3.connect(source_path.as_uri() + "?mode=ro", uri=True)) as source:
        source.row_factory = sqlite3.Row
        source.execute("PRAGMA query_only=ON")
        source.execute("BEGIN")
        tables = {item[0] for item in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if source.execute("PRAGMA user_version").fetchone()[0] != 1 or not {"works", "records", "operations", "claims"} <= tables:
            raise ValueError("source must be a version-1 Devflow state.sqlite3 database")
        states = {item["work_id"]: json.loads(item["state"]) for item in source.execute("SELECT work_id,state FROM works")}
        records = {identity: dict(state.get("records", {})) for identity, state in states.items()}
        for item in source.execute("SELECT work_id,record_key,payload FROM records"):
            records.setdefault(item["work_id"], {}).setdefault(item["record_key"], json.loads(item["payload"]))
        source_tag = hashlib.sha256(source_name.encode()).hexdigest()[:16]

        def reference(work_id, key):
            return source_name + "#" + work_id + "/" + key

        def skip(kind, work_id, key):
            skipped[kind] += 1
            skipped_records.append({"kind": kind, "id": key, "source_ref": reference(work_id, key)})

        def note(work_id, key, text, raw, **facts):
            history(db, "legacy-note:" + source_tag + ":" + work_id + ":" + key,
                    work_id if row(db, "works", work_id) else None, "legacy", key,
                    "import", {name: value for name, value in raw.items()
                               if name in {"revision", "operation_id", "request_hash", "approved_delta", "authority_reference", "operation", "external_id", "details", "limitations"}},
                    note=text, source_ref=reference(work_id, key), **facts)
            imported["history"] += 1

        # Import work identities first, so shared usage allocations retain their
        # actual destinations. Existing works are never overwritten.
        for work_id, state in states.items():
            unresolved = [(key, action) for key, action in state.get("actions", {}).items()
                          if action.get("status") not in {"confirmed", "failed", "canceled", "completed", "replaced"}]
            if row(db, "works", work_id):
                skip("existing_work", work_id, work_id)
            else:
                contract, attempt = state.get("contract") or {}, state.get("attempt") or {}
                candidate = records[work_id].get("candidate:" + str(state.get("candidate_id")), {})
                source_info = contract.get("source") or {}
                blocker = state.get("blocker")
                blocker = encode(blocker) if blocker is not None and not isinstance(blocker, str) else blocker
                if unresolved:
                    blocker = (blocker + "; " if blocker else "") + "Imported unresolved legacy operations; inspect history before continuing"
                record(db, "work", dict(id=work_id, title=contract.get("title") or work_id,
                       repository=candidate.get("repository") or (state.get("authority") or {}).get("repository"),
                       issue=source_info.get("reference"), commit=candidate.get("head_sha"),
                       status=state.get("lifecycle"), stage=state.get("phase"), blocker=blocker,
                       started_at=attempt.get("started_at"),
                       details={"legacy_source": source_name, "legacy_revision": state.get("revision")}))
                imported["work"] += 1
            for key, action in unresolved:
                note(work_id, "action:" + key, "Unresolved legacy operation; import is not recovery", action,
                     status=action.get("status"))
            for index, event in enumerate(state.get("history", [])):
                note(work_id, "history:" + str(index), event.get("command"), event,
                     occurred_at=event.get("recorded_at"), stage=event.get("to_phase"), previous_stage=event.get("from_phase"))
            for index, event in enumerate(state.get("phase_history", [])):
                note(work_id, "phase:" + str(index), "Legacy phase interval", event,
                     stage=event.get("phase"), started_at=event.get("started_at"), ended_at=event.get("ended_at"))

        pending_usage, segment_runs = {}, {}
        for work_id, entries in records.items():
            if work_id not in states:
                for key in entries:
                    skip("orphan_record", work_id, key)
                continue
            # Mutable finding projection is the latest disposition, whereas the
            # original immutable record can still say open after a verified fix.
            entries.update({"finding:" + key: value for key, value in states[work_id].get("findings", {}).items()})
            for key, original in entries.items():
                kind = original.get("record_type")
                if kind == "usage":
                    identity = original["response_id"]
                    if identity in pending_usage and pending_usage[identity] != original:
                        raise ValueError("conflicting legacy usage for response " + identity)
                    pending_usage[identity] = original
                    continue
                ref = reference(work_id, key)
                common = {"id": "legacy:" + work_id + ":" + key, "work_id": work_id,
                          "details": {"legacy_ref": ref}}
                candidate = entries.get("candidate:" + str(original.get("candidate_id")), {})
                target = None
                if kind == "execution_segment":
                    target = "run"
                    common.update(agent=original.get("task_id"), role=original.get("role"), model=original.get("model_id"),
                                  effort=original.get("reasoning_effort"), started_at=original.get("started_at"),
                                  ended_at=original.get("ended_at"), source_ref=original.get("source_reference") or ref)
                elif kind in {"check_evidence", "gate_result", "delivery"}:
                    target = "result"
                    artifact = original.get("artifact_hash") or original.get("producer_result_artifact_hash")
                    common["details"].update({name: original[name] for name in
                                              ("evidence_ids", "finding_ids", "limitations", "recipe_id", "observed_result", "endpoint", "resulting_merge") if name in original})
                    common.update(kind={"check_evidence": "check", "gate_result": original.get("role"), "delivery": "delivery"}[kind],
                                  status=original.get("execution_status") if kind == "check_evidence" else original.get("status"), commit=candidate.get("head_sha"),
                                  evidence_ref=str(source_path.parent / "artifacts" / artifact) if artifact else ref,
                                  recorded_at=original.get("completed_at") or original.get("delivered_at") or original.get("ended_at") or original.get("recorded_at"))
                elif kind == "finding":
                    target = "finding"
                    common["details"].update({name: original[name] for name in
                                              ("pr_reference", "evidence_ids", "fix_evidence_ids", "closure", "followup_reference") if name in original})
                    common.update(summary=original.get("summary"), severity=original.get("severity"), status=original.get("disposition"),
                                  commit=candidate.get("head_sha"), fix_ref=original.get("fix_reference"),
                                  thread_ref=original.get("thread_id"), evidence_ref=ref, recorded_at=original.get("detected_at"))
                elif kind == "outcome_event":
                    note(work_id, key, original.get("event_kind"), original, occurred_at=original.get("occurred_at"))
                    continue
                if target:
                    result = record(db, target, common)
                    if kind == "execution_segment":
                        segment_runs[original["segment_id"]] = common["id"]
                    if result["replayed"]:
                        skip(target, work_id, key)
                    else:
                        imported[target] += 1
                else:
                    skip(kind or "unknown_record", work_id, key)

        for identity, original in pending_usage.items():
            allocations = original.get("allocations", [])
            if any(not row(db, "works", item["work_id"]) for item in allocations):
                skip("usage_missing_allocation_work", "shared", identity)
                notes.append("Usage " + identity + " remains in source: an allocation references an absent work")
                continue
            components = [original.get(key) for key in ("uncached_input_tokens", "cache_read_tokens", "cache_write_tokens")]
            total_input = sum(components) if all(value is not None for value in components) else None
            # Legacy cache_read and cache_write were disjoint from uncached input.
            # Preserve the cache-write subset separately; never add reasoning to output.
            run_id = segment_runs.get(original.get("segment_id"))
            run = row(db, "runs", run_id) if run_id else None
            result = record(db, "usage", dict(id="legacy:usage:" + identity, agent=original.get("task_id"),
                            run_id=run_id, model=run["model"] if run else None,
                            input_tokens=total_input, cached_input_tokens=original.get("cache_read_tokens"),
                            cache_write_tokens=original.get("cache_write_tokens"), output_tokens=original.get("output_tokens"),
                            reasoning_output_tokens=original.get("reasoning_output_tokens"),
                            estimated_cost_usd=original.get("api_equivalent_usd"), estimated_credits=original.get("estimated_codex_credits"),
                            allocations=allocations, recorded_at=original.get("recorded_at"), source_ref=source_name + "#usage:" + identity,
                            details={name: original[name] for name in ("segment_id", "price_snapshot_id", "pricing_status", "attribution_status", "ccusage_version") if name in original}))
            if result["replayed"]:
                skip("usage", "shared", identity)
            else:
                imported["usage"] += 1

        # The operations table is a replay journal, not execution evidence.
        # Preserve unfinished backlog writes visibly; completed journals and
        # ownership claims remain in the source instead of reviving host state.
        for operation in source.execute("SELECT operation_id,result FROM operations"):
            value = json.loads(operation["result"])
            if value.get("kind") == "backlog_capture" and value.get("status") in {"dispatched", "ambiguous"}:
                note(value.get("work_id", "unassigned"), "operation:" + operation["operation_id"],
                     "Unresolved legacy backlog operation; import is not recovery", value, status=value.get("status"))
            else:
                skip("operation_journal", "journal", operation["operation_id"])
        for claim in source.execute("SELECT work_id FROM claims"):
            skip("ownership_claim", claim["work_id"], claim["work_id"])
    report = {"source": source_name, "imported": dict(imported), "skipped": dict(skipped), "skipped_records": skipped_records, "notes": notes,
              "limitations": ["One-shot historical import; existing works are never rewritten and source changes are not resynchronized.",
                              "Import does not recover operations or establish successful execution.",
                              "Unsupported records and original artifacts remain in the source; retain the source database and artifacts directory."]}
    insert(db, "imports", {"source": source_name, "imported_at": now(), "report": encode(report)})
    return {"replayed": False, "report": report}
