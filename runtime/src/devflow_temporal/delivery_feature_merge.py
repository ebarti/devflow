"""Merge one explicitly authorized feature stack after fresh evidence readback."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta

from .contracts import canonical_json, digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from .delivery_feature_execution import registry
from .delivery_feature_pass import checkpoints as current_checkpoints
from .delivery_feature_publication import current_record, live_members
from .delivery_feature_workflow import publication
from .delivery_github_contract import GitHubDelivery, ordered_chunks
from .delivery_merge import MergeBroker, native_implementation_output, require_merge_gates
from .delivery_resources import observe_finalized_resources
from .delivery_stopped_resume import executed_specs


def authorize(store, spec, requested, command, *, settlement=False):
    with store._connect() as db:
        row = db.execute(
            "SELECT pr_json,phase FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if (
            row is None
            or (row["phase"] != "merging" and not settlement)
            or not requested.get("scope_complete")
            or json.loads(row["pr_json"] or "null") != requested
        ):
            raise OwnershipConflict("merge differs from the complete reviewed feature revision")
        if spec["authorized_endpoint"] == "merged":
            return {
                "kind": "admitted_endpoint",
                "command_id": spec["command_id"],
                "request_digest": spec["request_digest"],
                "publication": requested,
            }
        if not isinstance(command, dict) or command.get("answer") != "merge":
            raise OwnershipConflict("feature merge requires an explicit merge instruction")
        saved = db.execute(
            "SELECT * FROM delivery_mutations WHERE command_id=? AND run_id=?",
            (command.get("command_id"), spec["run_id"]),
        ).fetchone()
        if (
            not saved
            or saved["kind"] != "decision"
            or saved["state"] == "rejected"
            or saved["request_digest"] != digest(command)
            or command.get("decision_id")
            != spec["run_id"] + ":merge:" + str(requested["record_revision"])
        ):
            raise OwnershipConflict("merge command is not an authenticated current decision")
        return {"kind": "explicit_instruction", "command": command, "publication": requested}


def evidence(store, parent, record, live, gh):
    checkpoints = current_checkpoints(parent)
    members = record["manifest"]["publication"]["members"]
    if len(members) != len(ordered_chunks(record["manifest"]["plan"])):
        raise OwnershipConflict("unpublished feature scope remains")
    trees, predecessor = [], None
    native_stack = record["manifest"]["publication"]["stack_id"] is not None
    for index, (member, remote) in enumerate(zip(members, live, strict=True)):
        proof = checkpoints.get("verified:" + member["chunk_id"])
        if not proof or proof["head"] != member["head"] or proof["number"] != member["number"]:
            raise OwnershipConflict("feature chunk has no current verification checkpoint")
        if proof["store_path"] != str(store.config.tracking_db):
            raise OwnershipConflict("feature evidence belongs to another runtime store")
        spec = store.effective_spec(proof["run_id"])
        with store._connect() as db:
            row = db.execute(
                "SELECT * FROM delivery_runs WHERE run_id=?", (proof["run_id"],)
            ).fetchone()
            checks = json.loads(row["checks_json"] or "{}")
            candidate, pr = json.loads(row["candidate_json"]), json.loads(row["pr_json"])
            attempts = [
                dict(item)
                for item in db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY rowid",
                    (proof["run_id"],),
                )
            ]
            effects = [
                json.loads(item["request_json"])
                for item in db.execute(
                    "SELECT request_json,observed_json FROM delivery_effects "
                    "WHERE run_id=? AND kind='publish' AND state='complete'",
                    (proof["run_id"],),
                )
                if json.loads(item["observed_json"] or "null") == pr
            ]
        if row["outcome"] != "delivered" or digest(checks) != proof["checks_digest"] or not effects:
            raise OwnershipConflict("verified chunk evidence changed")
        implementations = [item for item in attempts if item["role"] == "implement"]
        if not implementations:
            raise OwnershipConflict("chunk lacks native implementation evidence")
        # A resumed chunk keeps the implementation an earlier sealed execution ran.
        implementations[-1]["controller_output_candidate"] = native_implementation_output(
            spec, implementations[-1], executed=executed_specs(store, spec)
        )
        # Endpoint authority is established separately above. All the existing
        # source, independent-session, cleanup and gate requirements still apply.
        require_merge_gates(
            {**spec, "authorized_endpoint": "merged", "merge_version": 1},
            candidate,
            pr,
            checks,
            attempts,
            effects[-1],
        )
        observe_finalized_resources(spec)
        commit = gh.api(f"repos/{spec['github_repo']}/git/commits/{member['head']}")
        tree = commit["tree"]["sha"]
        if not remote["merged"]:
            if native_stack and index and predecessor is None:
                raise OwnershipConflict("native stack predecessor has no current hosted CI binding")
            predecessor = MergeBroker(store, spec).live_ci(
                pr, remote, tree, native_stack=native_stack, predecessor=predecessor
            )
        else:
            predecessor = None
        trees.append(tree)
    # Stack merge is enforced against trunk, not the unprotected layer branches.
    base = {**parent, "publication_base_ref": members[0]["base_branch"]}
    MergeBroker(store, base).require_base_enforcement()
    return trees


def closure_groups(record):
    """Map each required chunk to its exact child before admitting a merge effect."""
    manifest = record["manifest"]
    plan = manifest["plan"]
    members = manifest["publication"]["members"]
    required = ordered_chunks(plan)
    if [member["chunk_id"] for member in members] != [chunk["id"] for chunk in required]:
        raise OwnershipConflict("merge publication does not contain every required chunk once")
    by_chunk = {member["chunk_id"]: member for member in members}
    groups, seen_issues = [], set()
    for stream in plan["workstreams"]:
        child = manifest["workstream_issues"].get(stream["id"])
        if not child or child["id"] in seen_issues:
            raise OwnershipConflict("workstream closure has no unique recorded child issue")
        if stream.get("issue_number") not in {None, child["number"]}:
            raise OwnershipConflict("workstream plan and child identity disagree")
        seen_issues.add(child["id"])
        chunk_ids = [chunk["id"] for chunk in stream["chunks"]]
        for chunk_id in chunk_ids:
            member = by_chunk[chunk_id]
            expected = {"workstream_id": stream["id"], "issue_id": child["id"],
                        "issue_number": child["number"], "issue_url": child["url"]}
            explicit = any(field in member for field in expected)
            if (manifest["version"] >= 2 or explicit) and any(
                member.get(field) != value for field, value in expected.items()
            ):
                raise OwnershipConflict("chunk publication belongs to another child issue")
        groups.append((child, chunk_ids))
    if set(manifest["workstream_issues"]) != {stream["id"] for stream in plan["workstreams"]}:
        raise OwnershipConflict("workstream closure includes an unplanned child issue")
    return groups


def hierarchy(spec, record, gh):
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    for child in [issue, *record["manifest"]["workstream_issues"].values()]:
        raw = gh.api(f"repos/{spec['github_repo']}/issues/{child['number']}")
        if (raw["node_id"] != child["id"] or raw.get("pull_request")
                or raw["number"] != child["number"] or raw["html_url"] != child["url"]):
            raise OwnershipConflict("feature issue identity changed")
        if child["id"] != issue["id"]:
            parent = gh.api(f"repos/{spec['github_repo']}/issues/{child['number']}/parent")
            if parent["node_id"] != issue["id"]:
                raise OwnershipConflict("workstream hierarchy changed before merge or closure")


MERGE_SUBMISSIONS = 2


def _drafts(members, live):
    """GitHub refuses to merge a draft layer; the owner's draft is a wait, not a refusal."""
    waiting = [member["number"] for member, raw in zip(members, live, strict=True)
               if raw["draft"] and not raw["merged"]]
    return ({"state": "pending", "reason": "draft_pull_request", "pull_requests": waiting}
            if waiting else None)


def _moved_target(store, spec, members, live, gh):
    """Return the moved trunk when the lowest unmerged layer's verified base is stale."""
    opened = next((index for index, raw in enumerate(live) if not raw["merged"]), None)
    proof = (current_checkpoints(spec).get("verified:" + members[opened]["chunk_id"])
             if opened is not None else None)
    if not proof:
        return None
    base = store.effective_spec(proof["run_id"])["base_sha"]
    # Native-stack PR base metadata can lag the authoritative trunk ref.
    trunk = members[0]["base_branch"]
    repo = f"repos/{spec['github_repo']}"
    head = gh.api(f"{repo}/git/ref/heads/{trunk}")["object"]["sha"]
    if not opened:
        return head if head != base else None
    # The owner merged the lower layers by hand. The rest merges onto the trunk,
    # which must hold exactly the tree this layer was verified on.
    if head == base or (gh.api(f"{repo}/git/commits/{head}")["tree"]["sha"]
                        == gh.api(f"{repo}/git/commits/{base}")["tree"]["sha"]):
        return None
    merged = ", ".join("#" + str(member["number"]) for member in members[:opened])
    raise OwnershipConflict(f"feature PRs {merged} were merged by hand and {trunk} has moved "
                            "since; the remaining stack needs re-integration before it can merge")


def _refused(submissions):
    last = submissions[-1]
    detail = (last.get("stderr") or last.get("error") or "").strip() or "no diagnostic output"
    return (f"feature stack merge was refused {len(submissions)} times; last exit code "
            f"{last.get('exit_code')}: {detail[-500:]}")


def merge(store, spec, requested, command=None, *, gh=None, execute=None, cancelled=None,
          settlement=False):
    gh = gh or GitHubDelivery()
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    authority = authorize(store, spec, requested, command, settlement=settlement)
    record = current_record(spec, gh)
    if publication(record, complete=True) != requested:
        raise OwnershipConflict("feature plan or stack changed after merge authorization")
    groups = closure_groups(record)
    key = "feature-merge:" + digest(authority)
    if settlement and shared.effect(token["issue_id"], key) is None:
        raise OwnershipConflict("settlement cannot originate a new merge")
    with shared.mutation(token):
        hierarchy(spec, record, gh)
        # Closed, moved, retargeted or out-of-order merged layers stop here by name.
        live = live_members(spec, record, gh)
        members = record["manifest"]["publication"]["members"]
        prior = shared.effect(token["issue_id"], key)
        if prior:
            if prior["kind"] != "feature_merge" or prior["request"]["authority"] != authority:
                raise OwnershipConflict("original merge authority changed")
            trees = prior["request"]["trees"]
            intent = {"fresh": False, "state": prior["state"]}
            submissions = (prior["result"] or {}).get("submissions", [])
        else:
            layers = [item["merged"] for item in live]
            if layers != sorted(layers, reverse=True):
                late = next(member for member, done in zip(
                    members[layers.index(False):], layers[layers.index(False):],
                    strict=True) if done)
                raise OwnershipConflict(
                    f"feature PR #{late['number']} for chunk {late['chunk_id']} was merged "
                    "before the stack layers below it")
            if not all(layers):
                waiting = _drafts(members, live)
                if waiting:
                    return waiting
                target = _moved_target(store, spec, members, live, gh)
                if target:
                    return {"state": "needs_integration", "target": target}
            trees = evidence(store, spec, record, live, gh)
            if cancelled and cancelled.is_set():
                raise OwnershipConflict("merge cancelled before submission")
            intent = shared.intent(
                token, key, "feature_merge", {"authority": authority, "trees": trees}
            )
            submissions = []
        if not all(item["merged"] for item in live):
            last = submissions[-1] if submissions else {}
            if not intent["fresh"]:
                if last.get("state") != "not_started" or intent["state"] != "pending":
                    # The original command may still be queued or in flight.
                    return {"state": "pending", "reason": "original_merge_readback",
                            "effect_key": key}
                if settlement:
                    # Only an authenticated local launch failure proves no request was sent.
                    # A CLI exit code cannot establish the remote outcome.
                    shared.finish_effect(token, key, {"submissions": submissions},
                                         no_effect=True)
                    return {"state": "absent", "reason": "merge_submission_failed",
                            "effect_key": key}
                if len(submissions) >= MERGE_SUBMISSIONS:
                    raise OwnershipConflict(_refused(submissions))
                waiting = _drafts(members, live)
                if waiting:
                    return waiting
                if _moved_target(store, spec, members, live, gh):
                    raise OwnershipConflict(
                        _refused(submissions) + "; the trunk has moved since, so the stack "
                        "needs re-integration")
            if record["manifest"]["publication"]["stack_id"]:
                argv = ["gh", "stack", "merge", str(requested["stack_id"]), "--yes", "--squash"]
            else:
                argv = [
                    "gh",
                    "pr",
                    "merge",
                    str(members[0]["number"]),
                    "--repo",
                    spec["github_repo"],
                    "--squash",
                    "--match-head-commit",
                    members[0]["head"],
                ]
            execute = execute or subprocess.run
            if cancelled and cancelled.is_set():
                shared.finish_effect(
                    token, key, {"cancelled_before_submission": True,
                                 **({"submissions": submissions} if submissions else {})},
                    no_effect=True,
                )
                raise OwnershipConflict("merge cancelled before submission")
            # Record the attempt before running it: an attempt without an outcome
            # may have reached GitHub and is only ever read back, never repeated.
            submissions = [*submissions, {"attempt": len(submissions) + 1, "state": "started"}]
            shared.note_effect(token, key, {"submissions": submissions})
            try:
                result = execute(
                    argv,
                    cwd=spec["source_path"],
                    env={**os.environ, "GH_REPO": spec["github_repo"],
                         "GH_PROMPT_DISABLED": "1"},
                    capture_output=True,
                    text=True,
                    timeout=540,
                    check=False,
                )
            except (FileNotFoundError, PermissionError) as exc:
                # The command never started, so GitHub received no merge request.
                submissions[-1] = {**submissions[-1], "state": "not_started",
                                   "error": f"{type(exc).__name__}: {exc}"[-2000:]}
                shared.note_effect(token, key, {"submissions": submissions})
                raise
            submissions[-1] = {**submissions[-1],
                               "state": "unknown" if result.returncode else "submitted",
                               "exit_code": result.returncode,
                               "stderr": (getattr(result, "stderr", None) or "")[-2000:]}
            shared.note_effect(token, key, {"submissions": submissions})
            live = live_members(spec, record, gh)
            if not all(item["merged"] for item in live):
                return {
                    "state": "pending",
                    "reason": ("merge_submission_failed" if result.returncode
                               else "queued_or_unconfirmed_merge"),
                    "exit_code": result.returncode,
                    **({"stderr": submissions[-1]["stderr"][-500:]} if result.returncode
                       else {}),
                    "effect_key": key,
                }
        merged = []
        for member, raw, tree in zip(members, live, trees, strict=True):
            if not raw.get("merged_at") or not raw.get("merge_commit_sha"):
                raise OwnershipConflict("GitHub merge receipt is incomplete")
            commit = gh.api(f"repos/{spec['github_repo']}/git/commits/{raw['merge_commit_sha']}")
            if commit["tree"]["sha"] != tree:
                raise OwnershipConflict("merged source differs from the verified chunk tree")
            comparison = gh.api(
                f"repos/{spec['github_repo']}/compare/"
                f"{raw['merge_commit_sha']}...{members[0]['base_branch']}"
            )
            if (
                comparison.get("status") not in {"ahead", "identical"}
                or comparison.get("merge_base_commit", {}).get("sha") != raw["merge_commit_sha"]
            ):
                raise OwnershipConflict("merged chunk is not incorporated into the target branch")
            merged.append(
                {
                    "number": member["number"],
                    "head": member["head"],
                    "merged_at": raw["merged_at"],
                    "merge_commit": raw["merge_commit_sha"],
                }
            )
        shared.finish_effect(token, key, {"pull_requests": merged})
        # The merge command already obtained authoritative PR readbacks. Commit
        # them with a projection event now; a known merge must not wait for the
        # independent consumer's daily external-drift check.
        from .delivery_features import transition

        stamp = datetime.now(UTC)
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            schedule = db.execute(
                "SELECT value FROM delivery_sync_settings WHERE key='pr_interval_seconds'"
            ).fetchone()
            due = stamp + timedelta(seconds=schedule[0] if schedule else 86400)
            for member, raw in zip(members, live, strict=True):
                observed = {
                    "url": member["url"], "state": "MERGED", "head": raw["head"]["sha"],
                    "base": raw["base"].get("sha"), "merged_at": raw["merged_at"],
                }
                db.execute(
                    "INSERT INTO delivery_pr_observations VALUES (?,?,?,?,NULL) "
                    "ON CONFLICT(url) DO UPDATE SET observation_json=excluded.observation_json,"
                    "checked_at=excluded.checked_at,next_check_at=excluded.next_check_at,error=NULL",
                    (member["url"], canonical_json(observed), stamp.isoformat(), due.isoformat()),
                )
            transition(db, store.config, spec["run_id"])
    # GitHub issues represent the business outcome. Close the feature and its
    # workstreams only after every accepted chunk is actually merged.
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    closures = [*groups, (issue, [member["chunk_id"] for member in members])]
    closure_record = current_record(spec, gh)
    if publication(closure_record, complete=True) != requested:
        raise OwnershipConflict("feature plan or stack changed before issue closure")
    hierarchy(spec, closure_record, gh)
    merged_by_chunk = {member["chunk_id"]: receipt
                       for member, receipt in zip(members, merged, strict=True)}
    confirmed = {member["chunk_id"] for member, raw in zip(members, live, strict=True)
                 if raw.get("merged") is True and raw.get("merged_at")
                 and raw.get("merge_commit_sha")}
    for child, chunk_ids in closures:
        if not chunk_ids or not set(chunk_ids) <= confirmed:
            raise OwnershipConflict("issue still has required chunks without confirmed merges")
        child_merges = [merged_by_chunk[chunk_id] for chunk_id in chunk_ids]
        endpoint = f"repos/{spec['github_repo']}/issues/{child['number']}"
        with shared.mutation(token):
            if cancelled and cancelled.is_set():
                raise OwnershipConflict("merge completed; issue closure still needs reconciliation")
            close_key = key + ":close:" + child["id"]
            prior_closure = shared.effect(token["issue_id"], close_key)
            closure_request = {"issue": child["id"], "required_chunks": chunk_ids,
                               "merges": child_merges}
            if prior_closure:
                original = prior_closure["request"]
                # Preserve historical v1 effects. They recorded all feature
                # merges for each child, after the same complete-feature fence.
                expected = closure_request if "required_chunks" in original else {
                    "issue": child["id"], "merges": merged}
                if prior_closure["kind"] != "close_merged_issue" or original != expected:
                    raise OwnershipConflict("original issue closure obligation changed")
                closure_request = original
            receipt = shared.intent(token, close_key, "close_merged_issue", closure_request)
            raw = gh.api(endpoint)
            if raw["node_id"] != child["id"] or raw.get("pull_request"):
                raise OwnershipConflict("merged issue identity changed")
            if raw["state"] != "closed":
                if not receipt["fresh"]:
                    raise UnresolvedEffect("original issue closure still needs readback")
                gh.api(
                    endpoint, method="PATCH", body={"state": "closed", "state_reason": "completed"}
                )
                raw = gh.api(endpoint)
            if raw["state"] != "closed" or raw.get("state_reason") != "completed":
                raise OwnershipConflict("merged issue closure readback differs from delivery")
            shared.finish_effect(token, key + ":close:" + child["id"], {"state": "closed"})
    return {"state": "confirmed", "stack_id": requested["stack_id"], "pull_requests": merged}
