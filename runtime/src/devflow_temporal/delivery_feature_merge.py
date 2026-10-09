"""Merge one explicitly authorized feature stack after fresh evidence readback."""

from __future__ import annotations

import json
import os
import subprocess

from .contracts import digest
from .delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from .delivery_feature_execution import registry
from .delivery_feature_publication import current_record, live_members
from .delivery_feature_workflow import publication
from .delivery_github_contract import GitHubDelivery, ordered_chunks
from .delivery_merge import MergeBroker, native_implementation_output, require_merge_gates
from .delivery_resources import observe_finalized_resources


def authorize(store, spec, requested, command):
    with store._connect() as db:
        row = db.execute(
            "SELECT pr_json,phase FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if (
            row is None
            or row["phase"] != "merging"
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
    shared = registry(parent)
    checkpoints = shared.checkpoints(parent["feature_delivery"]["owner"]["issue_id"])
    members = record["manifest"]["publication"]["members"]
    if len(members) != len(ordered_chunks(record["manifest"]["plan"])):
        raise OwnershipConflict("unpublished feature scope remains")
    trees = []
    for member, remote in zip(members, live, strict=True):
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
        implementations[-1]["controller_output_candidate"] = native_implementation_output(
            spec, implementations[-1]
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
            MergeBroker(store, spec).live_ci(pr, remote, tree)
        trees.append(tree)
    # Stack merge is enforced against trunk, not the unprotected layer branches.
    base = {**parent, "publication_base_ref": members[0]["base_branch"]}
    MergeBroker(store, base).require_base_enforcement()
    return trees


def merge(store, spec, requested, command=None, *, gh=None, execute=None, cancelled=None):
    gh = gh or GitHubDelivery()
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    authority = authorize(store, spec, requested, command)
    record = current_record(spec, gh)
    if publication(record, complete=True) != requested:
        raise OwnershipConflict("feature plan or stack changed after merge authorization")
    key = "feature-merge:" + digest(authority)
    with shared.mutation(token):
        live = live_members(spec, record, gh)
        if any(item["state"] == "closed" and not item["merged"] for item in live):
            raise OwnershipConflict("a feature PR was closed without merging")
        prior = shared.effect(token["issue_id"], key)
        if prior:
            if prior["kind"] != "feature_merge" or prior["request"]["authority"] != authority:
                raise OwnershipConflict("original merge authority changed")
            trees = prior["request"]["trees"]
            intent = {"fresh": False, "state": prior["state"]}
        else:
            if any(item["merged"] for item in live) and not all(item["merged"] for item in live):
                raise OwnershipConflict("partial remote merge requires reconciliation")
            trees = evidence(store, spec, record, live, gh)
            if cancelled and cancelled.is_set():
                raise OwnershipConflict("merge cancelled before submission")
            intent = shared.intent(
                token, key, "feature_merge", {"authority": authority, "trees": trees}
            )
        members = record["manifest"]["publication"]["members"]
        if not all(item["merged"] for item in live):
            if not intent["fresh"]:
                return {"state": "pending", "reason": "original_merge_readback", "effect_key": key}
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
                    token, key, {"cancelled_before_submission": True}, no_effect=True
                )
                raise OwnershipConflict("merge cancelled before submission")
            result = execute(
                argv,
                cwd=spec["source_path"],
                env={**os.environ, "GH_REPO": spec["github_repo"], "GH_PROMPT_DISABLED": "1"},
                capture_output=True,
                text=True,
                timeout=540,
                check=False,
            )
            live = live_members(spec, record, gh)
            if not all(item["merged"] for item in live):
                return {
                    "state": "pending",
                    "reason": "queued_or_unconfirmed_merge",
                    "exit_code": result.returncode,
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
    # GitHub issues represent the business outcome. Close the feature and its
    # workstreams only after every accepted chunk is actually merged.
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    closures = [*record["manifest"]["workstream_issues"].values(), issue]
    for child in closures:
        endpoint = f"repos/{spec['github_repo']}/issues/{child['number']}"
        with shared.mutation(token):
            if cancelled and cancelled.is_set():
                raise OwnershipConflict("merge completed; issue closure still needs reconciliation")
            receipt = shared.intent(
                token,
                key + ":close:" + child["id"],
                "close_merged_issue",
                {"issue": child["id"], "merges": merged},
            )
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
