"""Settings semantics cross the actual marked-release CLI and artifact store."""

import json

import pytest
import test_user_request_console
from test_user_request_console import Console
from test_user_request_console import installed as installed

from devflow.adapters.sqlite_store import SQLiteStore


@pytest.fixture
def stopped_console(tmp_path, installed, monkeypatch, request):
    calls = []
    original_run = test_user_request_console.subprocess.run

    def run(*args, **kwargs):
        result = original_run(*args, **kwargs)
        argv = args[0] if args else kwargs.get("args")
        if isinstance(argv, list) and "devflow.cli" in argv:
            calls.append({"argv": argv, "input": kwargs.get("input"), "exit_code": result.returncode,
                          "stdout": result.stdout, "stderr": result.stderr})
        return result

    request.addfinalizer(lambda: (tmp_path / "settings-cli-evidence.json").write_text(
        json.dumps(calls, indent=2) + "\n"))
    monkeypatch.setattr(test_user_request_console.subprocess, "run", run)
    s = Console(tmp_path, installed)
    s.start()
    s.mutate("work.block", blocker={"code": "synthetic-operational-stop",
                                   "reason": "Synthetic prerequisite needs verified repair",
                                   "next_action": "Record operational recovery evidence"})
    return s


def recapture(s, **changes):
    before = s.show()
    old = before["records"]["workflow_snapshot:" + before["attempt"]["workflow_snapshot_id"]]
    old_settings = json.loads((s.state_dir / "artifacts" / old["model_policy_hash"]).read_bytes())
    fresh = s.call("snapshot.capture", {"snapshot_id": "recaptured-settings", "effective_settings":
                   old_settings | {"source_reference": "synthetic:same-settings-observed-later"} | changes})
    return before, old, fresh


def amend_request(s, fresh):
    return {"operation_id": "settings-amendment", "work_id": s.contract["work_id"],
            "expected_revision": s.revision, "record": s.contract | {"scope_revision": 2},
            "user_request": s.ready_request["user_request"], "workflow_snapshot": fresh}


def test_metadata_only_recapture_retains_blocker_and_original_provenance(stopped_console):
    s = stopped_console
    before, old, fresh = recapture(s)
    assert old["model_policy_hash"] != fresh["model_policy_hash"]
    # Serialized caller claims are not trusted semantic comparison inputs.
    s.call("work.amend", amend_request(s, fresh) | {
        "operative_change": True, "amendment_settings": [{"model": "old"}, {"model": "new"}]})
    after = s.show()
    assert after["blocker"] == before["blocker"]
    assert after["attempt"]["blocker"] == before["blocker"]
    assert after["candidate_id"] == before["candidate_id"] is None
    assert after["scope_hash"] == before["scope_hash"] and after["authority"] == before["authority"]
    assert after["history"][:-1] == before["history"]
    assert all(after["records"][key] == value for key, value in before["records"].items())
    assert after["records"]["workflow_snapshot:" + fresh["snapshot_id"]] == fresh
    assert not any(r.get("record_type") == "operational_recovery" for r in after["records"].values())
    settings = [json.loads((s.state_dir / "artifacts" / snapshot["model_policy_hash"]).read_bytes())
                for snapshot in (old, fresh)]
    assert settings[0] | {"source_reference": settings[1]["source_reference"]} == settings[1]
    (s.state_dir.parent / "settings-semantic-proof.json").write_text(json.dumps({
        "before": before, "after": after, "old_snapshot": old, "new_snapshot": fresh,
        "old_settings": settings[0], "new_settings": settings[1]}, indent=2) + "\n")


@pytest.mark.parametrize("setting,value", [("model", "different-model"), ("reasoning_effort", "different-effort"),
                                          ("service_tier", "different-tier"), ("role", "implementation_worker")])
def test_real_settings_change_remains_an_operative_amendment(stopped_console, setting, value):
    s = stopped_console
    before, _, fresh = recapture(s, **{setting: value})
    s.call("work.amend", amend_request(s, fresh))
    after = s.show()
    assert after["blocker"] is None
    assert after["scope_hash"] == before["scope_hash"]
    assert all(after["records"][key] == record for key, record in before["records"].items())


@pytest.mark.parametrize("which", ["old", "new"])
@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_both_settings_artifacts_are_verified_before_amendment(stopped_console, which, damage):
    s = stopped_console
    before, old, fresh = recapture(s)
    artifact = s.state_dir / "artifacts" / (old if which == "old" else fresh)["model_policy_hash"]
    if damage == "missing":
        artifact.unlink()
    else:
        artifact.write_bytes(b"Synthetic corrupted settings bytes")
    s.call("work.amend", amend_request(s, fresh), error=f"{damage}_artifact")
    assert s.show() == before


@pytest.mark.parametrize("content", [b"{", b'{"model":"synthetic","reasoning_effort":"high",'
                                    b'"source_reference":"synthetic:observation","role":{}}'])
def test_unverifiable_settings_bytes_cannot_authorize_amendment(stopped_console, content):
    s = stopped_console
    before, _, fresh = recapture(s)
    # Real private artifact, deliberately not a valid observed-settings object.
    artifact_hash = SQLiteStore(s.state_dir).put_artifact(content)
    fresh |= {"model_policy_hash": artifact_hash, "effective_settings_reference": f"sha256:{artifact_hash}"}
    s.call("work.amend", amend_request(s, fresh), error="settings_invalid")
    assert s.show() == before
