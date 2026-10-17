"""Revision checkpoint namespaces follow the highest revision that re-qualified a chunk."""

from __future__ import annotations

import pytest

from devflow_temporal import delivery_feature_revisions as revisions
from devflow_temporal.delivery_execution_registry import ExecutionRegistry

PREFIX = revisions.PREFIX


def receipt(number, affected, implementation=()):
    return {
        "revision_id": f"revision-{number}",
        "identity": {"plan_revision": number},
        "affected_chunks": list(affected),
        "implementation_chunks": list(implementation),
    }


def _written(monkeypatch, values, key):
    """Write ``key`` by the live rule, then read it back through the revision view."""
    monkeypatch.setattr(revisions, "_values", lambda spec, shared=None: values)
    written = revisions.revision_checkpoint_key({}, key)
    return written, revisions.revision_checkpoints({}, {**values, written: {"ok": True}})


@pytest.mark.parametrize(
    "key", ["verified:A", "assignment:A:build"]
)
def test_later_unrelated_revision_keeps_proof_under_the_revision_that_requalified_it(
    monkeypatch, key,
):
    values = {
        PREFIX + "adopted:revision-2": receipt(2, ["A"], ["A"]),
        PREFIX + "adopted:revision-3": receipt(3, ["B"], ["B"]),
    }
    written, visible = _written(monkeypatch, values, key)
    assert written == "revision:2:" + key
    assert visible[key] == {"ok": True}
    assert key not in revisions.revision_checkpoints({}, values)


def test_revision_that_requalifies_a_chunk_again_hides_the_earlier_namespace(monkeypatch):
    values = {
        PREFIX + "adopted:revision-2": receipt(2, ["A"], ["A"]),
        "revision:2:verified:A": {"run_id": "under-two"},
        "revision:2:build:A": {"sha256": "under-two"},
        PREFIX + "adopted:revision-3": receipt(3, ["A"], ["A"]),
    }
    visible = revisions.revision_checkpoints({}, values)
    assert "verified:A" not in visible and "build:A" not in visible
    written, visible = _written(monkeypatch, values, "verified:A")
    assert written == "revision:3:verified:A"
    assert visible["verified:A"] == {"ok": True}


def test_integration_pass_proof_namespaces_under_the_requalifying_revision(monkeypatch):
    values = {
        PREFIX + "adopted:revision-2": receipt(2, ["A"]),
        PREFIX + "adopted:revision-3": receipt(3, ["B"]),
        "integration-pass": {"number": 4},
    }
    written, visible = _written(monkeypatch, values, "pass:4:verified:A")
    assert written == "revision:2:pass:4:verified:A"
    assert visible["verified:A"] == {"ok": True}
    stale = {**values, "revision:2:pass:3:verified:A": {"run_id": "previous-pass"}}
    assert "verified:A" not in revisions.revision_checkpoints({}, stale)


def test_journalled_plain_proof_after_its_revision_stays_current(tmp_path):
    shared = ExecutionRegistry(tmp_path / "control" / "execution.sqlite3")
    issue = {"id": "I_feature", "repository_id": "R_fixture", "url": "https://example.invalid/1"}
    token = shared.claim({"issue": issue}, "feature-run", str(tmp_path / "local.sqlite3"))
    shared.checkpoint(token, "verified:A", {"run_id": "before-revision-two"})
    shared.checkpoint(token, "pass:1:verified:A", {"run_id": "before-revision-two"})
    shared.checkpoint(token, "build:B", {"sha256": "before-revision-three"})
    shared.checkpoint(token, PREFIX + "adopted:revision-2", receipt(2, ["A"], ["A"]))
    shared.checkpoint(token, PREFIX + "adopted:revision-3", receipt(3, ["B"], ["B"]))
    # The earlier namespacing rule wrote these plainly once revision 3 was latest.
    shared.checkpoint(token, "build:A", {"sha256": "after-revision-three"})
    shared.checkpoint(token, "pass:2:verified:A", {"run_id": "after-revision-three"})
    values = shared.checkpoints(issue["id"])
    assert list(values)[:4] == [
        "verified:A", "pass:1:verified:A", "build:B", PREFIX + "adopted:revision-2",
    ]

    visible = revisions.revision_checkpoints({}, values)
    assert "verified:A" not in visible and "build:B" not in visible
    assert visible["build:A"] == {"sha256": "after-revision-three"}
    assert visible["pass:2:verified:A"] == {"run_id": "after-revision-three"}

    aliased = {key: value for key, value in values.items() if key != "verified:A"}
    aliased["verified:A"] = aliased["pass:2:verified:A"]
    aliased["integration-pass"] = {"number": 2}
    assert revisions.revision_checkpoints({}, aliased)["verified:A"] == {
        "run_id": "after-revision-three"
    }
    stale_pass = {key: value for key, value in values.items() if key != "verified:A"}
    stale_pass["verified:A"] = stale_pass["pass:1:verified:A"]
    stale_pass["integration-pass"] = {"number": 1}
    assert "verified:A" not in revisions.revision_checkpoints({}, stale_pass)


def test_new_integration_pass_requires_fresh_revision_proofs_and_assignments(tmp_path, monkeypatch):
    from devflow_temporal import delivery_feature_pass as passes

    shared = ExecutionRegistry(tmp_path / "control" / "execution.sqlite3")
    issue = {"id": "I_feature", "repository_id": "R_fixture", "url": "https://example.invalid/1"}
    token = shared.claim({"issue": issue}, "feature-run", str(tmp_path / "local.sqlite3"))
    chunks = ["api1", "sql1", "web1"]
    shared.checkpoint(token, PREFIX + "adopted:revision-2", receipt(2, chunks, chunks))
    shared.checkpoint(token, "revision:2:build:api1", {"sha256": "preserved-build"})
    for chunk in chunks:
        shared.checkpoint(token, "revision:2:verified:" + chunk, {"run_id": "old-" + chunk})
        shared.checkpoint(token, "revision:2:assignment:" + chunk + ":chunk",
                          {"run_id": "old-" + chunk})
    previous = shared.checkpoints(issue["id"])
    spec = {"feature_delivery": {"owner": token}, "feature_plan_revision": {"plan_revision": 2}}
    monkeypatch.setattr(passes, "registry", lambda _: shared)
    monkeypatch.setattr(revisions, "_values", lambda *_: shared.checkpoints(issue["id"]))
    assert passes.checkpoints(spec)["verified:web1"] == {"run_id": "old-web1"}
    shared.checkpoint(token, "integration-pass:1", {"number": 1, "target": "current-trunk"})
    visible = passes.checkpoints(spec)
    for chunk in chunks:
        assert "verified:" + chunk not in visible
        assert "assignment:" + chunk + ":chunk" not in visible
        for alias in ("verified:" + chunk, "assignment:" + chunk + ":chunk"):
            key = passes.checkpoint_key(spec, alias)
            assert key == "revision:2:pass:1:" + alias
            shared.checkpoint(token, key, {"run_id": "current-" + chunk})
    visible = passes.checkpoints(spec)
    assert visible["build:api1"] == {"sha256": "preserved-build"}
    for chunk in chunks:
        assert visible["verified:" + chunk] == {"run_id": "current-" + chunk}
        assert visible["assignment:" + chunk + ":chunk"] == {"run_id": "current-" + chunk}
    assert all(shared.checkpoints(issue["id"])[key] == value for key, value in previous.items())
