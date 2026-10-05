from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_broker
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import publication_base_ref
from devflow_temporal.delivery_store import DeliveryStore


def bind_default(source: Path) -> str:
    _git(source, "branch", "-M", "main")
    head = _git(source, "rev-parse", "HEAD")
    _git(source, "update-ref", "refs/remotes/origin/main", head)
    _git(source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    return head


@pytest.mark.parametrize("raw", ["main", "origin/main", "refs/heads/main", "HEAD", "sha"])
def test_branch_resolution_keeps_the_pinned_object(service, raw):
    store, _ = service
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    head = bind_default(source)
    assert publication_base_ref(source, head if raw == "sha" else raw, head) == "main"


@pytest.mark.parametrize("case", ["missing-default", "moved-default", "tag"])
def test_unbound_commit_or_tag_cannot_guess_a_publication_branch(service, case):
    store, _ = service
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    head = bind_default(source)
    raw = head
    if case == "missing-default":
        _git(source, "symbolic-ref", "--delete", "refs/remotes/origin/HEAD")
    elif case == "moved-default":
        (source / "README.md").write_text("Later base\n")
        _git(source, "commit", "-qam", "docs: advance default")
        _git(source, "update-ref", "refs/remotes/origin/main", _git(source, "rev-parse", "HEAD"))
    else:
        _git(source, "tag", "pinned-tag", head)
        raw = "pinned-tag"
    with pytest.raises(ValueError, match="publication"):
        publication_base_ref(source, raw, head)


def test_native_admission_freezes_branch_before_roles_or_effects(native_configuration):
    config, request = native_configuration
    repository = config.raw["repositories"]["fixture"]
    head = bind_default(Path(repository["source_path"]))
    repository.update(base_ref=head, expected_base_sha=head)
    spec = config.admit({**request, "base_ref": head})
    assert spec["base_ref"] == spec["base_sha"] == head
    assert spec["publication_base_ref"] == "main"


def test_unbound_native_pin_is_rejected_before_accepting_work(native_configuration):
    config, request = native_configuration
    repository = config.raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    head = bind_default(source)
    repository.update(base_ref=head, expected_base_sha=head)
    _git(source, "symbolic-ref", "--delete", "refs/remotes/origin/HEAD")
    store = DeliveryStore(config)
    with pytest.raises(ValueError, match="publication"):
        store.submit({**request, "base_ref": head})
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_runs").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0] == 0


def test_legacy_sha_publication_uses_branch_for_creation_and_readback(service, monkeypatch):
    store, request = service
    repository = store.config.raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    base = bind_default(source)
    repository.update(base_ref=base, expected_base_sha=base)
    store.config.path.write_text(json.dumps(store.config.raw))
    request = {**request, "base_ref": base}
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request["run_id"]))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700)
    (broker.checkout / "README.md").write_text("Owned document\n")
    before = broker.candidate()
    created = []
    original_run = delivery_broker._run

    def run(argv, **kwargs):
        if argv[:3] == ["gh", "pr", "create"]:
            assert argv[argv.index("--base") + 1] == "main"
            created.append(argv)
            return "https://github.com/example/fixture/pull/9"
        if argv[:3] == ["gh", "pr", "list"]:
            if not created:
                return "[]"
            return json.dumps([{
                "number": 9, "url": "https://github.com/example/fixture/pull/9",
                "state": "OPEN", "isDraft": False, "baseRefName": "main",
                "headRefName": request["branch"],
                "headRefOid": _git(broker.checkout, "rev-parse", "HEAD"),
                "title": "docs: document the owned feature",
            }])
        return original_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, "_run", run)
    result = broker.publish(0, before)
    assert result["base"] == base and result["state"] == "OPEN"
    assert broker._existing_pr()["baseRefName"] == "main"
    assert store.spec(request["run_id"])["base_ref"] == base
    assert store.spec(request["run_id"])["policy"]["max_repairs"] == 2
