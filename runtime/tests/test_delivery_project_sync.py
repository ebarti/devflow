"""Pure SQLite/Git/stub checks; no Temporal or native process fixtures."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from test_delivery_store import service as delivery_service

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_features import current, record_tracking, transition
from devflow_temporal.delivery_project_sync import DAY, ProjectSynchronizer, Superseded
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import _tracking_ready

service = delivery_service


class Remote:
    def __init__(self):
        self.state = "OPEN"
        self.head = "a" * 40
        self.reads = []
        self.writes = []
        self.failure = False

    def pull_request(self, url):
        self.reads.append(url)
        return {"url": url, "state": self.state, "head": self.head,
                "base": "b" * 40, "merged_at": None}

    def issue(self, url):
        return {"url": url, "state": "OPEN", "state_reason": None, "closed_at": None}

    def mirror(self, feature, fence):
        fence()
        self.writes.append(feature["status"])
        if self.failure:
            raise RuntimeError("GitHub unavailable")
        return {"status": feature["status"], "assignee": "owner"}


def published(service, *, active=False):
    store, request = service
    store.submit(request)
    receipt = {"url": "https://github.com/example/fixture/pull/7", "state": "OPEN",
               "head": "a" * 40, "number": 7}
    store.project(request["run_id"], phase="merging" if active else "delivered",
                  execution_state="running" if active else "terminal",
                  event_type="published", message="published",
                  outcome=None if active else "published_unmerged",
                  pull_request=receipt)
    return store, request, receipt


def test_event_and_feature_roll_back_with_run_transaction(service):
    store, request = service
    store.submit(request)
    with store._connect() as db:
        before = db.execute("SELECT payload_json FROM delivery_features").fetchone()[0]
        count = db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0]
    with pytest.raises(RuntimeError), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE delivery_runs SET phase='blocked'")
        store._event(db, request["run_id"], 2, "blocked", "fail", {})
        raise RuntimeError("crash before commit")
    with store._connect() as db:
        assert db.execute("SELECT payload_json FROM delivery_features").fetchone()[0] == before
        assert db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0] == count


def test_merge_observation_preserves_receipts_and_run_outcome(service):
    store, request, receipt = published(service)
    remote = Remote()
    remote.state = "MERGED"
    sync = ProjectSynchronizer([store], remote)
    result = sync.tick()[request["issue_url"]]
    assert result["status"] == "Done"
    assert result["mirror"]["state"] == "consistent"
    detail = store.detail(request["run_id"])
    assert detail["pull_request"] == receipt
    assert detail["outcome"] == "published_unmerged"
    assert detail["phase"] == "delivered"
    assert detail["feature"]["status"] == "Done"


def test_external_issue_completion_and_reopening_preserve_failed_attempt(service):
    store, request, receipt = published(service, active=True)
    store.project(request["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message="CI failed", outcome="blocked")
    original = store.detail(request["run_id"])
    remote = Remote()
    remote.issue = lambda url: {"url": url, "state": "CLOSED", "state_reason": "COMPLETED",
                                "closed_at": "2026-10-19T10:00:00Z"}
    stamp = datetime(2026, 10, 19, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    completed = sync.tick()[request["issue_url"]]
    assert completed["status"] == "Backlog"  # An open PR does not prove it merged.
    after = store.detail(request["run_id"])
    for key in ("phase", "outcome", "pull_request", "checks", "cleanup_recorded"):
        assert after[key] == original[key]
    assert after["feature"]["issue_observation"]["observation"]["state"] == "CLOSED"
    remote.issue = lambda url: {"url": url, "state": "OPEN", "state_reason": None,
                                "closed_at": None}
    stamp += timedelta(seconds=300)
    assert sync.tick()[request["issue_url"]]["status"] == "Blocked"
    assert receipt == store.detail(request["run_id"])["pull_request"]


def test_closed_completed_issue_finishes_legacy_partial_projection_without_forging_scope():
    from devflow_temporal.delivery_features import _status

    receipt = {"scope_complete": False, "pull_requests": [{"url": "pr"}]}
    row = {"request_json": json.dumps({"feature_delivery": True}),
           "pr_json": json.dumps(receipt), "phase": "cancelled", "outcome": "cancelled"}
    observed = [{"observation": {"state": "MERGED"}}]
    assert _status(row, observed) == "In Progress"
    assert _status(row, observed, issue_observation={"state": "CLOSED",
                                                    "state_reason": "COMPLETED"}) == "Done"
    assert receipt["scope_complete"] is False and row["outcome"] == "cancelled"
    assert _status(row, observed, issue_observation={"state": "CLOSED",
                                                    "state_reason": "NOT_PLANNED"}) != "Done"


def test_issue_read_failure_never_acknowledges_synced_project(service):
    store, request, _ = published(service)
    remote = Remote()

    def unavailable(_url):
        raise RuntimeError("issue read unavailable")

    remote.issue = unavailable
    result = ProjectSynchronizer([store], remote).tick()[request["issue_url"]]
    assert result["mirror"]["state"] == "pending"
    assert "issue read unavailable" in result["mirror"]["last_error"]
    assert remote.writes == []


@pytest.mark.parametrize(
    ("state", "head", "expected"),
    [("OPEN", "a" * 40, "Awaiting merge"), ("OPEN", "c" * 40, "Validating")],
)
def test_current_pr_lifecycle(service, state, head, expected):
    store, request, _ = published(service)
    remote = Remote()
    remote.state, remote.head = state, head
    assert ProjectSynchronizer([store], remote).tick()[request["issue_url"]]["status"] == expected


def test_poll_schedule_and_failure_backoff_survive_restart(service):
    store, request, _ = published(service)
    remote = Remote()
    remote.failure = True
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    clock = lambda: stamp  # noqa: E731
    sync = ProjectSynchronizer([store], remote,
                                     clock=clock, interval=DAY, project_interval=DAY)
    first = sync.tick()[request["issue_url"]]
    assert first["status"] == "Awaiting merge"
    assert first["mirror"]["state"] == "pending"
    assert "GitHub unavailable" in first["mirror"]["last_error"]
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote,
                                     clock=clock, interval=DAY, project_interval=DAY)
    restarted.tick()
    assert len(remote.reads) == len(remote.writes) == 1
    stamp += timedelta(seconds=15)
    remote.failure = False
    assert restarted.tick()[request["issue_url"]]["mirror"]["state"] == "consistent"
    assert len(remote.reads) == 1
    stamp += timedelta(seconds=86384)
    restarted.tick()
    assert len(remote.reads) == 1
    stamp += timedelta(seconds=1)
    restarted.tick()
    assert len(remote.reads) == 2


@pytest.mark.parametrize("status", ["blocked", "in-review"])
@pytest.mark.parametrize("mapped", [False, True])
def test_local_tracking_releases_claim_without_any_remote_call(
    service, monkeypatch, status, mapped,
):
    store, request = service
    if mapped:
        store.config.raw["repositories"]["fixture"]["project_statuses"] = {
            status: "Validating",
        }
        store.config.path.write_text(json.dumps(store.config.raw))
    store.submit(request)
    spec = store.spec(request["run_id"])
    monkeypatch.setattr(delivery_activities, "_context", lambda *_, **_options: (store, None))
    monkeypatch.setattr(delivery_activities.subprocess, "run",
                        lambda *_args, **_kwargs: pytest.fail("unexpected remote call"))
    receipt = delivery_activities._tracker_sync(spec, status, release=True, terminal=True)
    assert receipt["state"] == "recorded"
    assert receipt["scope"] == "local"
    assert _tracking_ready(spec, receipt)
    assert not _tracking_ready({}, receipt)
    with store._connect() as db:
        assert store.state.claim_for(db, request["work_id"]) is None
        assert db.execute("SELECT count(*) FROM reconcile_intents").fetchone()[0] == 0
    assert record_tracking(store, spec, status, True)["claim_released"]


def test_legacy_active_workflow_keeps_project_ownership_until_terminal(service):
    store, request = service
    store.submit(request)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        spec = json.loads(db.execute("SELECT request_json FROM delivery_runs").fetchone()[0])
        spec.pop("project_sync_version")
        db.execute("UPDATE delivery_runs SET request_json=?", (json.dumps(spec),))
        transition(db, store.config, request["run_id"])
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    assert sync.tick()[request["issue_url"]]["mirror"]["state"] == "pending"
    assert not remote.writes
    store.project(request["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message="Stopped", outcome="blocked")
    assert sync.tick()[request["issue_url"]]["mirror"]["state"] == "consistent"
    assert remote.writes == ["Blocked"]


def test_older_run_changes_cannot_steal_feature(service):
    store, request, _ = published(service)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        store.state.release_work(db, request["work_id"], "external:devflow:run-1")
    second = {**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"}
    store.submit(second)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE delivery_runs SET phase='blocked',revision=revision+1 "
                   "WHERE run_id='run-1'")
        transition(db, store.config, "run-1")
        assert current(db, request["issue_url"])["run_id"] == "run-2"


def test_stale_mirror_is_not_acknowledged(service):
    store, request, _ = published(service)
    remote = Remote()

    def changed(feature, fence):
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked'")
            transition(db, store.config, request["run_id"])
        with pytest.raises(Superseded):
            fence()
        return {"status": feature["status"]}

    remote.mirror = changed
    assert ProjectSynchronizer([store], remote).tick() == {}
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_project_outbox").fetchone()[0] == "pending"


def test_duplicate_poll_does_not_add_events_or_remote_reads(service):
    store, _, _ = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    sync.tick()
    with store._connect() as db:
        before = db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0]
    sync.tick()
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0] == before
    assert len(remote.reads) == len(remote.writes) == 1


def test_unknown_pr_read_retains_last_fact_and_error(service):
    store, request, _ = published(service)
    remote = Remote()
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    first = sync.tick()[request["issue_url"]]
    stamp += timedelta(days=1)

    def unavailable(_url):
        raise RuntimeError("PR read unavailable")

    remote.pull_request = unavailable
    after = sync.tick()[request["issue_url"]]
    assert after["status"] == first["status"]
    assert after["pull_requests"][0]["observation"] == first["pull_requests"][0]["observation"]
    assert after["pull_requests"][0]["error"] == "PR read unavailable"


def test_stack_requires_every_member_to_merge(service):
    store, request, receipt = published(service)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        receipt = {"pull_requests": [receipt, {**receipt, "url": receipt["url"][:-1] + "8"}]}
        db.execute("UPDATE delivery_runs SET pr_json=?", (json.dumps(receipt),))
        transition(db, store.config, request["run_id"])
    remote = Remote()
    read = remote.pull_request
    remote.pull_request = lambda url: {
        **read(url), "state": "MERGED" if url.endswith("7") else "OPEN",
    }
    assert ProjectSynchronizer([store], remote).tick()[request["issue_url"]]["status"] != "Done"


def test_multiple_databases_select_one_owner_and_mirror_same_view(service, tmp_path):
    from devflow_temporal.delivery_config import DeliveryConfig

    older, request, _ = published(service)
    raw = {**older.config.raw, "tracking_db": str(tmp_path / "second.sqlite3"),
           "state_root": str(tmp_path / "second-state")}
    config_path = tmp_path / "second.json"
    config_path.write_text(json.dumps(raw))
    newer = DeliveryStore(DeliveryConfig.load(config_path))
    newer.submit({**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"})
    remote = Remote()
    remote.state = "MERGED"  # The older PR cannot complete the newer feature attempt.
    sync = ProjectSynchronizer([older, newer], remote)
    result = sync.tick()[request["issue_url"]]
    assert result["status"] == "Queued" and result["run_id"] == "run-2"
    for store in (older, newer):
        with store._connect() as db:
            assert current(db, request["issue_url"])["run_id"] == "run-2"
    assert remote.writes == ["Queued"]
    with sync.ownership(), pytest.raises(BlockingIOError), sync.ownership():
        pytest.fail("overlapping source consumer acquired ownership")


def test_project_drift_is_rechecked_at_twenty_four_hours(service):
    store, _, _ = published(service)
    remote = Remote()
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    sync.tick()
    stamp += timedelta(seconds=86399)
    sync.tick()
    assert len(remote.writes) == 1
    stamp += timedelta(seconds=1)
    sync.tick()
    assert len(remote.writes) == 2


@pytest.mark.parametrize("assignee", ["owner", "@me"])
@pytest.mark.parametrize("already_assigned", [True, False])
@pytest.mark.parametrize("existing_status", ["Done", "dOnE"])
def test_github_preserves_option_ids_paginates_and_reads_back_exact_status(
    assignee, already_assigned, existing_status,
):
    from devflow_temporal.delivery_project_sync import GitHub

    class FakeGraph(GitHub):
        def __init__(self):
            self.options = [{"id": "existing", "name": "Existing", "color": "BLUE",
                             "description": "keep"}]
            if existing_status:
                self.options.append({"id": "merged", "name": existing_status,
                                     "color": "GREEN", "description": "keep"})
            self.status = {"optionId": "existing", "name": "Existing"}
            self.cursors = []
            self.changes = []
            self.assigned = already_assigned
            self.assignment_writes = 0

        def command(self, *args, **_kwargs):
            if args[:2] == ("api", "user"):
                assert args == ("api", "user", "--hostname", "github.com")
                return {"login": "owner"}
            assert args[:2] == ("issue", "view")
            return {"id": "issue", "url": "https://github.com/example/fixture/issues/3",
                    "state": "OPEN", "assignees": [{"login": "owner"}] if self.assigned else []}

        def graphql(self, host, query, **variables):
            assert host == "github.com"
            if "user(login:$login)" in query:
                assert variables["login"] == "owner"
                return {"user": {"id": "owner-id"}}
            if "addAssigneesToAssignable" in query:
                assert "assignable{... on Issue{id}}" in query
                assert variables == {"id": "issue", "assignees": ["owner-id"]}
                self.assigned = True
                self.assignment_writes += 1
                return {"addAssigneesToAssignable": {"assignable": {"id": "issue"}}}
            if "projectV2(number:" in query:
                return {"user": {"projectV2": {"id": "project", "closed": False,
                        "field": {"id": "field", "options": self.options}}}}
            if "updateProjectV2Field(" in query:
                pytest.fail("item mirroring must not change Project schema")
            if "projectItems(first:" in query:
                self.cursors.append(variables["cursor"])
                first = variables["cursor"] is None
                return {"node": {"projectItems": {
                    "nodes": [] if first else [{"id": "item", "project": {"id": "project"}}],
                    "pageInfo": {"hasNextPage": first, "endCursor": "next"},
                }}}
            if "updateProjectV2ItemFieldValue(" in query:
                self.changes.append(variables)
                self.status = {"name": existing_status or "Done", "optionId": "merged"}
                return {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "item"}}}
            if "fieldValueByName" in query:
                return {"node": {"project": {"id": "project"}, "fieldValueByName": self.status}}
            pytest.fail(query)

    remote = FakeGraph()
    feature = {"issue": "https://github.com/example/fixture/issues/3", "status": "Done",
               "binding": {"project": "https://github.com/users/example/projects/1",
                           "assignee": assignee}}
    result = remote.mirror(feature, lambda: None)
    assert result["status"] == "Done"
    assert remote.cursors == [None, "next"]
    assert len(remote.changes) == 1
    remote.mirror(feature, lambda: None)
    assert len(remote.changes) == 1  # Lost acknowledgement is safe to replay.
    assert remote.assignment_writes == (0 if already_assigned else 1)
    assert result["assignee"] == "owner"


@pytest.mark.parametrize("assignee", ["owner", "@me"])
@pytest.mark.parametrize("observed_name", ["Awaiting merge", "aWaItInG MeRgE"])
def test_audit_compares_canonical_status_and_live_pr_not_historical_mapping(
    service, monkeypatch, assignee, observed_name,
):
    import importlib.util
    import sys

    store, request, _ = published(service)
    remote = Remote()
    receipt = {"project": "https://github.com/users/example/projects/1", "project_id": "p",
               "item_id": "i", "field_id": "f", "option_id": "o", "status": "Awaiting merge",
               "assignee": "owner"}
    remote.mirror = lambda *_: receipt
    with store._connect() as db:
        saved = json.loads(db.execute("SELECT payload_json FROM delivery_features").fetchone()[0])
        saved["binding"] = {"project": receipt["project"], "assignee": assignee}
        db.execute("UPDATE delivery_features SET payload_json=?", (json.dumps(saved),))
    # Keep the binding frozen across the PR-observation transition.
    with store._connect() as db:
        spec = json.loads(db.execute("SELECT request_json FROM delivery_runs").fetchone()[0])
        spec["project_binding"] = saved["binding"]
        db.execute("UPDATE delivery_runs SET request_json=?", (json.dumps(spec),))
    ProjectSynchronizer([store], remote).tick()
    monkeypatch.setitem(sys.modules, "state", store.state)
    loader = importlib.util.spec_from_file_location("feature_github",
                                                   store.config.helpers_dir / "github.py")
    helper = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(helper)
    monkeypatch.setattr(helper, "view", lambda _: {"state": "OPEN",
                                                    "assignees": [{"login": "owner"}]})
    monkeypatch.setattr(helper, "project_item", lambda *_: {
        "project": {"id": "p"}, "fieldValueByName": {"optionId": "o", "name": observed_name},
    })
    monkeypatch.setattr(helper, "gh", lambda *args, **_kw: {"login": "owner"}
                        if args[:2] == ("api", "user") else {
        "url": "https://github.com/example/fixture/pull/7", "state": "MERGED",
        "headRefOid": "a" * 40,
    })
    with store._connect() as db:
        result = helper.audit(db, request["work_id"])
    assert result["local_status"] == "Awaiting merge"
    assert result["state"] == "reconciliation_required"
    assert result["reconciliation_required"] == ["pr_state_mismatch"]


def test_legacy_reconciler_cannot_apply_delivery_projection(service, monkeypatch):
    import importlib.util
    import sys

    store, request, _ = published(service)
    monkeypatch.syspath_prepend(str(store.config.helpers_dir))
    monkeypatch.setitem(sys.modules, "state", store.state)
    loaders = {}
    for name in ("github", "reconcile"):
        loader = importlib.util.spec_from_file_location(
            name, store.config.helpers_dir / f"{name}.py",
        )
        module = importlib.util.module_from_spec(loader)
        monkeypatch.setitem(sys.modules, name, module)
        loader.loader.exec_module(module)
        loaders[name] = module
    with store._connect() as db:
        assert not loaders["reconcile"].current(db, {"work_id": request["work_id"]})


def test_stale_worker_cannot_leave_a_forever_consistent_badge():
    from devflow_temporal.delivery_features import mirror_freshness

    feature = {"mirror": {"state": "consistent", "next_attempt_at": "2000-01-01T00:00:00+00:00"}}
    assert mirror_freshness(feature)["mirror"]["state"] == "stale"


def test_service_install_stages_owned_plist_without_starting_a_process(tmp_path, monkeypatch):
    import importlib.util
    import plistlib
    from pathlib import Path
    from types import SimpleNamespace

    script = Path(__file__).resolve().parents[2] / "scripts/project-sync-service.py"
    loader = importlib.util.spec_from_file_location("project_sync_service", script)
    service_module = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(service_module)
    owner = tmp_path / "owner.json"
    owner.write_text('{}')
    config = tmp_path / "sync.json"
    config.write_text(json.dumps({"version": 1, "owners": [str(owner)]}))
    monkeypatch.setattr(service_module.shutil, "which", lambda _: "/usr/bin/gh")
    monkeypatch.setattr(service_module.subprocess, "check_output", lambda *_a, **_kw: "a" * 40)
    monkeypatch.setattr(service_module, "launch", lambda *_a, **_kw: pytest.fail("process control"))
    target = tmp_path / "agents" / (service_module.LABEL + ".plist")
    result = service_module.install(SimpleNamespace(config=config, no_start=True), target)
    assert not result["active"]
    plist = plistlib.loads(target.read_bytes())
    assert plist["KeepAlive"] and plist["RunAtLoad"]
    assert "--config-sha256" in plist["ProgramArguments"]
    assert not (tmp_path / "project-sync-activation.json").exists()


def load_tracking_helpers(store, monkeypatch):
    import importlib.util
    import sys

    monkeypatch.setitem(sys.modules, "state", store.state)
    modules = {}
    for name in ("github", "reconcile"):
        loader = importlib.util.spec_from_file_location(
            name, store.config.helpers_dir / f"{name}.py",
        )
        module = importlib.util.module_from_spec(loader)
        monkeypatch.setitem(sys.modules, name, module)
        loader.loader.exec_module(module)
        modules[name] = module
    return modules["github"], modules["reconcile"]


def second_owner(store, tmp_path, *, copied=False):
    import sqlite3

    from devflow_temporal.delivery_config import DeliveryConfig

    target = tmp_path / "z-current.sqlite3"
    if copied:
        with store._connect() as source, sqlite3.connect(target) as destination:
            source.backup(destination)
    raw = {**store.config.raw, "tracking_db": str(target),
           "state_root": str(tmp_path / "second-state")}
    config = tmp_path / "second.json"
    config.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(config))


def test_copied_admissions_compare_versions_only_within_selected_source(
    service, tmp_path, monkeypatch,
):
    older, request = service
    older.submit(request)
    selected = second_owner(older, tmp_path, copied=True)
    older.project(request["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message="Old source stopped", outcome="blocked")
    github, _ = load_tracking_helpers(older, monkeypatch)
    result = ProjectSynchronizer([older, selected], Remote()).tick()[request["issue_url"]]
    assert result["source"] == str(selected.config.tracking_db.resolve())
    assert result["status"] == "Queued" and result["version"] >= 1
    for store in (older, selected):
        with store._connect() as db:
            assert current(db, request["issue_url"])["status"] == "Queued"
            assert github.feature_state(db, request["issue_url"])["status"] == "Queued"


def test_handoff_fences_actual_legacy_apply_before_remote_effect(service, tmp_path, monkeypatch):
    import fcntl
    import hashlib

    old, request = service
    old.submit(request)
    with old._connect() as db:
        spec = json.loads(db.execute("SELECT request_json FROM delivery_runs").fetchone()[0])
        spec.pop("project_sync_version")
        db.execute("UPDATE delivery_runs SET request_json=?", (json.dumps(spec),))
        transition(db, old.config, request["run_id"])
    new = second_owner(old, tmp_path)
    new.submit({**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"})
    github, reconcile = load_tracking_helpers(old, monkeypatch)
    monkeypatch.setattr(github, "gh", lambda *_a, **_k: pytest.fail("legacy remote effect"))
    with old._connect() as db:
        old.state.update(db, "work", {"id": request["work_id"], "details": {
            "github": {"project": "https://github.com/users/example/projects/1"},
        }}, None)
        saved = reconcile.queue(db, request["work_id"], "sync", {
            "issue": request["issue_url"], "status": "in-review", "release": False,
            "project": "https://github.com/users/example/projects/1",
            "project_status": "In review", "assignee": "owner",
        }, owner="external:devflow:run-1")
        assert reconcile.current(db, saved)
    lock_name = hashlib.sha256(request["work_id"].encode()).hexdigest()[:24]
    lock_path = old.config.tracking_db.with_name(f".reconcile-{lock_name}.lock")
    remote = Remote()
    mirror = remote.mirror

    def overlap(feature, fence):
        with lock_path.open("a") as handle, pytest.raises(BlockingIOError):
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with old._connect() as db:
            assert github.feature_state(db, request["issue_url"])["run_id"] == "run-2"
            assert reconcile.apply_one(db, saved)["state"] == "superseded"
        return mirror(feature, fence)

    remote.mirror = overlap
    sync = ProjectSynchronizer([old, new], remote)
    # A legacy write already in flight owns the lock; the projector must wait.
    with lock_path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert sync.tick() == {}
        assert not remote.writes
    assert sync.tick()[request["issue_url"]]["mirror"]["state"] == "consistent"
    assert remote.writes == ["Queued"]


@pytest.mark.parametrize("fresh", [False, True])
@pytest.mark.parametrize("drift", [False, True])
def test_terminal_tracking_retains_publication_custody_readback(monkeypatch, fresh, drift):
    import asyncio

    from devflow_temporal import delivery_terminal_recovery

    seen = []
    spec = {"provider": "codex", **({"project_sync_version": 1} if fresh else {})}
    monkeypatch.setattr(delivery_activities, "_context", lambda *_, **_options: (None, None))

    def guard(*_):
        seen.append("guard")
        if drift:
            raise ValueError("publication identity drift")

    def acknowledge(*_, **_kwargs):
        seen.append("local")
        return {"state": "recorded" if fresh else "consistent"}

    monkeypatch.setattr(delivery_terminal_recovery, "published_readback", guard)
    monkeypatch.setattr(delivery_activities, "_tracker_sync", acknowledge)
    result = asyncio.run(delivery_activities.delivery_terminal_tracker({
        "spec": spec, "status": "in-review", "release": True,
        "candidate": {"head": "a" * 40}, "pull_request": {"url": "https://github.com/o/r/pull/1"},
    }))
    assert seen == (["guard"] if drift else ["guard", "local"])
    assert result["state"] == ("pending" if drift else "recorded" if fresh else "consistent")


def test_explicit_refresh_is_durable_and_preserves_execution_and_daily_schedule(
    service, monkeypatch,
):
    store, request, receipt = published(service)
    remote = Remote()
    stamp = datetime(2026, 10, 10, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    sync.tick()
    original = store.detail(request["run_id"])
    notified = []
    monkeypatch.setattr("devflow_temporal.delivery_project_sync.notify", notified.append)
    queued = sync.request_pr_refresh([receipt["url"]])
    assert queued == {"state": "queued", "pull_requests": [receipt["url"]]}
    assert notified == [store.config.tracking_db]
    assert len(remote.reads) == 1 and len(remote.writes) == 1
    assert sync.next_delay() == 15
    remote.state = "MERGED"
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    assert restarted.tick()[request["issue_url"]]["status"] == "Done"
    detail = store.detail(request["run_id"])
    for key in ("outcome", "phase", "pull_request", "iteration"):
        assert detail[key] == original[key]
    restarted.tick()
    assert len(remote.reads) == 2
    with store._connect() as db:
        assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()
        due = db.execute("SELECT next_check_at FROM delivery_pr_observations").fetchone()[0]
        assert datetime.fromisoformat(due) == stamp + timedelta(days=1)


def test_unchanged_pr_refresh_reconciles_project_drift_before_daily_deadline(service):
    store, request, receipt = published(service)
    remote = Remote()
    remote.state = "MERGED"
    stamp = datetime(2026, 10, 10, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    project = {"status": "Done"}
    mirror = remote.mirror

    def apply(feature, fence):
        result = mirror(feature, fence)
        project["status"] = result["status"]
        return result

    remote.mirror = apply
    first = sync.tick()[request["issue_url"]]
    project["status"] = "Done"  # An issue-close automation changes the Project afterward.
    stamp += timedelta(minutes=1)
    sync.request_pr_refresh([receipt["url"]])
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    after = restarted.tick()[request["issue_url"]]
    assert after["status"] == first["status"] == project["status"] == "Done"
    assert after["version"] > first["version"]
    assert after["mirror"]["checked_at"] == stamp.isoformat()
    assert after["mirror"]["next_attempt_at"] == (stamp + timedelta(days=1)).isoformat()
    restarted.tick()
    assert len(remote.reads) == len(remote.writes) == 2


def test_same_status_event_requires_new_readback_with_durable_failure_retry(service):
    store, request, _ = published(service, active=True)
    remote = Remote()
    remote.state = "MERGED"
    stamp = datetime(2026, 10, 10, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    first = sync.tick()[request["issue_url"]]
    # The final execution event follows PR observation and issue closure.
    store.project(request["run_id"], phase="merged", execution_state="terminal",
                  event_type="delivered", message="Issue closure confirmed", outcome="delivered")
    remote.failure = True
    assert sync.next_delay() == 15
    pending = sync.tick()[request["issue_url"]]
    assert pending["status"] == first["status"] == "Done"
    assert pending["mirror"]["state"] == "pending"
    assert pending["project_receipt"] is None
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    restarted.tick()
    assert len(remote.writes) == 2
    stamp += timedelta(seconds=15)
    remote.failure = False
    after = restarted.tick()[request["issue_url"]]
    assert after["mirror"]["state"] == "consistent"
    assert after["version"] > first["version"]
    assert len(remote.reads) == 1 and len(remote.writes) == 3


def test_same_status_revision_during_mirror_cannot_acknowledge_old_event(service):
    store, request, _ = published(service, active=True)
    remote = Remote()
    remote.state = "MERGED"
    mirror = remote.mirror

    def superseded(feature, fence):
        result = mirror(feature, fence)
        store.project(request["run_id"], phase="merged", execution_state="terminal",
                      event_type="delivered", message="Issue closure confirmed",
                      outcome="delivered")
        return result

    remote.mirror = superseded
    sync = ProjectSynchronizer([store], remote)
    assert sync.tick() == {}
    remote.mirror = mirror
    result = sync.tick()[request["issue_url"]]
    assert result["mirror"]["state"] == "consistent"
    assert remote.writes == ["Done", "Done"]


def test_refresh_rejects_unbound_urls_before_queueing_any_request(service):
    store, _, receipt = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    with pytest.raises(ValueError, match="bound"):
        sync.request_pr_refresh([receipt["url"], "https://github.com/another/repo/pull/99"])
    with store._connect() as db:
        assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()
    assert not remote.reads and not remote.writes


def test_request_arriving_during_remote_read_is_not_lost(service):
    store, _, receipt = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    sync.tick()
    sync.request_pr_refresh([receipt["url"]])
    read = remote.pull_request

    def another_request(url):
        sync.request_pr_refresh([url])
        return read(url)

    remote.pull_request = another_request
    sync.tick()
    with store._connect() as db:
        assert db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchone()
    remote.pull_request = read
    sync.tick()
    assert len(remote.reads) == 3
    with store._connect() as db:
        assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()


def test_refresh_cli_does_not_compete_for_active_consumer_lock(
    service, tmp_path, monkeypatch, capsys,
):
    from devflow_temporal.delivery_project_sync import main

    store, _, receipt = published(service)
    config = tmp_path / "synchronizer.json"
    config.write_text(json.dumps({"version": 1, "owners": [str(store.config.path)]}))
    monkeypatch.setattr("sys.argv", ["devflow-project-sync", "--config", str(config),
                                    "--refresh-pr", receipt["url"]])
    sync = ProjectSynchronizer([store], Remote())
    with sync.ownership():
        main()
    assert json.loads(capsys.readouterr().out)["state"] == "queued"


def test_shared_pr_refresh_reads_once_across_owners(service, tmp_path):
    from devflow_temporal.delivery_config import DeliveryConfig

    first, request, receipt = published(service)
    raw = {**first.config.raw, "tracking_db": str(tmp_path / "other.sqlite3"),
           "state_root": str(tmp_path / "other-state")}
    config = tmp_path / "other.json"
    config.write_text(json.dumps(raw))
    second = DeliveryStore(DeliveryConfig.load(config))
    other = {**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"}
    published((second, other))
    remote = Remote()
    sync = ProjectSynchronizer([first, second], remote)
    sync.tick()
    sync.request_pr_refresh([receipt["url"], receipt["url"]])
    remote.state = "MERGED"
    sync.tick()
    assert remote.reads == [receipt["url"], receipt["url"]]
    for store in (first, second):
        with store._connect() as db:
            assert json.loads(db.execute(
                "SELECT observation_json FROM delivery_pr_observations").fetchone()[0])[
                    "state"] == "MERGED"
            assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()


def test_explicit_failed_read_preserves_last_fact_and_discloses_failure(service):
    store, request, receipt = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    previous = sync.tick()[request["issue_url"]]
    sync.request_pr_refresh([receipt["url"]])

    def failure(_url):
        raise RuntimeError("remote unavailable")

    remote.pull_request = failure
    after = sync.tick()[request["issue_url"]]
    assert after["status"] == previous["status"]
    assert after["pull_requests"][0]["observation"] == previous["pull_requests"][0]["observation"]
    assert after["pull_requests"][0]["error"] == "remote unavailable"
    assert store.detail(request["run_id"])["outcome"] == "published_unmerged"
