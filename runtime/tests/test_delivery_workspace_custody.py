from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_feature_gates import _base_content, _base_files
from devflow_temporal.delivery_native_dependencies import frozen_pnpm_inputs
from devflow_temporal.delivery_plan_checks import _base_recipe_metadata
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workspace_custody import ensure_owner, source_for


def git(path, *args):
    return subprocess.run(
        ["git", "-C", str(path), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def owned_delivery(api_fixture):
    config_path, request = api_fixture
    config = json.loads(config_path.read_text())
    config["workspace_custody_version"] = 1
    original = Path(config["repositories"]["fixture"]["source_path"])
    (original / "scripts").mkdir()
    recipe = b"# frozen recipe metadata\n"
    (original / "scripts/checks.toml").write_bytes(recipe)
    (original / "package.json").write_text('{"packageManager":"pnpm@10.0.0"}\n')
    (original / "pnpm-lock.yaml").write_text("lockfileVersion: '9.0'\n")
    git(original, "add", ".")
    git(original, "commit", "-qm", "Fixture frozen metadata")
    setup = original.with_name("setup-chat-worktree")
    git(original, "worktree", "add", "--detach", str(setup), "HEAD")
    repository = config["repositories"]["fixture"]
    repository["source_path"] = str(setup)
    repository["expected_base_sha"] = git(setup, "rev-parse", "HEAD")
    config_path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(config_path))
    store.submit(request)
    return store, store.spec(request["run_id"]), original, setup, recipe


def test_delivery_survives_setup_worktree_and_common_git_deletion(owned_delivery):
    store, spec, original, setup, recipe = owned_delivery
    assert spec["workspace_custody_version"] == 1
    broker = DeliveryBroker(store, spec)
    prepared = broker.prepare()
    candidate = broker.candidate()
    owner = source_for(spec)
    assert owner != setup and not owner.is_relative_to(original)
    common = git(broker.checkout, "rev-parse", "--path-format=absolute", "--git-common-dir")
    assert common == str(owner)
    assert spec["source_path"] == str(setup)
    assert not (owner / "objects/info/alternates").exists()
    # No local Git helpers, credentials, hooks or arbitrary configuration escape
    # the caller-owned setup checkout into independent delivery custody.
    assert git(owner, "config", "--local", "--get", "user.name") == "Fixture"
    assert git(owner, "config", "--local", "--get", "user.email") == "fixture@example.invalid"
    shutil.rmtree(setup)
    shutil.rmtree(original)
    resumed = DeliveryBroker(store, store.spec(spec["run_id"]))
    assert resumed.prepare() == prepared
    assert resumed.candidate() == candidate
    assert _base_content(spec, "README.md") == "Fixture"
    assert "README.md" in _base_files(spec)
    assert _base_recipe_metadata(spec) == recipe
    assert frozen_pnpm_inputs(spec, resumed.checkout)[0] == "pnpm@10.0.0"
    gate = resumed.gate_checkout("review", 1, candidate)
    assert git(gate, "rev-parse", "HEAD") == candidate["head"]
    assert (gate / "README.md").read_text() == "Fixture\n"
    with store._connect() as db:
        count = db.execute("SELECT COUNT(*) FROM delivery_effects WHERE kind='prepare'")
        assert count.fetchone()[0] == 1


def test_owner_publication_serializes_concurrent_resolvers(owned_delivery):
    _, spec, _, _, _ = owned_delivery
    with ThreadPoolExecutor(max_workers=4) as pool:
        owners = list(pool.map(lambda _: ensure_owner(spec), range(8)))
    assert len(set(owners)) == 1
    assert not list(Path(spec["state_dir"]).glob(".workspace-owner-*"))
    assert source_for(spec) == owners[0]


@pytest.mark.asyncio
async def test_public_refused_admission_allows_corrected_retry(api_fixture):
    from devflow_temporal.delivery_api import create_app

    config_path, request = api_fixture
    config = json.loads(config_path.read_text())
    config["workspace_custody_version"] = 1
    config_path.write_text(json.dumps(config))
    app = create_app(config_path)
    store = app.state.delivery.store
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=config["dashboard_url"]) as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        headers = {"Origin": config["dashboard_url"], "X-Devflow-CSRF": token}
        assert (await client.post("/api/runs", json=request, headers=headers)).status_code == 200
        rejected = {**request, "command_id": "public-retry", "run_id": "run-2",
                    "branch": "feat/second"}
        refusal = await client.post("/api/runs", json=rejected, headers=headers)
        assert refusal.status_code == 409
        state = store.config.state_root / "runs" / "run-2"
        assert not (state / "workspace-owner.json").exists()
        with store._connect() as db:
            assert db.execute("SELECT 1 FROM delivery_runs WHERE run_id='run-2'").fetchone() is None
            db.execute("DELETE FROM claims WHERE work_id=?", (request["work_id"],))
        accepted = await client.post("/api/runs", json={**rejected, "work_id": "work-2"},
                                     headers=headers)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()["run_id"] == "run-2"
        assert (json.loads((state / "workspace-owner.json").read_text())["binding"]["work_id"]
                == "work-2")


def _die_during_owner_publication(store, spec, point):
    from devflow_temporal import delivery_resources, delivery_workspace_custody

    write = delivery_resources.write_private
    rename = os.rename
    def interrupted_write(path, value):
        if Path(path).name == "workspace-admission.json":
            if ((point == 1 and value["stage_identity"] is None)
                    or (point == 2 and value["phase"] == "preparing"
                        and value["stage_identity"] is not None)
                    or (point == 3 and value["phase"] == "ready")):
                os._exit(31)
        return write(path, value)

    def interrupted_rename(source, target):
        rename(source, target)
        if (point == 4 and Path(source).name == "workspace-owner.git"
                and Path(source).parent.name.startswith(".workspace-admission-")):
            os._exit(31)

    delivery_resources.write_private = interrupted_write
    delivery_workspace_custody.os.rename = interrupted_rename
    if point == 5:
        delivery_workspace_custody.subprocess.run = lambda *_a, **_k: os._exit(31)
    with delivery_workspace_custody.provisional_owner(store, spec):
        raise AssertionError("fixture did not reach its interruption point")


@pytest.mark.parametrize("point", [1, 2, 3, 4, 5])
def test_process_loss_reconciles_each_provisional_admission_phase(service, point):
    original, request = service
    config = json.loads(original.config.path.read_text())
    config["workspace_custody_version"] = 1
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    spec = store.config.admit(request)
    process = multiprocessing.get_context("fork").Process(
        target=_die_during_owner_publication, args=(store, spec, point))
    process.start()
    process.join(15)
    assert process.exitcode == 31
    state = Path(spec["state_dir"])
    if point == 5:
        from devflow_temporal.delivery_workspace_custody import WorkspaceAdmissionUnknown

        # A lost Git invocation has no authenticated complete helper closure.
        # Every retry retains one exact allocation and refuses another launch.
        allocations = list(state.glob(".workspace-admission-*"))
        assert len(allocations) == 1
        for _ in range(2):
            with pytest.raises(WorkspaceAdmissionUnknown, match="custody is unknown"):
                store.submit(request)
            assert list(state.glob(".workspace-admission-*")) == allocations
        return
    assert store.submit(request)["run_id"] == request["run_id"]
    assert not list(state.glob(".workspace-admission-*"))
    assert not (state / "workspace-admission.json").exists()
    assert source_for(store.spec(request["run_id"])) == state / "workspace-owner.git"


@pytest.mark.asyncio
async def test_public_admission_reports_unknown_git_custody(api_fixture):
    from devflow_temporal.delivery_api import create_app

    config_path, request = api_fixture
    config = json.loads(config_path.read_text())
    config["workspace_custody_version"] = 1
    config_path.write_text(json.dumps(config))
    app = create_app(config_path)
    store = app.state.delivery.store
    spec = store.config.admit(request)
    process = multiprocessing.get_context("fork").Process(
        target=_die_during_owner_publication, args=(store, spec, 5))
    process.start()
    process.join(15)
    assert process.exitcode == 31
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url=config["dashboard_url"]) as client:
        token = (await client.get("/api/session")).json()["csrf_token"]
        response = await client.post("/api/runs", json=request, headers={
            "Origin": config["dashboard_url"], "X-Devflow-CSRF": token})
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "workspace_admission_git_custody_unknown"
    assert "retain its journal and allocation" in response.json()["detail"]["message"]


def test_admission_replay_finishes_journal_after_commit(service, monkeypatch):
    from devflow_temporal import delivery_workspace_custody as custody

    original, request = service
    config = json.loads(original.config.path.read_text())
    config["workspace_custody_version"] = 1
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    reconcile = custody._reconcile_provisional

    def interrupted(state, marker, *, admitted):
        if admitted:
            raise RuntimeError("simulated loss after admission commit")
        return reconcile(state, marker, admitted=admitted)

    monkeypatch.setattr(custody, "_reconcile_provisional", interrupted)
    with pytest.raises(RuntimeError, match="after admission commit"):
        store.submit(request)
    state = store.config.state_root / "runs" / request["run_id"]
    assert (state / "workspace-admission.json").exists()
    monkeypatch.setattr(custody, "_reconcile_provisional", reconcile)
    response = store.submit(request)
    assert response["run_id"] == request["run_id"]
    assert not (state / "workspace-admission.json").exists()
    assert not list(state.glob(".workspace-admission-*"))
    assert source_for(store.spec(request["run_id"])) == state / "workspace-owner.git"


@pytest.mark.parametrize("damage", ["receipt", "owner", "alternates"])
def test_custody_damage_never_falls_back_to_original_source(owned_delivery, damage):
    _, spec, _, setup, _ = owned_delivery
    owner = ensure_owner(spec)
    if damage == "receipt":
        receipt = Path(spec["state_dir"]) / "workspace-owner.json"
        value = json.loads(receipt.read_text())
        value["binding"]["issue_url"] = "https://github.com/other/repo/issues/1"
        receipt.write_text(json.dumps(value))
    elif damage == "owner":
        shutil.rmtree(owner)
    else:
        alternate = owner / "objects/info/alternates"
        alternate.write_text(str(setup / ".git/objects") + "\n")
    with pytest.raises(ValueError, match="durable Git"):
        source_for(spec)
    assert setup.is_dir()


def native_sha_case(api_fixture):
    path, request = api_fixture
    raw = json.loads(path.read_text())
    raw["provider"] = "codex"
    source = Path(raw["repositories"]["fixture"]["source_path"])
    git(source, "branch", "-M", "main")
    sha = git(source, "rev-parse", "HEAD")
    git(source, "update-ref", "refs/remotes/origin/main", sha)
    git(source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
    raw["repositories"]["fixture"].update(
        base_ref=sha,
        checks=[{"id": "local", "argv": ["/usr/bin/true"]}],
        readiness={"version": 1, "generation": 0, "browser_engines": [],
                   "temporal_checks": []},
        prepublish_checks=[{"id": "prepublish", "argv": ["/usr/bin/true"]}],
        required_ci=["ci"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    path.write_text(json.dumps(raw))
    config = DeliveryConfig.load(path)
    original = config.admit({**request, "base_ref": sha})
    assert original["publication_base_ref"] == "main"
    return config, original, source, sha


@pytest.mark.parametrize("damage", ["head-symref", "branch-symref", "unrelated-head", "origin"])
def test_publication_branch_custody_refuses_changed_identity_or_ancestry(api_fixture, damage):
    _, original, source, _ = native_sha_case(api_fixture)
    owner = ensure_owner(original)
    if damage == "head-symref":
        git(owner, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/other")
    elif damage == "branch-symref":
        git(owner, "update-ref", "refs/remotes/origin/other", original["base_sha"])
        git(owner, "symbolic-ref", "refs/remotes/origin/main", "refs/remotes/origin/other")
    elif damage == "unrelated-head":
        git(source, "checkout", "--orphan", "unrelated-fixture")
        git(source, "add", ".")
        git(source, "commit", "-qm", "Unrelated fixture history")
        unrelated = git(source, "rev-parse", "HEAD")
        git(owner, "fetch", "--no-tags", str(source), unrelated)
        git(owner, "update-ref", "refs/remotes/origin/main", unrelated)
    else:
        git(owner, "remote", "set-url", "origin", "https://github.com/foreign/repo.git")
    with pytest.raises(ValueError, match="durable Git owner"):
        source_for(original)
