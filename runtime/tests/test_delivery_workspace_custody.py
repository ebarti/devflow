from __future__ import annotations

import json
import multiprocessing
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import httpx
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig, publication_base_ref
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


def test_read_only_broker_refuses_missing_owner_lock_without_recreating_it(owned_delivery):
    store, spec, *_ = owned_delivery
    lock = Path(spec["state_dir"]) / "workspace-owner.lock"
    lock.unlink()
    readonly = DeliveryStore(store.config, read_only=True)
    with pytest.raises(ValueError, match="observation lock is unavailable"):
        DeliveryBroker(readonly, spec)
    assert not lock.exists()


@pytest.mark.parametrize("interrupted", [False, True])
def test_refused_admission_reconciles_only_its_provisional_owner(service, monkeypatch, interrupted):
    from devflow_temporal import delivery_workspace_custody as custody

    original, request = service
    config = json.loads(original.config.path.read_text())
    config["workspace_custody_version"] = 1
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    store.submit(request)
    rejected = {**request, "command_id": "command-2", "run_id": "run-2",
                "branch": "feat/second"}
    real_reconcile = custody._reconcile_provisional
    if interrupted:
        def crash(*_args, **_kwargs):
            raise RuntimeError("simulated interruption during provisional cleanup")
        monkeypatch.setattr(custody, "_reconcile_provisional", crash)
    with pytest.raises((ValueError, RuntimeError)):
        store.submit(rejected)
    state = store.config.state_root / "runs" / "run-2"
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM delivery_runs WHERE run_id='run-2'").fetchone() is None
        db.execute("DELETE FROM claims WHERE work_id='work-1'")
    if interrupted:
        assert (state / "workspace-admission.json").is_file()
        monkeypatch.setattr(custody, "_reconcile_provisional", real_reconcile)
    else:
        assert not (state / "workspace-owner.json").exists()
    corrected = {**rejected, "work_id": "work-2"}
    assert store.submit(corrected)["run_id"] == "run-2"
    receipt = json.loads((state / "workspace-owner.json").read_text())
    assert receipt["binding"]["work_id"] == "work-2"
    assert not (state / "workspace-admission.json").exists()


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
        assert json.loads((state / "workspace-owner.json").read_text())["binding"]["work_id"] == "work-2"


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


def test_abnormal_git_exit_preserves_unknown_allocation(service, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_workspace_custody as custody

    original, request = service
    config = json.loads(original.config.path.read_text())
    config["workspace_custody_version"] = 1
    original.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(original.config.path))
    spec = store.config.admit(request)
    monkeypatch.setattr(custody.subprocess, "run", lambda *_a, **_k: SimpleNamespace(
        returncode=-9, stdout="", stderr="terminated"))
    with pytest.raises(custody.WorkspaceAdmissionUnknown, match="custody is unknown"):
        with custody.provisional_owner(store, spec):
            pytest.fail("abnormal Git exit reached admission")
    state = Path(spec["state_dir"])
    marker = json.loads((state / "workspace-admission.json").read_text())
    assert marker["phase"] == "git_active"
    assert len(list(state.glob(".workspace-admission-*"))) == 1


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


def test_unacknowledged_owner_and_credential_origin_refuse(owned_delivery):
    _, spec, _, _, _ = owned_delivery
    owner = Path(spec["state_dir"]) / "workspace-owner.git"
    # Simulate loss of the source-owner publication acknowledgement, retaining
    # its already published Git directory. No later observation may adopt it.
    (Path(spec["state_dir"]) / "workspace-owner.json").unlink()
    with pytest.raises(ValueError, match="publication is incomplete"):
        ensure_owner(spec)
    shutil.rmtree(owner)
    spec = {**spec, "origin_url": "https://token@example.invalid/repo.git"}
    with pytest.raises(ValueError, match="credential-bearing"):
        ensure_owner(spec)


def test_legacy_spec_keeps_historical_source_binding(owned_delivery):
    store, spec, _, setup, _ = owned_delivery
    raw = dict(store.config.raw)
    raw.pop("workspace_custody_version")
    store.config.path.write_text(json.dumps(raw))
    legacy_store = DeliveryStore(DeliveryConfig.load(store.config.path))
    legacy_store.submit({
        "command_id": "legacy-admission", "run_id": "legacy-run", "work_id": "legacy-work",
        "issue_url": spec["issue_url"].rsplit("/", 1)[0] + "/4",
        "branch": "feat/owned-legacy", **{key: spec[key] for key in (
            "repository_key", "goal", "base_ref", "accepted_plan", "authorized_endpoint")},
    })
    legacy = legacy_store.spec("legacy-run")
    assert source_for(legacy) == setup
    assert not (Path(legacy["state_dir"]) / "workspace-owner.json").exists()


def test_admission_owns_source_before_first_activity_or_setup_cleanup(owned_delivery):
    store, spec, original, setup, _ = owned_delivery
    assert (Path(spec["state_dir"]) / "workspace-owner.json").is_file()
    assert (Path(spec["state_dir"]) / "workspace-writers.lock").is_file()
    assert not Path(spec["checkout"]).exists()
    shutil.rmtree(setup)
    shutil.rmtree(original)
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    assert candidate["head"] == spec["base_sha"]
    assert git(broker.checkout, "rev-parse", "--git-common-dir") == str(source_for(spec))


def test_owner_does_not_copy_local_credentials_or_unrelated_refs(owned_delivery):
    _, spec, _, setup, _ = owned_delivery
    git(setup, "config", "credential.helper", "!do-not-copy-secret")
    git(setup, "config", "http.extraheader", "Authorization: private fixture")
    git(setup, "branch", "unrelated-fixture", "HEAD")
    owner = ensure_owner(spec)
    local = git(owner, "config", "--local", "--list")
    assert "credential" not in local and "Authorization" not in local
    assert not git(owner, "for-each-ref", "--format=%(refname)", "refs/heads/unrelated-fixture")
    for base, _, files in os.walk(owner / "objects"):
        for name in files:
            assert (Path(base) / name).stat().st_nlink == 1


def feature_child(spec):
    return {
        **spec,
        "run_id": "run-child",
        "work_id": "child-work",
        "branch": "feat/child",
        "base_ref": spec["base_sha"],
        "state_dir": str(Path(spec["state_dir"]).parent / "run-child"),
        "checkout": str(Path(spec["checkout"]).parent / "run-child"),
        "feature_worker": {"parent_run_id": spec["run_id"]},
    }


def test_feature_child_imports_coordinator_owner_after_setup_cleanup(owned_delivery):
    store, spec, original, setup, _ = owned_delivery
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Prerequisite source\n")
    git(broker.checkout, "add", "README.md")
    git(broker.checkout, "commit", "-qm", "Fixture prerequisite")
    child = feature_child(spec)
    child["base_sha"] = git(broker.checkout, "rev-parse", "HEAD")
    child["base_ref"] = child["base_sha"]
    shutil.rmtree(setup)
    shutil.rmtree(original)
    child_owner = ensure_owner(child)
    assert child_owner != broker.source
    assert git(child_owner, "show", child["base_sha"] + ":README.md") == "Prerequisite source"
    assert git(child_owner, "rev-parse", "refs/devflow/base") == child["base_sha"]


@pytest.mark.parametrize("damage", ["missing-parent", "changed-parent", "cross-repo", "escape"])
def test_feature_owner_requires_exact_coordinator_custody(owned_delivery, damage):
    _, spec, _, _, _ = owned_delivery
    ensure_owner(spec)
    child = feature_child(spec)
    receipt_path = Path(spec["state_dir"]) / "workspace-owner.json"
    if damage == "missing-parent":
        receipt_path.unlink()
    elif damage == "changed-parent":
        receipt = json.loads(receipt_path.read_text())
        receipt["binding"]["work_id"] = "foreign-work"
        receipt_path.write_text(json.dumps(receipt))
    elif damage == "cross-repo":
        child["github_repo"] = "foreign/repo"
    else:
        child["feature_worker"]["parent_run_id"] = "../run-1"
    with pytest.raises((ValueError, FileNotFoundError)):
        ensure_owner(child)
    assert not (Path(child["state_dir"]) / "workspace-owner.json").exists()


def continued_request(spec):
    keys = (
        "work_id",
        "issue_url",
        "repository_key",
        "goal",
        "accepted_plan",
        "base_ref",
        "authorized_endpoint",
    )
    return {
        **{key: spec[key] for key in keys},
        "command_id": "owned-continuation-command",
        "run_id": "run-owned-continuation",
        "branch": "fix/owned-continuation",
        "supersedes_run_id": spec["run_id"],
    }


def test_authenticated_new_admission_uses_predecessor_owner_after_setup_cleanup(owned_delivery):
    store, spec, original, setup, _ = owned_delivery
    old_owner = ensure_owner(spec)
    shutil.rmtree(setup)
    shutil.rmtree(original)
    successor = store.config.admit(continued_request(spec), _workspace_base=spec)
    assert successor["source_path"] == spec["source_path"]
    assert not Path(successor["source_path"]).exists()
    owner = ensure_owner(successor)
    assert owner != old_owner
    assert git(owner, "rev-parse", "refs/devflow/base") == spec["base_sha"]
    assert not (owner / "objects/info/alternates").exists()
    with pytest.raises(ValueError, match="authenticated predecessor"):
        store.config.admit(
            {**continued_request(spec), "work_id": "foreign-work"}, _workspace_base=spec
        )


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


def test_native_sha_continuation_retains_publication_branch_after_source_cleanup(api_fixture):
    config, original, source, sha = native_sha_case(api_fixture)
    owner = ensure_owner(original)
    shutil.rmtree(source)
    assert publication_base_ref(owner, sha, sha) == "main"
    successor = config.admit(continued_request(original), _workspace_base=original)
    assert successor["base_sha"] == original["base_sha"]
    assert successor["base_ref"] == sha
    assert successor["publication_base_ref"] == "main"
    assert successor["source_path"] == original["source_path"]
    next_owner = ensure_owner(successor)
    assert publication_base_ref(next_owner, sha, sha) == "main"
    assert git(next_owner, "symbolic-ref", "refs/remotes/origin/HEAD") == "refs/remotes/origin/main"
    assert git(next_owner, "for-each-ref", "--format=%(refname)", "refs/remotes/").splitlines() == [
        "refs/remotes/origin/HEAD", "refs/remotes/origin/main",
    ]


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


def test_publication_alias_can_advance_only_without_replacing_frozen_base(api_fixture):
    _, original, source, sha = native_sha_case(api_fixture)
    owner = ensure_owner(original)
    (source / "README.md").write_text("Next main fixture\n")
    git(source, "add", "README.md")
    git(source, "commit", "-qm", "Advance admitted main branch")
    descendant = git(source, "rev-parse", "HEAD")
    git(owner, "fetch", "--no-tags", str(source), descendant + ":refs/remotes/origin/main")
    assert source_for(original) == owner
    assert git(owner, "rev-parse", "refs/devflow/base") == sha
    assert publication_base_ref(owner, descendant, descendant) == "main"


def test_legacy_predecessor_fallback_requires_immutable_original_admission(api_fixture):
    path, request = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    original = store.spec(request["run_id"])
    assert "workspace_custody_version" not in original
    raw = json.loads(path.read_text())
    raw["workspace_custody_version"] = 1
    path.write_text(json.dumps(raw))
    upgraded = DeliveryStore(DeliveryConfig.load(path))
    successor = upgraded.config.admit(continued_request(original), _workspace_base=original)
    owner = ensure_owner(successor)
    assert git(owner, "rev-parse", "refs/devflow/base") == original["base_sha"]
    with upgraded._connect() as db:
        assert (
            json.loads(
                db.execute(
                    "SELECT request_json FROM delivery_runs WHERE run_id=?", (request["run_id"],)
                ).fetchone()[0]
            )
            == original
        )


@pytest.mark.parametrize("location", ["source-state", "source-database", "linked-worktree"])
def test_new_custody_admission_refuses_disposable_authority_locations(api_fixture, location):
    path, request = api_fixture
    raw = json.loads(path.read_text())
    raw["workspace_custody_version"] = 1
    source = Path(raw["repositories"]["fixture"]["source_path"])
    if location == "source-state":
        destination = source / "looks-durable-state"
        raw["state_root"] = str(destination)
    elif location == "source-database":
        destination = source / "looks-durable.sqlite3"
        raw["tracking_db"] = str(destination)
    else:
        worktree = source.parent / "managed-controller-worktree"
        git(source, "worktree", "add", "--detach", str(worktree), "HEAD")
        destination = worktree / "looks-durable-state"
        raw["state_root"] = str(destination)
    path.write_text(json.dumps(raw))
    config = DeliveryConfig.load(path)
    with pytest.raises(ValueError, match="disjoint from source|disposable linked worktree"):
        config.admit(request)
    assert not destination.exists()
