"""Actual stopped authority and native Node-wrapper assembly with owned SDK probes."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_delivery_blocked_scope as scoped
import test_delivery_browser_mode_recovery as browser
from test_delivery_amendment_capacity import assert_owner_pair, cut_operational_capacity, frozen_six
from test_delivery_blocked_scope import scope_case as scope_case
from test_delivery_browser_mode_recovery import browser_case, restore_native_observers
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal import delivery_temporal_prerequisites as tools
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_gate_retry import prepare_runtime
from devflow_temporal.delivery_resources import (
    RunResources,
    read_private,
    write_private,
)
from devflow_temporal.delivery_sandbox import prepare_native_check


def fixture_inputs():
    from portable_temporal_fixture import fixture_root

    stage = fixture_root()
    provenance = stage / "temporal-prerequisites.provenance.v1.json"
    return stage, provenance, hashlib.sha256(provenance.read_bytes()).hexdigest()


WRAPPER = (
    "import path from 'node:path';import fs from 'node:fs';import {spawnSync} from 'n"
    "ode:child_process';const value=process.env.PYTEST_ADDOPTS||'';if(!value.startsWi"
    "th('--basetemp='))throw Error('controller artifact path missing');const root=val"
    "ue.slice(11).replace(/^'|'$/g,'');fs.mkdirSync(root,{recursive:true});const cand"
    "idate=process.cwd();const env={...process.env,PYTHONPATH:path.join(candidate,'wo"
    'rkers/automation/src\')};const argv=["-m", "pytest", "tests", "-q", "-n", "2", "-'
    '-dist=loadfile", "--max-worker-restart=0", "-m", "not system_browser"];argv.push'
    '("--junitxml="+path.join(root,\'junit.xml\'));const child=spawnSync("./.venv/bin/p'
    'ython",argv,{stdio:\'inherit\',cwd:path.resolve("workers/automation"),env});if(chi'
    "ld.error)throw child.error;process.exit(child.status??1);"
)
CHECK = {
    "id": "worker-regressions",
    "kind": "test",
    "argv": ["node", "--input-type=module", "-e", WRAPPER],
    "cwd": ".",
    "timeout_seconds": 2700,
    "min_tests": 1000,
    "test_count_regex": r"(\d+) passed",
    "junit_required": True,
}
DEPENDENCY = {
    "id": "owned-python-layout",
    "argv": ["/usr/bin/true"],
    "generated_directories": ["workers/automation/.venv"],
}

PROBE = """import asyncio,json,os,shlex,uuid
from datetime import timedelta
from pathlib import Path
import pytest
from temporalio import activity,workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporal_env import time_skipping_env,local_env

@activity.defn
async def echo(value: str)->str:
    return value

@workflow.defn
class Timer:
    @workflow.run
    async def run(self):
        before=workflow.now()
        await asyncio.sleep(6*60*60)
        return (workflow.now()-before).total_seconds()

@workflow.defn
class Echo:
    @workflow.run
    async def run(self):
        return await workflow.execute_activity(echo,'owned',
                                               start_to_close_timeout=timedelta(seconds=10))

@workflow.defn
class Durable:
    complete=False
    @workflow.signal
    async def finish(self): self.complete=True
    @workflow.run
    async def run(self):
        await workflow.wait_condition(lambda:self.complete)
        return 'same-run'

@pytest.fixture(autouse=True)
def observe_existing_paths(monkeypatch):
    base=Path(shlex.split(os.environ['PYTEST_ADDOPTS'])[0].removeprefix('--basetemp='))
    report=base.parent/('sdk-paths-'+str(os.getpid())+'-'+uuid.uuid4().hex+'.json')
    records=[]
    for name,key,variable in [('start_time_skipping','test_server_existing_path',
                              'JOBCTRL_TEMPORAL_TEST_SERVER_PATH'),
                               ('start_local','dev_server_existing_path','JOBCTRL_TEMPORAL_CLI_PATH')]:
        original=getattr(WorkflowEnvironment,name)
        async def observe(_original=original,_key=key,_var=variable,**kwargs):
            assert kwargs.get(_key)==os.environ[_var]
            assert Path(kwargs[_key]).is_file()
            records.append({'key':_key,'path':kwargs[_key], 'sdk_temp':os.environ['TMPDIR']})
            report.write_text(json.dumps(records))
            return await _original(**kwargs)
        monkeypatch.setattr(WorkflowEnvironment,name,staticmethod(observe))
    yield
    assert not list(Path(os.environ['TMPDIR']).glob('temporal-test-server-sdk-*'))

@pytest.mark.asyncio
async def test_real_six_hour_time_skipping():
    async with time_skipping_env() as env:
        queue='owned-timer-'+uuid.uuid4().hex
        async with Worker(env.client,task_queue=queue,workflows=[Timer]):
            assert await env.client.execute_workflow(Timer.run,id=queue,task_queue=queue)>=21600

@pytest.mark.asyncio
async def test_real_local_activity():
    async with local_env() as env:
        queue='owned-echo-'+uuid.uuid4().hex
        async with Worker(env.client,task_queue=queue,workflows=[Echo],activities=[echo]):
            assert await env.client.execute_workflow(Echo.run,id=queue,task_queue=queue)=='owned'

@pytest.mark.asyncio
async def test_real_same_workflow_after_worker_restart():
    async with local_env() as env:
        queue='owned-durable-'+uuid.uuid4().hex
        async with Worker(env.client,task_queue=queue,workflows=[Durable]):
            handle=await env.client.start_workflow(Durable.run,id=queue,task_queue=queue)
            assert (await handle.describe()).status==WorkflowExecutionStatus.RUNNING
            run=handle.first_execution_run_id
        assert (await handle.describe()).status==WorkflowExecutionStatus.RUNNING
        async with Worker(env.client,task_queue=queue,workflows=[Durable]):
            await handle.signal(Durable.finish)
            assert await handle.result()=='same-run'
            assert handle.first_execution_run_id==run
"""


def temporal_case(service, monkeypatch, *, minimum=1000, amendment=None, ports=None):
    stage, provenance, provenance_sha = fixture_inputs()
    source = stage / "immutable-source/workers/automation"
    extra = {
        ".gitignore": (
            "node_modules/\nworkers/automation/.venv/\n__pycache__/\n.pytest_cache/\ndist/\n"
        ),
        "workers/automation/tests/temporal_env.py": (source / "tests/temporal_env.py").read_text(),
        "workers/automation/tests/test_owned_temporal_tools.py": PROBE,
    }
    for name in ("pyproject.toml", "uv.lock"):
        extra["workers/automation/" + name] = (source / name).read_text()
    check = {**copy.deepcopy(CHECK), "min_tests": minimum}
    create = browser_case if amendment == "browser" else scope_case
    case = create(
        service,
        monkeypatch,
        extra_sources=extra,
        checks=[DEPENDENCY, check],
        **({"ports": ports} if amendment == "browser" and ports is not None else {}),
    )
    command = (
        copy.deepcopy(case.command)
        if amendment is not None
        else {key: value for key, value in case.command.items() if key in resume.FIELDS}
    )
    path = case.store.config.state_root / "operator-inputs/temporal-prerequisites.json"
    path.parent.mkdir(mode=0o700)
    path.write_bytes(provenance.read_bytes())
    path.chmod(0o600)
    command.update(
        command_id="temporal-prerequisite-iteration4",
        **{
            tools.FIELD: {
                "check_id": check["id"],
                "python_project": "workers/automation",
                "provenance_path": str(path),
                "provenance_sha256": provenance_sha,
            }
        },
    )
    case.command, case.check, case.provenance_path = command, check, path
    monkeypatch.setattr(resume, "prepare_runtime", prepare_runtime)
    return case


@pytest.fixture
def stopped_temporal(service, monkeypatch):
    return temporal_case(service, monkeypatch)


def admitted(case):
    response = case.store.continue_repair("run-1", case.command)
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    return response, current, recovery


def native_check_ready(case, current):
    """Project the owned fixture's synthetic implementation/publication boundary."""
    broker = DeliveryBroker(case.store, current)
    case.store.project(
        "run-1",
        phase="checks",
        execution_state="running",
        event_type="fixture_native_check_ready",
        message="Owned synthetic implementation and publication boundary completed",
        candidate=broker.candidate(),
        iteration=4,
        cleanup="none",
    )
    return broker


@pytest.mark.parametrize(
    "defect",
    [
        "project",
        "check",
        "public",
        "missing",
        "hash",
        "schema",
        "partial",
        "reference",
        "sdk",
        "archive",
        "member",
        "executable",
        "metadata",
        "effect",
        "claim",
        "zero_allowance",
        "scope_mix",
    ],
)
def test_public_preflight_refuses_unsupported_or_uncertain_preparation(stopped_temporal, defect):
    case = stopped_temporal
    delta = case.command[tools.FIELD]
    if defect == "project":
        delta["python_project"] = "other/project"
    elif defect == "check":
        delta["check_id"] = "unfrozen-check"
    elif defect == "public":
        case.provenance_path.chmod(0o644)
    elif defect == "missing":
        case.provenance_path.unlink()
    elif defect == "hash":
        delta["provenance_sha256"] = "0" * 64
    elif defect in {"schema", "partial", "reference", "sdk", "archive", "member", "executable"}:
        value = read_private(case.provenance_path)
        if defect == "schema":
            value["schema"] = "foreign"
        elif defect == "partial":
            del value["tools"]["cli"]["archive"]
        elif defect == "reference":
            value["acquisition_facts"]["official_metadata"][0]["sha256"] = "0" * 64
        elif defect == "sdk":
            value["compatibility_facts"]["locked_sdk_version"] = "1.33.0"
        elif defect == "archive":
            value["tools"]["cli"]["published_digest"]["value"] = "0" * 64
        elif defect == "member":
            value["tools"]["cli"]["extraction"]["exact_member"] = "../../temporal"
        else:
            value["tools"]["cli"]["binary"]["sha256"] = "0" * 64
        write_private(case.provenance_path, value)
        delta["provenance_sha256"] = hashlib.sha256(case.provenance_path.read_bytes()).hexdigest()
    elif defect == "metadata":
        path = case.broker.checkout / "workers/automation/uv.lock"
        path.write_bytes(path.read_bytes() + b"\n")
    elif defect == "effect":
        with case.store._connect() as db:
            db.execute(
                "INSERT INTO delivery_effects (effect_key,run_id,kind,state,request_json,"
                "updated_at) VALUES ('pending','run-1','publish','pending','{}','')"
            )
    elif defect == "claim":
        with case.store._connect() as db:
            case.store.state.claim_work(db, case.spec["work_id"], "external:other", "fixture")
    elif defect == "zero_allowance":
        case.command["additional_iterations"] = 0
    else:
        case.command.update(
            added_paths=["README.md"],
            amended_config_path="/tmp/foreign",
            amended_config_sha256="0" * 64,
        )
    with pytest.raises((ValueError, OSError)):
        case.store.repair_admission_preflight("run-1", case.command)
    with case.store._connect() as db:
        assert not db.execute(
            "SELECT 1 FROM delivery_commands WHERE command_id=?", (case.command["command_id"],)
        ).fetchone()


@pytest.mark.parametrize(
    "defect", ["provenance", "journal", "source", "marker", "policy", "goal", "new_unknown"]
)
def test_actual_worker_readback_reauthenticates_provenance_and_original_custody(
    stopped_temporal, defect
):
    case = stopped_temporal
    response, current, recovery = admitted(case)
    if defect == "provenance":
        case.provenance_path.write_bytes(case.provenance_path.read_bytes() + b"\n")
    elif defect == "journal":
        path = case.folder / "native-process.json"
        path.write_bytes(path.read_bytes() + b"\n")
    elif defect == "new_unknown":
        with case.store._connect() as db:
            db.execute(
                "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,"
                "candidate_id,state,cleanup,result_json) VALUES "
                "('new-unknown','run-1','verify',4,?,'recovery_unknown','unknown','{}')",
                (case.partial["id"],),
            )
    elif defect == "source":
        path = case.broker.checkout / "workers/automation/pyproject.toml"
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        if defect == "marker":
            del current[tools.MARKER]
        elif defect == "goal":
            current["goal"] = "Unauthenticated goal replacement"
        else:
            current["policy"]["checks"][1]["timeout_seconds"] += 1
            current["policy_digest"] = digest(current["policy"])
        recovery["execution_spec"] = current
        path = Path(current["state_dir"]) / resume.namespace(recovery) / "admission.json"
        write_private(path, recovery)
        response["admission_sha256"] = digest(recovery)
        with case.store._connect() as db:
            db.execute("UPDATE delivery_runs SET recovery_json=?", (canonical_json(recovery),))
            db.execute(
                "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
                (canonical_json(response), case.command["command_id"]),
            )
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery}))


def owned_sdk_environment(broker, checkout=None):
    """Own only a tiny venv shell; use read-only already-verified SDK dependencies."""
    stage, _, _ = fixture_inputs()
    original = str(stage / "immutable-source/workers/automation/.venv/bin/python")
    assert Path(original).is_file(), "stage an owned verified SDK1.26 test dependency fixture"
    checkout = checkout or broker.checkout
    result = subprocess.check_output(
        [
            original,
            "-c",
            "import json,sys,sysconfig;print(json.dumps([sys._base_executable,"
            "sysconfig.get_paths()['purelib']]))",
        ],
        text=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    executable, dependencies = json.loads(result)
    paths = broker._register_generated(checkout, ["workers/automation/.venv"])
    environment = checkout / "workers/automation/.venv"
    subprocess.run(
        [executable, "-m", "venv", "--without-pip", str(environment)],
        check=True,
        capture_output=True,
        env={"PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    purelib = next(environment.glob("lib/python*/site-packages"))
    (purelib / "owned-verified-sdk.pth").write_text(dependencies + "\n")
    broker._record_generated(paths)
    assert (
        subprocess.check_output(
            [
                str(environment / "bin/python"),
                "-c",
                "from importlib.metadata import version;print(version('temporalio'))",
            ],
            text=True,
        ).strip()
        == "1.26.0"
    )


@pytest.mark.parametrize("kind", ["scope", "browser"])
def test_combined_prerequisites_share_one_original_finite_admission(service, monkeypatch, kind):
    case = temporal_case(frozen_six(service), monkeypatch, amendment=kind)
    cut_operational_capacity(case)
    original = copy.deepcopy(case.state)
    preflight = case.store.repair_admission_preflight("run-1", case.command)
    assert preflight["authorized_through_iteration"] == 4
    response, current, recovery = admitted(case)
    assert recovery["maximum_iteration"] == 4 and recovery["state"] == original
    assert recovery["session_id"] == case.result["session_id"]
    assert tools.MARKER in current and tools.FIELD in recovery
    assert current["policy"]["checks"] == case.spec["policy"]["checks"]
    if kind == "scope":
        assert current["policy"]["source_scope"]["allowed_files"] == case.command["added_paths"]
        assert recovery["blocked_scope_finding"]
    else:
        assert current["policy"]["browser_qa"]["env"]["JOBCTRL_E2E_ISOLATED"] == "0"
        assert recovery["browser_qa_amendment"]["failed_browser"]["cases"][0]["state"] == "skipped"
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": recovery})) == {
        "state": "confirmed"
    }
    assert_owner_pair(case, current, recovery)
    assert case.store.continue_repair("run-1", case.command) == response
    evidence = native_check_ready(case, current).evidence_dir / "checks/4"
    try:
        _profile, environment = prepare_native_check(
            current, case.broker.checkout, evidence, case.check
        )
        assert set(environment) >= {p["environment"] for p in tools.TOOLS.values()}
        process = SimpleNamespace(
            spec=current,
            folder=evidence / case.check["id"] / "native",
            environment=environment,
            argv=case.check["argv"],
            cwd=case.broker.checkout,
            timeout=2700,
        )
        tools.check_launch(process)
    finally:
        assert RunResources(current).finalize("blocked")["state"] == "confirmed"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["scope", "browser"])
async def test_composed_public_admission_actual_temporal_worker_native_sdk_and_replay(
    service, monkeypatch, kind
):
    from devflow_temporal.delivery_activities import delivery_checks

    saved, observed = [], []
    execute = DeliveryBroker._run_check_list

    def dependency_fixture(self, checkout, checks, *args, **kwargs):
        if any(check["id"] == CHECK["id"] for check in checks):
            # Dependency acquisition is fixture setup. The declared frozen
            # command and production check activity/process/evidence stay real.
            owned_sdk_environment(self, checkout)
        return execute(self, checkout, checks, *args, **kwargs)

    async def actual_checks(body):
        result = await delivery_checks(body)
        observed.append(result)
        output = Path(saved[0].spec["state_dir"]) / f"devflow-temporal-composed-{kind}-checks.json"
        output.write_text(json.dumps(result, indent=2) + "\n")
        return result

    def create(value, patch, **kwargs):
        case = temporal_case(
            frozen_six(value), patch, minimum=3, amendment=kind, ports=kwargs.get("ports")
        )
        cut_operational_capacity(case)
        restore_native_observers(patch)
        case.actual_checks, case.owner_test_timeout = actual_checks, 120
        saved.append(case)
        return case

    monkeypatch.setattr(DeliveryBroker, "_run_check_list", dependency_fixture)
    if kind == "scope":
        monkeypatch.setattr(scoped, "scope_case", create)
        await scoped.test_public_native_blocked_scope_runs_worker_same_session_and_all_normal_gates(
            service, monkeypatch
        )
    else:
        monkeypatch.setattr(browser, "browser_case", create)
        await browser.test_public_browser_amendment_real_preparation_worker_native_gate_and_replay(
            service, monkeypatch
        )
    assert len(observed) == 1 and observed[0]["state"] == "passed"
    item = next(result for result in observed[0]["results"] if result["id"] == CHECK["id"])
    assert item["junit"]["passed"] == 3 and item["passed"] is True
    assert tools.FIELD in item and item["argv"] == CHECK["argv"]
    case = saved[0]
    with case.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    assert recovery["maximum_iteration"] == 4
    assert_owner_pair(case, case.store.effective_spec("run-1"), recovery)


@pytest.mark.parametrize("changed_claim", [False, True])
def test_complete_receipt_is_inert_until_owner_seal_and_exact_replay_succeeds(
    stopped_temporal, monkeypatch, changed_claim
):
    import devflow_temporal.delivery_resources as resources

    case = stopped_temporal
    _response, current, _recovery = admitted(case)
    broker = native_check_ready(case, current)
    evidence = broker.evidence_dir / "checks/4"
    manifest_path = Path(current["state_dir"]) / "resources/manifest.json"
    original = resources.write_private

    def crash_before_owner_ack(path, value):
        if path == manifest_path and value.get("temporal_test_preparations"):
            raise RuntimeError("controlled crash after receipt fsync before owner seal")
        return original(path, value)

    monkeypatch.setattr(resources, "write_private", crash_before_owner_ack)
    try:
        with pytest.raises(RuntimeError, match="controlled crash"):
            prepare_native_check(current, broker.checkout, evidence, case.check)
        path = evidence / case.check["id"] / "temporal-prerequisites.json"
        before = path.read_bytes()
        receipt = read_private(path)
        process = SimpleNamespace(
            spec=current,
            folder=path.parent / "native",
            cwd=broker.checkout,
            argv=case.check["argv"],
            timeout=2700,
            environment={**receipt["environment"], "TMPDIR": receipt["scratch"]},
        )
        with pytest.raises(ValueError, match="acknowledged resource owner"):
            tools.check_launch(process)
        monkeypatch.setattr(resources, "write_private", original)
        if changed_claim:
            with case.store._connect() as db:
                owner = "external:devflow:" + current["run_id"]
                case.store.state.release_work(db, current["work_id"], owner)
                case.store.state.claim_work(db, current["work_id"], owner, "New owned generation")
            with pytest.raises(ValueError):
                prepare_native_check(current, broker.checkout, evidence, case.check)
            assert path.read_bytes() == before
            assert not read_private(manifest_path).get("temporal_test_preparations")
            return
        _profile, environment = prepare_native_check(current, broker.checkout, evidence, case.check)
        assert path.read_bytes() == before
        process.environment = environment
        tools.check_launch(process)
        manifest = read_private(manifest_path)
        assert len(manifest["temporal_test_preparations"]) == 1
    finally:
        monkeypatch.setattr(resources, "write_private", original)
        assert RunResources(current).finalize("blocked")["state"] == "confirmed"
