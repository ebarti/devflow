"""Feature coordination stays live across infrastructure failures without weakening gates.

Lost worker observations are re-issued within a bound, one chunk's failure
leaves healthy siblings running, infrastructure closures resume only through the
real admission and within a durable bound, and the merge and readback loops back
off instead of stopping. Real local Temporal servers verify the command sequence
and its replay; the real store drives the admission; the asyncio protocol stubs
cover the fast paths; a history recorded before the marker replays unchanged.
"""
from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutType
from temporalio.exceptions import TimeoutError as ActivityTimeoutError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_execution import Controller, feature_service
from test_delivery_feature_integration import integration
from test_delivery_feature_recovered_closure import recovered_worker as recovered_worker
from test_delivery_feature_revision_preparation import (
    pending_preparation as pending_preparation,
)
from test_delivery_feature_revision_subject import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revision_subject import intake_fixture as intake_fixture
from test_delivery_feature_revision_subject import native_configuration as native_configuration
from test_delivery_feature_workflow_temporal import FeatureActivities
from test_delivery_github_contract import plan
from test_delivery_store import service as service

import devflow_temporal.delivery_feature_workflow as protocol
from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_feature_pass import checkpoint_key
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _interrupted_role

FIXTURES = Path(__file__).parent / "fixtures"
MARKER = "delivery-feature-liveness-v1"
TRANSIENT = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
             "planning_defect": False, "error": "workstream build failed: ActivityError",
             "failure": {"classification": "transient", "cause_type": "ActivityTimeoutError"}}
TERMINAL = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
            "planning_defect": False, "error": "workstream product repair limit exhausted",
            "failure": {"classification": "terminal"}}
REFUSAL = "blocked implementation has no authenticated launch outcome"


def _activity_error(cause, name="delivery_feature_reserve"):
    error = ActivityError("activity failed", scheduled_event_id=1, started_event_id=2,
                          identity="", activity_type=name, activity_id="1", retry_state=None)
    error.__cause__ = cause
    return error


def _lost_heartbeat():
    return ActivityTimeoutError("heartbeat", type=TimeoutType.HEARTBEAT, last_heartbeat_details=[])


def _transient_error(name, type="ConnectionError", message="connection reset"):
    return _activity_error(ApplicationError(message, type=type), name)


def _delivered(controller, chunk_id, kind):
    if kind == "chunk":
        publication = controller.record["manifest"]["publication"]
        publication["members"].append({
            "chunk_id": chunk_id, "number": len(publication["members"]) + 1,
        })
        if len(publication["members"]) > 1:
            publication["stack_id"] = 42
        controller.record["manifest"]["revision"] += 1
    return {"outcome": "delivered", "record": deepcopy(controller.record), "budget": {"used": 0}}


class LivenessController(Controller):
    def __init__(self, value, *, liveness=True):
        super().__init__(value)
        self.feature_liveness_v1 = liveness
        self.messages = []
        self.sleeps = []
        self.admissions = []

    async def _project(self, spec, event, message):
        self.messages.append((event, message))
        await super()._project(spec, event, message)

    async def _stop(self, spec, message, **kwargs):
        self.messages.append(("stopped", message))
        return await super()._stop(spec, message, **kwargs)

    async def _activity(self, name, request):
        if name == "delivery_feature_reserve":
            # The real admission is modeled as accepted; refusal tests override it.
            self.admissions.append((request["chunk_id"], request["kind"]))
            child = request["kind"] + ":" + request["chunk_id"]
            return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                    "completed": False, "resumed": True}
        return await super()._activity(name, request)


def _protocol_stubs(monkeypatch, controller, *, bounded_wait=False):
    """Protocol stubs run outside Temporal: timers resolve at once and are recorded."""
    monkeypatch.setattr(protocol.workflow, "patched",
                        lambda marker: bounded_wait and marker == "feature-worker-bounded-wait-v1")

    async def sleep(delay):
        controller.sleeps.append(delay)

    async def wait_condition(fn, *, timeout=None):
        if timeout is not None:
            controller.sleeps.append(timeout)
            return
        while not fn():
            await asyncio.sleep(0)

    monkeypatch.setattr(protocol.workflow, "sleep", sleep)
    monkeypatch.setattr(protocol.workflow, "wait_condition", wait_condition)


def _feature_spec(run_id):
    return {"run_id": run_id, "provider": "fake", "config_path": "/frozen/config",
            "authorized_endpoint": "merged", "feature_delivery": {"version": 1},
            "policy": {"max_repairs": 10}}


def _handlers(fixtures, *overrides):
    replaced = {activity._Definition.must_from_callable(f).name for f in overrides}
    return [*(f for f in fixtures.handlers()
              if activity._Definition.must_from_callable(f).name not in replaced), *overrides]


def _base_handler(fixtures, name):
    return next(f for f in fixtures.handlers()
                if activity._Definition.must_from_callable(f).name == name)


async def _scheduled(history, name):
    scheduled = []
    for event in history.events:
        if not event.HasField("activity_task_scheduled_event_attributes"):
            continue
        attributes = event.activity_task_scheduled_event_attributes
        if attributes.activity_type.name == name:
            request = (await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads))[0]
            scheduled.append((request, attributes))
    return scheduled


def _timers(history):
    return [e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
            for e in history.events if e.HasField("timer_started_event_attributes")]


# --- H-6a: the coordinator's worker wait survives lost attempts -------------------------------








@pytest.mark.asyncio
async def test_real_temporal_permanent_github_refusal_stops_without_an_observation_loop():
    fixtures = FeatureActivities()

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        child = payload["kind"] + ":" + payload["chunk_id"]
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk"}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        # GitHubDelivery.api raises GitHubContractError for every gh failure.
        raise ApplicationError("gh: HTTP 401: Bad credentials", type="GitHubContractError")

    spec = _feature_spec("github-refusal")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert result["outcome"] == "blocked"
    assert ("feature build worker for chunk model delivery_feature_finish_worker failed: "
            "GitHubContractError: gh: HTTP 401: Bad credentials") in result["error"]
    assert "feature_observation_retries" not in result["checks"]
    assert "feature_worker_recoveries" not in result["checks"]
    events = [p["event_type"] for n, p in fixtures.calls if n == "delivery_project"]
    assert "feature_worker_observation_lost" not in events
    assert len(history.events) < 120


# --- H-6b: per-chunk failure isolation and bounded infrastructure resume ----------------------




@pytest.mark.asyncio
@pytest.mark.parametrize("recovers", [True, False])
async def test_infrastructure_closures_resume_through_admission_within_a_durable_bound(
    monkeypatch, recovers,
):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind, resume_allowed, assignment))
        failures = sum(1 for item in attempts if item[:2] == (chunk_id, kind))
        if (chunk_id, kind) == ("client", "build") and (failures <= 2 or not recovers):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TRANSIENT))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    client = [(allowed, assignment) for c, k, allowed, assignment in attempts
              if (c, k) == ("client", "build")]
    assert [allowed for allowed, _ in client] == [True, True, False]
    assert client[0][1] is None, "the first attempt reserves on its own"
    assert all(assignment["resumed"] for _, assignment in client[1:]), (
        "every re-issue carries the admitted continuation instead of reserving again")
    assert controller.admissions == [("client", "build")] * 2
    assert controller.state["checks"]["feature_worker_recoveries"] == {"client:build": 2}
    recovering = [m for e, m in controller.messages if e == "feature_worker_recovering"]
    assert recovering == [
        "Resumed the build worker for client after an infrastructure failure (1 of 2)",
        "Resumed the build worker for client after an infrastructure failure (2 of 2)",
    ]
    if recovers:
        assert result["phase"] == "merged"
    else:
        assert result["outcome"] == "blocked"
        assert "client" in result["error"] and "exhausted 2 automatic resumes" in result["error"]
        assert "product repair" not in result["error"]
    assert any(name == "delivery_feature_stop" for name, _ in controller.events)








@pytest.mark.asyncio
async def test_real_temporal_infrastructure_closure_resumes_while_the_sibling_keeps_building():
    """One build closes blocked for infrastructure while its sibling is still
    running: the coordinator re-reserves it (the supported resume path), the
    sibling is never cancelled, and the feature completes."""
    fixtures = FeatureActivities()
    base_finish = _base_handler(fixtures, "delivery_feature_finish_worker")
    reserves, waits = {}, {}
    client_resumed, release_model = asyncio.Event(), asyncio.Event()

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        child = payload["kind"] + ":" + payload["chunk_id"]
        reserves[child] = reserves.get(child, 0) + 1
        if child == "build:client" and reserves[child] == 2:
            client_resumed.set()
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk",
                **({"resumed": True} if reserves[child] > 1 else {})}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        child = payload["child_id"]
        waits[child] = waits.get(child, 0) + 1
        if child == "build:client" and waits[child] == 1:
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TRANSIENT)}
        if child == "build:model":
            await asyncio.wait_for(release_model.wait(), 30)
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        if payload["child_id"] == "build:client":
            assert client_resumed.is_set(), "a resumable closure is not retired before its resume"
        return await base_finish(payload)

    spec = _feature_spec("resumed-worker")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(client_resumed.wait(), 30)
            state = await handle.query(DeliveryWorkflow.status)
            release_model.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert state["phase"] == "feature_implementation" and state["outcome"] is None
    assert result["phase"] == "merged"
    assert reserves["build:client"] == 2 and waits["build:client"] == 2
    assert result["checks"]["feature_worker_recoveries"] == {"client:build": 1}
    recovering = [p["message"] for n, p in fixtures.calls
                  if n == "delivery_project" and p["event_type"] == "feature_worker_recovering"]
    assert recovering == ["Resumed the build worker for client after an infrastructure "
                          "failure (1 of 2)"]
    settled = [p["workers"] for n, p in fixtures.calls if n == "delivery_feature_settle_workers"]
    assert settled == [{}], "every worker closed through finish_worker; none was cancelled"


@pytest.mark.asyncio
async def test_real_temporal_chunk_verdict_drains_the_running_sibling_before_stopping():
    """The first chunk's build closes with a gate verdict while its sibling is
    still running: the coordinator waits for the sibling's own closure, then
    stops naming the chunk; the sibling is finished, not cancelled."""
    fixtures = FeatureActivities()
    draining, release_client = asyncio.Event(), asyncio.Event()
    base_project = _base_handler(fixtures, "delivery_project")

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        fixtures.calls.append(("delivery_feature_reserve", deepcopy(payload)))
        child = payload["kind"] + ":" + payload["chunk_id"]
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk"}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        fixtures.calls.append(("delivery_feature_wait_worker", deepcopy(payload)))
        if payload["child_id"] == "build:model":
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TERMINAL)}
        await asyncio.wait_for(release_client.wait(), 30)
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        fixtures.calls.append(("delivery_feature_finish_worker", deepcopy(payload)))
        outcome = "blocked" if payload["child_id"] == "build:model" else "delivered"
        return {"record": deepcopy(fixtures.record), "outcome": outcome, "budget": {}}

    @activity.defn(name="delivery_project")
    async def project(payload):
        if payload["event_type"] == "feature_worker_stopped":
            draining.set()
        return await base_project(payload)

    spec = _feature_spec("drained-sibling")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker,
                                               project)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(draining.wait(), 30)
            state = await handle.query(DeliveryWorkflow.status)
            release_client.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert state["outcome"] is None and state["phase"] == "feature_implementation"
    assert result["outcome"] == "blocked"
    assert "model" in result["error"] and "FeatureWorkerStopped" in result["error"]
    finished = [p["child_id"] for n, p in fixtures.calls if n == "delivery_feature_finish_worker"]
    assert finished == ["build:model", "build:client"]
    stopped = [p["message"] for n, p in fixtures.calls
               if n == "delivery_project" and p["event_type"] == "feature_worker_stopped"]
    assert len(stopped) == 1 and "waiting for 1 running build(s)" in stopped[0]
    assert "feature_worker_recoveries" not in result["checks"], "a verdict is never recovered"
    settled = [p["workers"] for n, p in fixtures.calls if n == "delivery_feature_settle_workers"]
    assert settled == [{}]
    assert not any(n == "delivery_feature_reserve" and p["kind"] == "chunk"
                   for n, p in fixtures.calls)


# --- Real admission: what the coordinator offers, reserve() -> resume_worker() admits ----------

IMPLEMENTED = {"role": "implement", "iteration": 0, "status": "pass",
               "session_id": "modeled-implementation", "cleanup": "confirmed"}
ADMITTED = {
    # Latest implementation passed; a controller deadline at a gate stage.
    "implement_pass_then_ci_deadline": ([IMPLEMENTED], "ci_deadline", "waiting_ci"),
    # Latest implementation passed; the review role process died with teardown.
    "review_role_exception": ([IMPLEMENTED, {
        "role": "review", "iteration": 0, "status": "blocked", "session_id": None,
        "cleanup": "confirmed", "finish_reason": "exception", "usage": None,
        "summary": "role process failed: ConnectionError", "findings": ["reset"]}],
        "role_interrupted", "review"),
    # The implementation never launched: nothing unrecorded can exist.
    "implement_prelaunch": ([{
        "role": "implement", "iteration": 0, "status": "blocked", "session_id": None,
        "cleanup": "confirmed", "finish_reason": "prelaunch", "usage": None,
        "summary": "native launch refused", "findings": ["sandbox profile unavailable"]}],
        "role_prelaunch", "implement"),
}
# The implementation process itself died: its launch outcome is unauthenticated.
IMPLEMENT_DEATH = [{"role": "implement", "iteration": 0, "status": "blocked",
                    "session_id": None, "cleanup": "confirmed", "finish_reason": "exception",
                    "usage": None, "summary": "role process failed: ConnectionError",
                    "findings": ["Connection reset by peer"]}]


def _closed_chunk_worker(service, monkeypatch, *, roles, failure, error, revision=9,
                         execution="modeled-closed"):
    """A real store, registry and git checkout; only the provider receipts and the
    Temporal closure proof are modeled. Returns what the coordinator observes."""
    store, parent, child, broker, candidate, member, _ = integration(service, monkeypatch)
    value = json.loads(parent["accepted_plan"])
    record = {"manifest": {"revision": 1, "plan": value,
                           "publication": {"stack_id": None, "members": [member]},
                           "workstream_issues": {
                               stream["id"]: {"url": "https://github.com/example/fixture/issues/10"}
                               for stream in value["workstreams"]}}}
    monkeypatch.setattr(activities, "current_record", lambda *_, **__: deepcopy(record))
    monkeypatch.setattr(publications, "current_record", lambda *_, **__: deepcopy(record))
    monkeypatch.setattr(publications, "live_members", lambda *_, **__: [
        {"number": member["number"], "state": "open", "merged": False}])
    roles = [{"candidate": candidate, **role} for role in roles]
    checks = {"failure": failure}
    state = {"run_id": child["run_id"], "phase": "blocked", "outcome": "blocked",
             "execution_state": "blocked", "cleanup": "confirmed", "revision": revision,
             "iteration": 0, "candidate": candidate, "pull_request": None,
             "roles": roles, "checks": checks, "error": error}
    store.project(child["run_id"], phase="blocked", execution_state="blocked", outcome="blocked",
                  cleanup="confirmed", event_type="blocked", message=error,
                  candidate=candidate, pull_request=None, checks=checks, iteration=0,
                  protocol_revision=revision, error=error)
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        for index, role in enumerate(roles):
            receipt = {k: v for k, v in role.items() if k not in {"role", "iteration", "candidate"}}
            db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                       "state,session_id,result_json,cleanup) "
                       "VALUES (?,?,?,0,?,'finished',?,?,'confirmed')",
                       (f"modeled-{index}", child["run_id"], role["role"], candidate["id"],
                        role.get("session_id"), json.dumps(receipt)))
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": execution,
              "request_digest": child["request_digest"], "recovery_digest": None,
              "result": state}
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda self, run_id, **kwargs: deepcopy(closed))
    chunk_id = child["feature_worker"]["chunk_id"]
    registry(parent).checkpoint(
        parent["feature_delivery"]["owner"],
        checkpoint_key(child, f"assignment:{chunk_id}:chunk"),
        {"run_id": child["run_id"], "store_path": str(store.config.tracking_db),
         "chunk_id": chunk_id, "kind": "chunk"})
    return store, parent, child, candidate, state


def _commands(store, child):
    with store._connect() as db:
        recovery = db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone()[0]
        commands = [r[0] for r in db.execute(
            "SELECT command_id FROM delivery_commands WHERE run_id=? ORDER BY rowid",
            (child["run_id"],))]
    return (json.loads(recovery) if recovery else None), commands




def test_an_implementation_death_is_neither_offered_nor_admitted(service, monkeypatch):
    from devflow_temporal.delivery_workflow import _failure_classification

    error = "workstream execution needs recovery"
    roles = [{"candidate": None, **r} for r in IMPLEMENT_DEATH]
    # What the worker's own _stop now records for this closure: a verdict.
    assert _interrupted_role(roles) is None
    failure = {**_failure_classification(None, None), "stage": "implement", "reason": error}
    assert failure["classification"] == "terminal"
    store, parent, child, candidate, _ = _closed_chunk_worker(
        service, monkeypatch, roles=IMPLEMENT_DEATH, failure=failure, error=error)
    closure = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed", "error": error,
               "failure": failure, "planning_defect": False}
    assert not protocol._resumable(closure)
    # And were it ever offered, the admission refuses it with the reason the
    # coordinator surfaces in its stop.
    with pytest.raises(ValueError, match=REFUSAL):
        activities.reserve(parent, child["feature_worker"]["chunk_id"], "chunk")
    recovery, commands = _commands(store, child)
    assert recovery is None and commands == []




# --- M-24: merge and readback loops back off instead of stopping ------------------------------






# --- Replay: executions recorded before the marker keep their commands -----------------------


# --- Child side: a worker's own controller classifies its closure -----------------------------






def _registered_build_worker(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    return store, parent, child




# --- Re-issue paths keep the naming and `_resumable`; allowlist coverage ----------------------








@pytest.mark.parametrize(
    "kind, message, transient",
    [
        ("ConnectionResetError", "[Errno 54] Connection reset by peer", True),
        ("ValueError", "stopped checkpoint changed before admission", False),
    ],
)
def test_infrastructure_allowlist_matches_on_the_child_and_coordinator_sides(
    kind,
    message,
    transient,
):
    from devflow_temporal.delivery_workflow import _failure_classification

    error = _activity_error(ApplicationError(message, type=kind), "delivery_feature_finish_worker")
    assert (_failure_classification(error, None)["classification"] == "transient") is transient
    assert protocol._transient_activity_failure(error) is transient
    # A non-retryable failure of a transient type is a verdict on both sides.
    final = _activity_error(ApplicationError(message, type=kind, non_retryable=True))
    assert _failure_classification(final, None)["classification"] == "terminal"
    assert not protocol._transient_activity_failure(final)






# --- An unsettled readback has a cancel exit and a bound; the effect stays pending -----------




@pytest.mark.asyncio
async def test_real_temporal_operator_cancel_leaves_an_unsettled_readback_pending(monkeypatch):
    fixtures = FeatureActivities()
    waiting, cancel_sent = asyncio.Event(), asyncio.Event()
    settlements = []

    @activity.defn(name="delivery_feature_settle_effects")
    async def settle_effects(payload):
        settlements.append(payload["spec"]["run_id"])
        if len(settlements) == 2:
            waiting.set()
            await asyncio.wait_for(cancel_sent.wait(), 30)
        return {"state": "pending", "effect_key": "merge:stack-42",
                "reason": "RPCError: unavailable"}

    monkeypatch.setattr(protocol, "READBACK_INTERVAL", timedelta(seconds=1))
    spec = _feature_spec("unsettled-readback-cancel")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, settle_effects),
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(waiting.wait(), 60)
            state = await handle.query(DeliveryWorkflow.status)
            assert state["phase"] == "waiting_feature_readback" and state["outcome"] is None
            await handle.execute_update(DeliveryWorkflow.cancel, {
                "command_id": "stop-unsettled-readback", "expected_revision": state["revision"],
                "reason": "Operator stops the wait",
            })
            cancel_sent.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    assert result["outcome"] == "cancelled" and result["cleanup"] == "unknown"
    assert "merge:stack-42" in result["error"] and "RPCError: unavailable" in result["error"]
    assert result["checks"]["feature_readback"]["state"] == "pending"
    assert result["checks"]["feature_stop"]["state"] == "withheld"
    assert len(settlements) == 2
    assert not any(n == "delivery_feature_stop" for n, _ in fixtures.calls)
    assert fixtures.calls[-1][0] == "delivery_project"
    assert _timers(history)[:1] == [timedelta(seconds=1)]












@pytest.mark.asyncio
async def test_transient_admission_retries_do_not_spend_a_resume(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    admissions = []
    admitted = controller._activity

    async def execute(name, request):
        if name == "delivery_feature_reserve":
            admissions.append(request["chunk_id"])
            if len(admissions) == 1:
                raise _transient_error(name, type="TimeoutExpired", message="gh timed out")
        return await admitted(name, request)

    controller._activity = execute
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind, resume_allowed))
        if (chunk_id, kind) == ("client", "build"):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TRANSIENT))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked" and "exhausted 2 automatic resumes" in result["error"]
    assert [allowed for c, k, allowed in attempts if (c, k) == ("client", "build")] == [
        True, True, False]
    assert admissions == ["client"] * 3, "one transient admission, then two admitted resumes"
    assert controller.state["checks"]["feature_worker_recoveries"] == {"client:build": 2}
    assert controller.state["checks"]["feature_worker_reissues"] == {"client:build": 1}
    assert [m for e, m in controller.messages if e == "feature_worker_recovering"] == [
        "Re-issuing the build worker admission for client after TimeoutExpired: gh timed out "
        f"(1 of {protocol.REISSUES})",
        "Resumed the build worker for client after an infrastructure failure (1 of 2)",
        "Resumed the build worker for client after an infrastructure failure (2 of 2)",
    ]
