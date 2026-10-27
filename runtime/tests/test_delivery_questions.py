"""Blocking question admission and deterministic originating-thread callbacks."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import Replayer, Worker
from test_delivery_intake import intake_fixture as source_intake_fixture

from devflow_temporal.delivery_activities import (
    delivery_accept_plan,
    delivery_intake,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_question_sender import (
    pump_blocking_questions,
)
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def intake_fixture(tmp_path):
    return source_intake_fixture.__wrapped__(tmp_path)


THREAD = "01a0c8be-e849-7ef2-ad81-78ccdb4b4275"
OTHER = "01a100ac-efd3-7dd2-9f25-504381f0dcd9"


BLOCKER = {
    "unknown": "The required external consumer format is not specified",
    "evidence_checked": ["README and tracked tests contain no consumer contract"],
    "why_no_safe_default": "A guessed format cannot establish the required consumer compatibility",
}


def _question(store, *, revision=1):
    decision = {"id": f"run-1:question:{revision}", "kind": "question", "revision": revision,
                "candidate_revision": 1, "prompt": "Which required consumer format?",
                "options": ["JSON", "CSV"], "state": "pending", "blocker": BLOCKER}
    store.project("run-1", phase="waiting_question", execution_state="waiting",
                  event_type="question_pending", message="Blocking clarification needed",
                  decision=decision, protocol_revision=revision + 3,
                  key=f"question:{revision}")
    return decision


def _queue_binary(tmp_path, *, mode="success"):
    binary = tmp_path / "queue-fixture"
    log = tmp_path / "queue-argv.jsonl"
    binary.write_text(f'''#!{sys.executable}
import json,sys,time,subprocess
from pathlib import Path
with Path({str(log)!r}).open('a') as log:
    log.write(json.dumps(sys.argv[1:])+'\\n')
mode={mode!r}
if mode=='timeout':
    child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'])
    Path({str(tmp_path / 'child.pid')!r}).write_text(str(child.pid))
    time.sleep(20)
if mode=='slow': time.sleep(.2)
if mode=='exit': sys.exit(42)
if mode=='malformed': print('queued maybe');sys.exit(0)
thread=sys.argv[3] if mode!='wrong-target' else {OTHER!r}
print('Queued message {OTHER} for thread '+thread+'.')
''')
    binary.chmod(0o700)
    return binary, log


def _store_with_queue(intake_fixture, tmp_path, *, mode="success", origin=THREAD):
    path, request = intake_fixture
    binary, log = _queue_binary(tmp_path, mode=mode)
    config = json.loads(path.read_text())
    config["codex_bin"] = str(binary)
    path.write_text(json.dumps(config))
    service = create_app(path).state.delivery
    service.store.submit({**request, **({"origin_thread_id": origin} if origin else {})})
    return service, log


def test_process_crash_after_queue_commit_stays_unknown_without_second_send(
    intake_fixture, tmp_path
):
    service, log = _store_with_queue(intake_fixture, tmp_path)
    _question(service.store)
    code = '''
import os,sys
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_question_sender import CodexQuestionQueue,pump_blocking_questions
class CrashAfterCommit(CodexQuestionQueue):
    def send(self,*args,**kwargs):
        super().send(*args,**kwargs)
        os._exit(9)
pump_blocking_questions(DeliveryStore(DeliveryConfig.load(Path(sys.argv[1]))),CrashAfterCommit())
'''
    crashed = subprocess.run([sys.executable, "-c", code, str(service.config.path)],
                             capture_output=True, text=True, timeout=10)
    assert crashed.returncode == 9, crashed.stderr
    assert service.store.question_notifications("run-1")[0]["state"] == "dispatching"
    pump_blocking_questions(service.store)
    assert service.store.question_notifications("run-1")[0]["state"] == "unknown"
    assert len(log.read_text().splitlines()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "justified,racing",
    [pytest.param(True, False, id="blocking"), pytest.param(True, True, id="receiver-race")],
)
async def test_public_blocking_question_callback_and_actual_user_answer_resume_temporal(
    intake_fixture, tmp_path, justified, racing
):
    path, request = intake_fixture
    binary, log = _queue_binary(tmp_path)
    config = json.loads(path.read_text())
    config["codex_bin"] = str(binary)
    question = config["fake_intake"][0]
    if not justified:
        question["questions"][0].pop("blocker")
    config["fake_intake"] = [question, *([config["fake_intake"][1]] if racing else []),
                             config["fake_intake"][2]]
    path.write_text(json.dumps(config))
    app = create_app(path)
    service = app.state.delivery
    implemented = []

    @activity.defn(name="delivery_tracker_start")
    async def tracker(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def implement(payload):
        implemented.append(payload["spec"])
        return {"status": "blocked", "candidate": payload["candidate"]}

    activities = [delivery_project, delivery_prepare, delivery_intake, delivery_accept_plan,
                  tracker, implement]
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "blocking-temporal.sqlite3"),
    ) as environment:
        async def client():
            return environment.client
        service.client = client
        transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
        async with httpx.AsyncClient(transport=transport,
                                    base_url=config["dashboard_url"]) as browser:
            csrf = (await browser.get("/api/session")).json()["csrf_token"]
            headers = {"Origin": config["dashboard_url"], "X-Devflow-CSRF": csrf}
            submitted = await browser.post("/api/runs", headers=headers,
                                           json={**request, "origin_thread_id": THREAD})
            assert submitted.status_code == 200, submitted.text
            queue = "blocking-fixture"
            async with Worker(environment.client, task_queue=queue,
                              workflows=[DeliveryWorkflow], activities=activities):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run, service.store.spec("run-1"),
                    id="delivery-run-1", task_queue=queue,
                )
                service.store.mark_start("run-1", accepted=True)
                if not justified:
                    result = await handle.result()
                    assert result["outcome"] == "blocked"
                    assert not implemented
                    assert service.store.question_notifications("run-1") == []
                    assert service.store.detail("run-1")["decisions"] == []
                    assert not log.exists()
                else:
                    for _ in range(200):
                        detail = (await browser.get("/api/runs/run-1")).json()["run"]
                        if detail["decisions"]:
                            break
                        await asyncio.sleep(.05)
                    assert detail["phase"] == "waiting_question" and not implemented
                    decision = detail["decisions"][0]
                    assert decision["blocker"] == question["questions"][0]["blocker"]
                    assert service.store.question_notifications("run-1")[0]["state"] == "pending"
                    await service.dispatch_questions_once()
                    assert service.store.question_notifications("run-1")[0]["state"] == "queued"
                    assert not implemented  # A callback cannot answer or grant authority.
                    presented = (detail["id"], decision["id"], decision["revision"],
                                 decision["candidate_revision"], detail["candidate"]["id"])
            if justified:
                # Restart both service sender and Temporal worker while waiting.
                restarted = DeliveryService(path)
                await restarted.dispatch_questions_once()
                assert len(log.read_text().splitlines()) == 1
                async with Worker(environment.client, task_queue=queue,
                                  workflows=[DeliveryWorkflow], activities=activities):
                    detail = (await browser.get("/api/runs/run-1")).json()["run"]
                    decision = detail["decisions"][0]
                    answer = {"command_id": "actual-user-answer",
                              "expected_revision": detail["protocol_revision"],
                              "decision_id": decision["id"],
                              "decision_revision": decision["revision"],
                              "candidate_revision": decision["candidate_revision"],
                              "answer": "User's exact required wording"}
                    if racing:
                        # Another dashboard client answers Q1 while the callback's
                        # human is still composing an answer to the presented Q1.
                        dashboard_answer = {**answer, "command_id": "dashboard-answer",
                                            "answer": "Dashboard supplied Q1 wording"}
                        response = await browser.post("/api/runs/run-1/decision", headers=headers,
                                                      json=dashboard_answer)
                        assert response.status_code == 200, response.text
                        for _ in range(200):
                            current = (await browser.get("/api/runs/run-1")).json()["run"]
                            if (current["decisions"]
                                and current["decisions"][0]["id"] != presented[1]):
                                break
                            await asyncio.sleep(.05)
                        q2 = current["decisions"][0]
                        refreshed = (current["id"], q2["id"], q2["revision"],
                                     q2["candidate_revision"], current["candidate"]["id"])
                        assert refreshed != presented and q2["prompt"] == "Which format?"
                        assert not implemented
                        # Refreshing only the protocol revision does not authorize
                        # rebinding Q1's old answer to Q2. Frozen IDs fail closed.
                        delayed = {**answer, "command_id": "delayed-q1-answer",
                                   "expected_revision": current["protocol_revision"]}
                        refused = await browser.post("/api/runs/run-1/decision", headers=headers,
                                                     json=delayed)
                        assert refused.status_code == 409, refused.text
                        unchanged = (await browser.get("/api/runs/run-1")).json()["run"]
                        assert unchanged["decisions"][0] == q2
                        assert [item["answer"] for item in unchanged["intake"]["answers"]] == [
                            dashboard_answer["answer"]
                        ]
                        # The changed identity requires a separately presented Q2
                        # and a fresh actual user's answer, never delayed Q1 text.
                        answer = {"command_id": "fresh-q2-answer",
                                  "expected_revision": unchanged["protocol_revision"],
                                  "decision_id": q2["id"], "decision_revision": q2["revision"],
                                  "candidate_revision": q2["candidate_revision"],
                                  "answer": "User's separately requested Markdown format"}
                    response = await browser.post("/api/runs/run-1/decision", headers=headers,
                                                  json=answer)
                    assert response.status_code == 200, response.text
                    repeated = await browser.post("/api/runs/run-1/decision", headers=headers,
                                                  json=answer)
                    assert repeated.json() == response.json()
                    assert (await handle.result())["outcome"] == "blocked"
                final = service.store.detail("run-1")
                assert len(implemented) == 1 and final["decisions"] == []
                assert implemented[0]["origin_thread_id"] == THREAD
                assert implemented[0]["authorized_endpoint"] == request["authorized_endpoint"]
                assert implemented[0]["policy"]["allowed_paths"] == ["README.md"]
                assert final["intake"]["answers"][-1]["answer"] == answer["answer"]
                if racing:
                    assert not any(item["answer"] == delayed["answer"]
                                   for item in final["intake"]["answers"])
                assert final["intake"]["accepted_plan"]["authorization"]["source"] == (
                    "run_authorization"
                )
                assert not any(event["type"] == "plan_pending" for event in
                               service.store.events("run-1"))
            history = await handle.fetch_history()
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
