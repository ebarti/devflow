from __future__ import annotations

import base64
import copy
import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import zipfile
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_blocked_scope import rewrite_assessment, rewrite_observation, scope_case
from test_delivery_browser_mode_recovery import restore_native_observers
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore


def wheelhouse(root):
    root.mkdir(mode=0o700)
    out = []
    for name in ("pytest", "iniconfig", "packaging", "pluggy", "pygments"):
        d = distribution(name)
        files = {}
        for f in d.files:
            path = Path(d.locate_file(f))
            raw = str(f)
            if ".." in Path(raw).parts or raw.endswith(".pyc") or raw.endswith("/RECORD"):
                continue
            if path.is_file():
                files[raw] = path.read_bytes()
        info = next(x.split("/")[0] for x in files if x.endswith(".dist-info/WHEEL"))
        tag = next(
            x.split(": ", 1)[1]
            for x in files[info + "/WHEEL"].decode().splitlines()
            if x.startswith("Tag: ")
        )
        normalized = d.metadata["Name"].replace("-", "_").lower()
        p = root / f"{normalized}-{d.version}-{tag}.whl"
        rows = []
        for f, b in files.items():
            rows.append(
                [
                    f,
                    "sha256="
                    + base64.urlsafe_b64encode(hashlib.sha256(b).digest()).decode().rstrip("="),
                    str(len(b)),
                ]
            )
        rows.append([info + "/RECORD", "", ""])
        s = io.StringIO()
        csv.writer(s).writerows(rows)
        files[info + "/RECORD"] = s.getvalue().encode()
        with zipfile.ZipFile(p, "x", zipfile.ZIP_DEFLATED) as z:
            for f, b in files.items():
                z.writestr(f, b)
        out.append(p)
    return out


@pytest.fixture
def stopped_prerequisite(service, monkeypatch, tmp_path):
    owner, request = service
    wheels = wheelhouse(tmp_path / "wheelhouse")
    project = tmp_path / "initial-project"
    project.mkdir()
    pyproject = (
        '[project]\nname="owned-frozen-gate"\nversion="0.1.0"\nrequires-python=">=3.12"\n'
        '[project.optional-dependencies]\ndev=["pytest=={version}"]\n'
        '[tool.uv]\nno-index=true\nenvironments=[\'sys_platform == "darwin"\']\n'
        'find-links=[{wheelhouse}]\n'
    ).format(
        version=distribution("pytest").version, wheelhouse=json.dumps(str(tmp_path / "wheelhouse"))
    )
    (project / "pyproject.toml").write_text(pyproject)
    subprocess.run(
        [
            shutil.which("uv"),
            "lock",
            "--offline",
            "--python",
            str(Path(sys.executable).resolve()),
            "--project",
            str(project),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
        env={k: v for k, v in os.environ.items() if k != "UV_EXCLUDE_NEWER"},
    )
    extra = {
        ".gitignore": "project/.venv/\n__pycache__/\n.pytest_cache/\n",
        "project/pyproject.toml": pyproject,
        "project/uv.lock": (project / "uv.lock").read_text(),
        "project/tests/test_owned_prerequisite.py": (
            'def test_real_owned_structural_invariant():\n'
            '    assert {"truth": 2+2} == {"truth":4}\n'
        ),
    }
    plan = canonical_json({"verification": ["Run project/tests/test_owned_prerequisite.py"]})
    case = scope_case((owner, {**request, "accepted_plan": plan}), monkeypatch, extra_sources=extra)
    # A neutral genuine completed typed native PASS, with separate input/output.
    raw = {
        **case.raw,
        "status": "pass",
        "summary": "Owned source change completed.",
        "findings": [],
    }
    rewrite_assessment(case, raw)
    obs = read_private(case.folder / "native-thread-observation.json")
    assessment = {k: raw[k] for k in ("status", "summary", "findings")}
    obs["turns"][-1]["items"][-1]["text"] = json.dumps(assessment)
    obs["turns"][-1]["turn"]["items"][-1]["text"] = json.dumps(assessment)
    rewrite_observation(case, obs)
    _git(case.broker.checkout, "add", case.api)
    _git(case.broker.checkout, "commit", "-qm", "Publish owned completed candidate")
    _git(case.broker.checkout, "push", "origin", "HEAD:refs/heads/" + case.spec["branch"])
    candidate = case.broker.candidate()
    case.publication.update(head=candidate["head"], candidate=candidate)
    case.state.update(candidate=candidate, pull_request=case.publication)
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_a, **_k: {
            "number": 12,
            "headRefOid": candidate["head"],
            "state": "OPEN",
            "isDraft": False,
            "url": case.publication["url"],
        },
    )
    restore_native_observers(monkeypatch)
    precheck = case.broker.run_prechecks(3, candidate)
    assert precheck["state"] == "passed"
    missing = next(p for p in wheels if p.name.startswith("pytest-"))
    data = missing.read_bytes()
    missing.unlink()
    local = case.broker.run_checks(3, candidate)
    assert local["state"] == "failed" and local["source_unchanged"] is True
    failed = [x for x in local["results"] if x.get("passed") is False]
    assert len(failed) == 1 and failed[0]["exit_code"] == 2
    assert failed[0]["argv"][1:4] == ["sync", "--locked", "--extra"]
    native = failed[0]["native_process"]
    assert (
        native["state"] == "finished"
        and native["monitoring_complete"] is True
        and native["exit_code"] == 2
    )
    missing.write_bytes(data)
    final = RunResources(case.spec).finalize("blocked")
    assert final["state"] == "confirmed"
    error = (
        "environment preparation failed: "
        + failed[0]["id"]
        + "; candidate retained without requesting code repair"
    )
    case.state.update(error=error, findings=[])
    case.state["checks"] = {
        "prepublish": precheck,
        "local": local,
        "resource_cleanup": final,
        "failure": {"classification": "terminal", "stage": "checks", "reason": error},
    }
    case.store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=error,
        candidate=candidate,
        pull_request=case.publication,
        checks=case.state["checks"],
        iteration=3,
        protocol_revision=40,
        error=error,
        cleanup="confirmed",
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "owned-closed-launched-prerequisite",
        "request_digest": case.spec["request_digest"],
        "recovery_digest": None,
        "result": case.state,
    }
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_a, **_k: closed)
    command = {
        "continuation_kind": "published_gate_retry",
        "command_id": "one-prerequisite-gate-recovery",
        "expected_revision": 40,
        "expected_iteration": 3,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 12,
        "expected_pr_head": candidate["head"],
        "additional_iterations": 0,
    }
    return case, local, command, closed


def test_public_published_prerequisite_recovery_keeps_failed_native_result(stopped_prerequisite):
    from devflow_temporal import delivery_gate_retry as gates

    case, local, command, closed = stopped_prerequisite
    original = copy.deepcopy(closed)
    raw = {
        Path(item["native_process"]["journal"]): Path(
            item["native_process"]["journal"]
        ).read_bytes()
        for item in local["results"]
    }
    preflight = case.store.repair_admission_preflight("run-1", command)
    assert (
        preflight["additional_iterations"] == 0 and preflight["implementation_authority"] is False
    )
    response = case.store.continue_repair("run-1", command)
    assert response["additional_iterations"] == 0 and response["implementation_authority"] is False
    assert case.store.continue_repair("run-1", command) == response
    current = case.store.effective_spec("run-1")
    with case.store._connect() as db:
        recovery = json.loads(
            db.execute("SELECT recovery_json FROM delivery_gate_admissions").fetchone()[0]
        )
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    assert current["gate_retry_stage"] == "prerequisite"
    assert current["policy"]["max_repairs"] == case.spec["policy"]["max_repairs"] == 10
    assert recovery["state"] == original["result"] and closed == original
    assert all(p.read_bytes() == b for p, b in raw.items())
    assert gates.readback(case.store, current, recovery)["number"] == 12
    assert (
        case.broker.candidate()["content_sha256"] == current_candidate(recovery)["content_sha256"]
    )
    with pytest.raises(ValueError):
        case.store.repair_admission_preflight(
            "run-1", {**command, "command_id": "another-setup-retry"}
        )


def current_candidate(recovery):
    return recovery["candidate"]
