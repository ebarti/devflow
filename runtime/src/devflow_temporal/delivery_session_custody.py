"""Keep an implementation session in its admitted home across gate-only retries."""

from __future__ import annotations

import json
from pathlib import Path

from .contracts import digest
from .delivery_continuation import session_state_digest
from .delivery_resources import _ancestors, read_private


def implementation_generation(spec, previous=None):
    """Recover legacy gate bindings from sealed predecessors, never a directory search."""
    if "implementation_role_home_generation" in spec:
        return spec["implementation_role_home_generation"]
    generation = spec.get("role_home_generation", "")
    while previous:
        from .delivery_gate_retry import (
            CI_KIND,
            CONTROLLER_KIND,
            KIND,
            PRELAUNCH_KIND,
            PREPUBLICATION_KIND,
        )

        if (
            previous["kind"]
            in {KIND, PREPUBLICATION_KIND, PRELAUNCH_KIND, CI_KIND, CONTROLLER_KIND}
            and previous["execution_spec"].get("role_home_generation", "") == generation
        ):
            before = previous["seal"]["original_spec"]
            if any(before[key] != spec[key] for key in ("run_id", "state_dir")):
                raise ValueError("implementation session home belongs to a different run")
            return implementation_generation(before, previous.get("original_recovery"))
        previous = previous.get("original_recovery")
    return generation


def _missing_rollout(spec, state, attempt, session, generation):
    from .delivery_gates_admission import _native_result_bytes
    from .delivery_sandbox import _native_role_home
    from .supervisor import DeliverySupervisor

    folder = Path(spec["state_dir"]) / "attempts" / attempt["job_key"]
    raw = json.loads(_native_result_bytes(spec, attempt))
    _ancestors(folder / "request.json")
    request = read_private(folder / "request.json")
    journal = read_private(folder / "native-process.json")
    observation = read_private(folder / "native-thread-observation.json")
    launch = read_private(folder / "launch.json")
    result = json.loads(attempt["result_json"])
    outcome = result.get("native_process", {})
    metadata = journal.get("provider_session", {})
    expected = {
        "role": "implement",
        "iteration": state["iteration"],
        "session_id": None,
        "resumed_from": session,
        "output_candidate": {k: state["candidate"][k] for k in ("id", "head", "content_sha256")},
        "result_digest": digest(result),
    }
    wrong_home = _native_role_home(request)
    correct_home = _native_role_home(
        {**request, "spec": {**spec, "implementation_role_home_generation": generation}}
    )
    if (
        request["spec"] != spec
        or request["role"] != "implement"
        or request["iteration"] != state["iteration"]
        or request.get("resume_session") != session
        or request["candidate"] != state["candidate"]
        or request["result_path"] != str(folder / "result.json")
        or request["workspace"] != spec["checkout"]
        or DeliverySupervisor._job_key(request) != attempt["job_key"]
        or attempt["result_path"] != request["result_path"]
        or attempt["candidate_id"] != state["candidate"]["id"]
        or any(raw.get(key) != result.get(key) for key in raw if key != "traceback")
        or result.get("status") != "blocked"
        or result.get("finish_reason") != "exception"
        or result.get("summary") != "role process failed: InvalidRequestError"
        or result.get("findings")
        != [f"JSON-RPC error -32600: no rollout found for thread id {session}"]
        or result.get("usage") is not None
        or journal.get("phase") != "finished"
        or journal.get("monitoring_complete") is not True
        or journal.get("result") != outcome
        or metadata != expected
        or outcome.get("cleanup") != "observed-native-confirmed"
        or outcome.get("state") != "finished"
        or outcome.get("exit_code") != 1
        or outcome.get("cancelled") is not False
        or outcome.get("timed_out") is not False
        or outcome.get("stdio_drained") is not True
        or journal["intent"]["run_id"] != spec["run_id"]
        or journal["intent"]["policy_digest"] != spec["policy_digest"]
        or journal["intent"]["cwd"] != spec["checkout"]
        or journal["intent"].get("environment_sha256") != digest(launch["environment"])
        or journal["owned"].get(str(attempt["pid"]), {}).get("identity")
        != attempt["process_identity"]
        or observation.get("schema") != "devflow-native-kit-threads-v1"
        or observation.get("run_id") != spec["run_id"]
        or observation.get("role") != "implement"
        or observation.get("iteration") != state["iteration"]
        or observation.get("resumed_from") != session
        or journal["owned"].get(str(observation.get("pid")), {}).get("identity")
        != observation.get("start_identity")
        or observation.get("start_identity") is None
        or observation.get("state") != "unknown"
        or observation.get("parent_thread_id") is not None
        or observation.get("turns") != []
        or observation.get("raw_turn_items") is not None
        or observation.get("collaboration_items") != []
        or observation.get("thread_inventory_before") != []
        or observation.get("thread_inventory_after") is not None
        or observation.get("new_child_thread_ids") is not None
        or wrong_home == correct_home
        or launch["environment"].get("CODEX_HOME") != str(wrong_home / "codex")
    ):
        raise ValueError("missing session is not an authenticated failure before the provider turn")
    return {
        name: digest(value)
        for name, value in (
            ("request", request),
            ("journal", journal),
            ("observation", observation),
            ("launch", launch),
            ("result", raw),
        )
    }


def _session_origin(spec, state, attempts, session, home):
    """The sealed predecessor home must also be the home that actually ran this session."""
    from .delivery_gates_admission import _native_result_bytes
    from .delivery_sandbox import _native_role_home
    from .supervisor import DeliverySupervisor

    owner = next(
        role
        for role in reversed(state["roles"])
        if role.get("role") == "implement" and role.get("session_id") == session
    )
    matching = [
        a
        for a in attempts
        if a["role"] == "implement"
        and a["iteration"] == owner["iteration"]
        and a["session_id"] == session
    ]
    if len(matching) != 1:
        raise ValueError("original implementation session has no exact owning attempt")
    attempt = matching[0]
    folder = Path(spec["state_dir"]) / "attempts" / attempt["job_key"]
    raw = json.loads(_native_result_bytes(spec, attempt))
    _ancestors(folder / "request.json")
    request = read_private(folder / "request.json")
    journal = read_private(folder / "native-process.json")
    launch = read_private(folder / "launch.json")
    result = json.loads(attempt["result_json"])
    metadata = journal.get("provider_session", {})
    if (
        any(request["spec"][key] != spec[key] for key in ("run_id", "state_dir", "checkout"))
        or request["role"] != "implement"
        or request["iteration"] != owner["iteration"]
        or DeliverySupervisor._job_key(request) != attempt["job_key"]
        or _native_role_home(request) != home
        or launch["environment"].get("CODEX_HOME") != str(home / "codex")
        or journal["intent"].get("environment_sha256") != digest(launch["environment"])
        or any(raw.get(key) != result.get(key) for key in raw if key != "traceback")
        or result.get("session_id") != session
        or result.get("cleanup") != "confirmed"
        or metadata.get("session_id") != session
        or metadata.get("result_digest") != digest(result)
        or metadata.get("role") != "implement"
        or metadata.get("iteration") != owner["iteration"]
        or journal.get("phase") != "finished"
        or journal.get("monitoring_complete") is not True
        or journal.get("result") != result.get("native_process")
    ):
        raise ValueError("original implementation session home lost its owning attempt")
    return digest({"request": request, "journal": journal, "launch": launch, "result": raw})


def implementation_custody(spec, state, attempts, previous):
    if spec["provider"] == "fake":
        return None
    implementations = [role for role in state.get("roles", []) if role.get("role") == "implement"]
    sessions = {role["session_id"] for role in implementations if role.get("session_id")}
    if len(sessions) != 1:
        return None  # Existing resume validation rejects missing or conflicting sessions.
    session = next(iter(sessions))
    generation = implementation_generation(spec, previous)
    from .delivery_sandbox import _native_role_home

    home = _native_role_home(
        {
            "spec": {**spec, "implementation_role_home_generation": generation},
            "role": "implement",
            "iteration": state["iteration"],
        }
    )
    _ancestors(home)
    result = {
        "generation": generation,
        "session_id": session,
        "session_state_digest": session_state_digest(home, session),
    }
    latest = implementations[-1]
    if latest.get("session_id") is None and latest.get("finish_reason") == "exception":
        matching = [
            a
            for a in attempts
            if a["role"] == "implement" and a["iteration"] == latest["iteration"]
        ]
        if len(matching) != 1:
            raise ValueError("missing session requires one exact completed implementation attempt")
        result["session_origin"] = _session_origin(spec, state, attempts, session, home)
        result["missing_rollout"] = _missing_rollout(spec, state, matching[0], session, generation)
    return result
