"""Keep an implementation session in its admitted home across gate-only retries."""

from __future__ import annotations

import hashlib
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


def _same_assessment(raw, result):
    enriched = {
        "cleanup",
        "process_cleanup",
        "resource_cleanup",
        "native_process",
        "role_artifacts",
        "traceback",
    }
    return isinstance(raw, dict) and (
        {key: value for key, value in raw.items() if key != "traceback"}
        == {key: value for key, value in result.items() if key not in enriched}
    )


def completed_blocked_finding(seal):
    """Authenticate a provider's completed blocked finding without rewriting it.

    The SDK's final assessment, raw role receipt and enriched supervisor result
    must agree. Input and partial output are separate recorded checkpoints.
    """
    from .delivery_gates_admission import _native_attempt_bytes
    from .delivery_sandbox import _native_role_home
    from .supervisor import DeliverySupervisor

    spec, state, session = seal['predecessor_spec'], seal['state'], seal['session_id']
    role = next((r for r in reversed(state.get('roles', []))
                 if r.get('role') == 'implement'), {})
    attempts = [a for a in seal['attempts'] if a['role'] == 'implement'
                and a['iteration'] == role.get('iteration')]
    custody = seal.get('session_custody') or {}
    if (spec.get('provider') != 'codex' or spec.get('feature_worker')
            or role.get('status') != 'blocked' or role.get('finish_reason') != 'done'
            or role.get('iteration') != state['iteration'] or len(attempts) != 1
            or not isinstance(session, str) or not session
            or role.get('session_id') != session or 'generation' not in custody):
        raise ValueError('blocked scope finding lacks its completed native session')
    attempt = attempts[0]
    names = ('request.json', 'result.json', 'native-process.json', 'launch.json',
             'start.json', 'native-thread-observation.json')
    try:
        originals = {name: _native_attempt_bytes(spec, attempt, name) for name in names}
        request, raw, journal, launch, start, observation = (
            json.loads(originals[name]) for name in names)
        result = json.loads(attempt['result_json'])
    except (OSError, ValueError, TypeError) as exc:
        raise ValueError('blocked scope finding lacks its private native receipts') from exc
    if not all(isinstance(value, dict) for value in (
            request, raw, journal, launch, start, observation, result)):
        raise ValueError('blocked scope finding native receipts are malformed')
    folder = Path(spec['state_dir']) / 'attempts' / attempt['job_key']
    home = _native_role_home({'spec': {**spec, 'implementation_role_home_generation':
                                      custody['generation']}, 'role': 'implement',
                              'iteration': role['iteration']})
    native, metadata, reference = (result.get('native_process') or {},
                                   journal.get('provider_session') or {},
                                   raw.get('native_thread_observation') or {})
    intent, owned = journal.get('intent') or {}, journal.get('owned') or {}
    environment = launch.get('environment')
    turns = observation.get('turns')
    if (not all(isinstance(v, dict) for v in (
            native, metadata, reference, intent, owned, environment))
            or not isinstance(turns, list) or not turns
            or any(not isinstance(turn, dict) for turn in turns)):
        raise ValueError('blocked scope finding native identities are malformed')
    last = turns[-1]
    turn, items = last.get('turn'), last.get('items')
    if (not isinstance(turn, dict) or not isinstance(items, list)
            or type(observation.get('pid')) is not int or observation['pid'] <= 1
            or type(attempt.get('pid')) is not int or attempt['pid'] <= 1
            or not isinstance(owned.get(str(observation['pid'])), dict)
            or not isinstance(owned.get(str(attempt['pid'])), dict)):
        raise ValueError('blocked scope finding final turn identity is malformed')
    messages = [item for item in items if isinstance(item, dict)
                and item.get('type') == 'agentMessage' and item.get('phase') == 'final_answer']
    try:
        assessment = json.loads(messages[0]['text']) if len(messages) == 1 else None
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError('blocked scope finding has no final structured assessment') from exc
    if (not isinstance(assessment, dict)
            or assessment != {key: raw.get(key) for key in ('status', 'summary', 'findings')}
            or raw.get('status') != 'blocked' or raw.get('finish_reason') != 'done'
            or not isinstance(raw.get('summary'), str) or not raw['summary'].strip()
            or not isinstance(raw.get('findings'), list) or not raw['findings']
            or any(not isinstance(item, str) or not item.strip() for item in raw['findings'])
            or not _same_assessment(raw, result)
            or role.get('candidate') != seal['candidate']
            or request.get('spec') != spec or request.get('role') != 'implement'
            or request.get('iteration') != role['iteration']
            or request.get('native_authorized') is not True
            or request.get('workspace') != spec['checkout']
            or request.get('result_path') != str(folder / 'result.json')
            or request.get('start_path') != str(folder / 'start.json')
            or not isinstance(request.get('candidate'), dict)
            or request['candidate'] != state.get('candidate')
            or request['candidate'].get('id') != attempt['candidate_id']
            or role.get('input_candidate_id') != attempt['candidate_id']
            or DeliverySupervisor._job_key(request) != attempt['job_key']
            or _native_role_home(request) != home
            or environment.get('CODEX_HOME') != str(home / 'codex')
            or intent.get('environment_sha256') != digest(launch.get('environment'))
            or intent.get('argv') != launch.get('argv')
            or intent.get('run_id') != spec['run_id']
            or intent.get('policy_digest') != spec['policy_digest']
            or intent.get('cwd') != spec['checkout']
            or attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
            or attempt.get('session_id') != session or raw.get('session_id') != session
            or result.get('cleanup') != 'confirmed'
            or result.get('process_cleanup') != 'observed-native-confirmed'
            or journal.get('phase') != 'finished' or journal.get('monitoring_complete') is not True
            or native != journal.get('result')
            or native.get('journal') != str(folder / 'native-process.json')
            or type(native.get('exit_code')) is not int
            or any(native.get(key) is not False for key in ('cancelled', 'timed_out'))
            or any(native.get(key) is not True for key in ('monitoring_complete', 'stdio_drained'))
            or any(native.get(k) != v for k, v in {
                'state': 'finished', 'exit_code': 0, 'cancelled': False, 'timed_out': False,
                'monitoring_complete': True, 'stdio_drained': True,
                'cleanup': 'observed-native-confirmed'}.items())
            or metadata != {'role': 'implement', 'iteration': role['iteration'],
                'session_id': session, 'resumed_from': request.get('resume_session'),
                'result_digest': digest(result), 'output_candidate': {
                    key: seal['candidate'][key] for key in ('id', 'head', 'content_sha256')}}
            or reference.get('path') != str(folder / 'native-thread-observation.json')
            or reference.get('sha256') != hashlib.sha256(
                originals['native-thread-observation.json']).hexdigest()
            or any(reference.get(key) != observation.get(key) for key in (
                'schema', 'run_id', 'role', 'iteration', 'parent_thread_id', 'resumed_from',
                'pid', 'start_identity', 'state'))
            or reference.get('turn_count') != len(turns)
            or observation.get('schema') != 'devflow-native-kit-threads-v1'
            or observation.get('state') != 'confirmed' or reference.get('state') != 'confirmed'
            or observation.get('run_id') != spec['run_id']
            or observation.get('role') != 'implement'
            or observation.get('iteration') != role['iteration']
            or observation.get('parent_thread_id') != session
            or observation.get('resumed_from') != request.get('resume_session')
            or observation.get('new_child_thread_ids') != []
            or observation.get('collaboration_items') != []
            or start.get('pid') != observation.get('pid')
            or owned.get(str(observation.get('pid')), {}).get('identity')
                != observation.get('start_identity') or not observation.get('start_identity')
            or owned.get(str(attempt.get('pid')), {}).get('identity') != attempt['process_identity']
            or last.get('thread_id') != session or last.get('turn_id') != turn.get('id')
            or not last.get('turn_id') or turn.get('status') != 'completed'
            or turn.get('error') is not None):
        raise ValueError('blocked scope finding lost its exact completed provider assessment')
    return {'job_key': attempt['job_key'], 'session_id': session,
            'result_sha256': digest(result), 'input_candidate_id': attempt['candidate_id'],
            'output_candidate': role['candidate'], 'native_receipts': {
                name: hashlib.sha256(content).hexdigest() for name, content in originals.items()}}


def _unstarted_native_launch(spec, state, attempt, session, generation, *, input_rejection=False):
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
    message = f"JSON-RPC error -32600: no rollout found for thread id {session}"
    launch_failure = (
        result.get('summary') == 'role process failed: InvalidRequestError'
        and result.get('findings') == [message]
        and observation.get('parent_thread_id') is None
        and observation.get('turns') == []
        and observation.get('thread_inventory_before') == []
        and wrong_home != correct_home)
    if input_rejection:
        turns = observation.get('turns') or []
        start = turns[0].get('start_error', {}) if len(turns) == 1 else {}
        data = start.get('data') or {}
        message = 'JSON-RPC error -32602: Input exceeds the maximum length of 1048576 characters.'
        launch_failure = (
            result.get('summary') == 'role process failed: InvalidParamsError'
            and result.get('findings') == [message]
            and result.get('session_id') is None
            and start == {'type': 'InvalidParamsError', 'code': -32602,
                          'message': message, 'data': data}
            and set(data) == {'input_error_code', 'actual_chars', 'max_chars'}
            and data.get('input_error_code') == 'input_too_large'
            and data.get('max_chars') == 1048576
            and type(data.get('actual_chars')) is int and data['actual_chars'] > 1048576
            and turns == [{'thread_id': session, 'start_error': start}]
            and observation.get('parent_thread_id') == session
            and observation.get('thread_inventory_before') == [session]
            and wrong_home == correct_home)
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
        or not _same_assessment(raw, result)
        or result.get("status") != "blocked"
        or result.get("finish_reason") != "exception"
        or not launch_failure
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
        or observation.get("raw_turn_items") is not None
        or observation.get("collaboration_items") != []
        or observation.get("thread_inventory_after") is not None
        or observation.get("new_child_thread_ids") is not None
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
        or not _same_assessment(raw, result)
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
        rejected = latest.get('summary') == 'role process failed: InvalidParamsError'
        result['input_rejection' if rejected else 'missing_rollout'] = _unstarted_native_launch(
            spec, state, matching[0], session, generation, input_rejection=rejected)
    return result
