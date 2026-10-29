"""One fresh assessment after a closed report gate's unresolved listener conflict.

The old verdict remains failed. Authority comes from its completed history,
native receipts and a measured correction to the listener observation loop.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import UTC
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_feature_revision_subject import _native_role, _private_receipt, _runtime, _time
from .delivery_resources import read_private

CAUSE = "closed_report_listener_verification"
_CORRECTED_LOOP = "b10c13ae2bee0b252831eb8b5c13b701c63748d227302344e60d7bd59c286ccf"
_PREVIOUS_LOOP = "0fe0982d11c4724894432b46b28d2bd9b9ed333276b9bec106cb31cad6c79758"


def possible(row):
    """Selection hint only; admission authenticates the entire closed assessment."""
    previous = json.loads(row.get("recovery_json") or "null") or {}
    browser = json.loads(row.get("checks_json") or "{}").get("browser_qa") or {}
    return bool(
        previous.get("kind") == "published_gate_retry"
        and previous.get("execution_spec", {}).get("gate_retry_stage") == "report"
        and previous.get("seal", {}).get("report_observation", {}).get("cause")
        == "fixture_junit_report_layout"
        and browser.get("state") == "failed"
        and browser.get("native_process", {}).get("listener_conflict") is True
    )


def _loop(identity):
    from .delivery_feature_review_context import _protected_root, _sources

    runtime = _runtime(identity)
    sources, payload = _sources(_protected_root(), runtime['revision'])
    if payload != runtime['runtime_payload_sha256']:
        raise ValueError('listener verification authenticated runtime payload changed')
    tree = ast.parse(sources['delivery_native_process.py'])
    loops = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.For)
        and isinstance(node.iter, ast.Call)
        and isinstance(node.iter.func, ast.Name)
        and node.iter.func.id == "listeners"
    ]
    if len(loops) != 1:
        raise ValueError("listener verification lost its measured observation loop")
    return hashlib.sha256(ast.dump(loops[0], include_attributes=False).encode()).hexdigest()


def _observer(spec, old):
    from .delivery_broker import _git
    from .delivery_feature_review_context import _protected_root
    from .delivery_native_preparation import native_identity

    measured = native_identity(spec)
    current = {
        "dirty": False,
        "local_digest": None,
        "revision": _git(_protected_root(), "rev-parse", "HEAD"),
        "runtime_payload_sha256": measured["runtime_payload_sha256"],
    }
    if (
        _git(_protected_root(), "status", "--porcelain", "--untracked-files=all")
        or _loop(current) != _CORRECTED_LOOP
        or _loop(old) != _PREVIOUS_LOOP
        or current["runtime_payload_sha256"] == old["runtime_payload_sha256"]
    ):
        raise ValueError("listener verification requires its corrected clean measured observer")
    return {
        "runtime": _runtime(current),
        "previous_runtime": _runtime(old),
        "observer_sha256": _CORRECTED_LOOP,
    }


def _root(spec, resource_spec):
    from .delivery_gate_retry import gate_namespace
    from .delivery_resources import _gate_roots

    bindings = {}
    _gate_roots(resource_spec, execution_specs=bindings)
    root = Path(spec["state_dir"]) / gate_namespace(1, "report") / "evidence"
    if bindings.get(root) != spec:
        raise ValueError("listener verification lost its exact report namespace")
    return root, bindings


def _native(spec, native, folder, argv, cwd, ports, *, conflict=False):
    from .delivery_check_evidence import _owned_bytes

    journal = _private_receipt(folder / "native-process.json")
    launch = _private_receipt(folder / "launch.json")
    intent = journal.get("intent") or {}
    if (
        native.get("state") != "finished"
        or type(native.get("exit_code")) is not int
        or native["exit_code"] != 0
        or native.get("cleanup") != "observed-native-confirmed"
        or native.get("monitoring_complete") is not True
        or native.get("stdio_drained") is not True
        or any(native.get(k) is not False for k in ("timed_out", "cancelled"))
        or native.get("listener_conflict") is not conflict
        or native.get("interrupted_monitor")
        or journal.get("interrupted_monitor")
        or native.get("journal") != str(folder / "native-process.json")
        or journal.get("phase") != "finished"
        or journal.get("monitoring_complete") is not True
        or journal.get("result") != native
        or journal.get("runtime_identity") != native.get("runtime_identity")
        or intent.get("run_id") != spec["run_id"]
        or intent.get("policy_digest") != spec["policy_digest"]
        or intent.get("argv") != argv
        or launch.get("argv") != argv
        or intent.get("cwd") != str(cwd)
        or intent.get("ports") != ports
        or intent.get("environment_sha256") != digest(launch.get("environment"))
        or native.get("log") != str(folder / "process.log")
        or hashlib.sha256(_owned_bytes(Path(native["log"]))).hexdigest() != native.get("log_sha256")
    ):
        raise ValueError("listener verification lost its completed native launch or monitor")
    _runtime(native.get("runtime_identity"))
    return journal


def _checks(spec, state, broker, root):
    from .delivery_check_evidence import _owned_bytes, junit_counts, verify_manifest
    from .delivery_feature_gates import (
        bind_selector_report,
        resolve_required_selectors,
        retained_junit,
        selector_evidence,
    )
    from .delivery_plan_checks import planned_checks
    from .delivery_validation_schedule import local_checks

    candidate, iteration = state["candidate"], state["iteration"]
    gate = root / "gates" / str(iteration) / "verify"
    for stage, configured in [("prepublish", "prepublish_checks"), ("local", "checks")]:
        check = state["checks"][stage]
        folder = root / ("prechecks" if stage == "prepublish" else "checks") / str(iteration)
        recipes = list(spec["policy"][configured])
        if stage == "local" and spec["policy"].get("host_sandbox") == "trusted-local":
            planned = planned_checks(spec, broker.checkout, folder)
            recipes = local_checks(spec, recipes, planned)
        results = check.get("results") or []
        ids = {c["id"] for c in recipes}
        original = [r for r in results if r.get("id") in ids]
        extra = [r for r in results if r.get("id") not in ids]
        if (
            check.get("state") != "passed"
            or check.get("candidate_id") != candidate["id"]
            or check.get("source_unchanged") is not True
            or [r["id"] for r in original] != [c["id"] for c in recipes]
            or len({r.get("id") for r in results}) != len(results)
        ):
            raise ValueError("listener verification requires every original configured check")
        if extra:
            addon = check.get("native_addon_preparation") or {}
            path = folder / "native-addon-preparation.json"
            if (
                addon.get("state") != "passed"
                or addon.get("candidate_id") != candidate["id"]
                or addon.get("results") != extra
                or addon.get("receipt") != str(path)
                or [r["id"] for r in extra]
                != ["native-addon-node-identity", "native-addon-build", "native-addon-load"]
                or read_private(path)
                != {k: v for k, v in addon.items() if k not in {"receipt", "receipt_sha256"}}
                or hashlib.sha256(_owned_bytes(path)).hexdigest() != addon.get("receipt_sha256")
            ):
                raise ValueError("listener verification lost its admitted native prerequisites")
        for recipe, result in zip(recipes, original, strict=True):
            bound = bind_selector_report(recipe, folder / recipe["id"], prepare=False)
            if (
                result.get("argv") != bound["argv"]
                or result.get("cwd") != str(gate / recipe.get("cwd", "."))
                or result.get("plan_provenance") != recipe.get("plan_provenance")
                or result.get("satisfies") != recipe.get("satisfies")
            ):
                raise ValueError("listener verification changed its frozen check command")
            selectors = resolve_required_selectors(recipe, broker.checkout, spec=spec)
            reference = result.get("artifacts")
            if reference:
                verify_manifest(reference, candidate["id"], Path(spec["state_dir"]))
            if bound.get("junit_required"):
                counts = junit_counts(reference, candidate["id"], Path(spec["state_dir"]))
                if (
                    counts["passed"] != counts["tests"]
                    or not counts["passed"]
                    or any(counts[k] for k in ("failures", "errors", "skipped"))
                    or result.get("junit") != counts
                ):
                    raise ValueError("listener verification cannot override native test findings")
            if selectors:
                receipts = selector_evidence(
                    recipe,
                    broker.checkout,
                    selectors,
                    junit=retained_junit(reference, candidate["id"], Path(spec["state_dir"])),
                    spec=spec,
                )
                expected = [
                    {**s, "path": str(gate / Path(s["path"]).relative_to(broker.checkout))}
                    for s in receipts
                ]
                if result.get("selectors") != expected:
                    raise ValueError("listener verification lost its complete selector evidence")
        for result in results:
            if (
                result.get("passed") is not True
                or type(result.get("exit_code")) is not int
                or result["exit_code"] != 0
                or result.get("cleanup") != "confirmed"
                or result.get("rejected_output") is not False
                or result.get("rejection_causes") != []
                or result.get("evidence_failure")
            ):
                raise ValueError("listener verification cannot replace a failed local predicate")
            native = result.get("native_process") or {}
            native_folder = Path(native["journal"]).parent
            if native_folder not in {
                folder / result["id"] / name for name in ("native", "native-retry")
            }:
                raise ValueError("listener verification check left its exact admitted folder")
            command = result["argv"]
            if "/store" in command:
                dependency = result.get("dependency_preparation") or {}
                path = (
                    root
                    / "dependency-preparation"
                    / ("native-" + str(dependency.get("generation")))
                    / "receipt.json"
                )
                store_path = (
                    Path(spec["state_dir"])
                    / "transient/dependencies"
                    / (spec["policy_digest"])
                    / "staging/store"
                )
                if (
                    dependency.get("state") != "passed"
                    or dependency.get("policy_digest") != spec["policy_digest"]
                    or dependency.get("store") != str(store_path)
                    or dependency.get("receipt") != str(path)
                    or read_private(path)
                    != {
                        k: v
                        for k, v in dependency.items()
                        if k not in {"receipt", "receipt_sha256"}
                    }
                    or hashlib.sha256(_owned_bytes(path)).hexdigest()
                    != dependency.get("receipt_sha256")
                ):
                    raise ValueError(
                        "listener verification lost its frozen native dependency command"
                    )
                command = [str(store_path) if item == "/store" else item for item in command]
            log = _owned_bytes(Path(result["log"]))
            if hashlib.sha256(log).hexdigest() != result.get("log_sha256"):
                raise ValueError("listener verification native check log changed")
            _native(spec, native, native_folder, command, Path(result["cwd"]), [])
            if log != _owned_bytes(Path(native["log"])):
                raise ValueError("listener verification lost its original check output")


def _browser_selectors(qa, output, checkout, gate, sources):
    """Resolve only the authenticated deleted gate's report paths into retained source."""
    from .delivery_feature_gates import selector_evidence

    reports = []
    decoder = json.JSONDecoder()
    for line in output.splitlines():
        if not line.lstrip().startswith("{"):
            continue
        try:
            report, _ = decoder.raw_decode(output[output.index(line) :].lstrip())
        except ValueError:
            continue
        if isinstance(report, dict) and {"config", "suites", "stats"} <= report.keys():
            reports.append(report)
    if len(reports) != 1:
        raise ValueError("listener verification lost its unique Playwright report")
    report = reports[0]

    def translate(value):
        path = Path(value)
        if (not path.is_absolute() or not path.is_relative_to(gate)
                or '..' in path.parts or path.resolve() != path):
            raise ValueError("listener verification browser report escaped its original gate")
        return str(checkout / path.relative_to(gate))

    project = gate / qa.get("selector_project", qa.get("cwd", "."))
    source_roots = {gate / Path(source['selector']).parent for source in sources}
    if Path(report["config"]["rootDir"]) not in {gate, project, *source_roots}:
        raise ValueError("listener verification Playwright root differs from its frozen project")
    report["config"]["rootDir"] = translate(report["config"]["rootDir"])

    def visit(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "file" and isinstance(item, str):
                    if '..' in Path(item).parts:
                        raise ValueError('listener verification browser report source '
                                         'escaped its root')
                    if Path(item).is_absolute():
                        value[key] = translate(item)
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(report["suites"])
    return selector_evidence(qa, checkout, sources, playwright=json.dumps(report))


def _browser(spec, state, broker, root):
    from .delivery_check_evidence import _owned_bytes
    from .delivery_feature_gates import resolve_required_selectors
    from .delivery_output import observed_test_count, rejection_causes, visible_output

    browser, qa = state["checks"]["browser_qa"], spec["policy"]["browser_qa"]
    folder = root / "browser-qa" / str(state["iteration"])
    path, native = folder / "receipt.json", browser.get("native_process") or {}
    ports = qa["ports"]
    selectors = resolve_required_selectors(qa, broker.checkout, spec=spec)
    gate = root / "gates" / str(state["iteration"]) / "verify"
    original_sources = [
        {**s, "path": str(gate / Path(s["path"]).relative_to(broker.checkout))} for s in selectors
    ]
    request = {
        "iteration": state["iteration"],
        "candidate_id": state["candidate"]["id"],
        "policy_digest": spec["policy_digest"],
        "qa_config_sha256": digest(qa),
        "ports": ports,
        "argv": qa["argv"],
    }
    if selectors:
        request["selector_sources"] = original_sources
    if (
        any(browser.get(k) != v for k, v in request.items())
        or browser.get("state") != "failed"
        or type(browser.get("exit_code")) is not int
        or browser["exit_code"] != 0
        or browser.get("cleanup") != "confirmed"
        or browser.get("source_unchanged") is not True
        or browser.get("rejected_output") is not False
        or browser.get("rejection_causes") != []
        or browser.get("evidence_failure")
        or browser.get("interrupted_native_processes")
        or browser.get("receipt") != str(path)
        or _private_receipt(path)
        != {k: v for k, v in browser.items() if k not in {"receipt", "receipt_sha256"}}
        or hashlib.sha256(_owned_bytes(path)).hexdigest() != browser.get("receipt_sha256")
        or browser.get("cwd") != str(gate / qa.get("cwd", "."))
    ):
        raise ValueError(
            "listener verification requires only its original failed browser predicate"
        )
    journal = _native(
        spec,
        native,
        folder / "native",
        qa["argv"],
        Path(browser["cwd"]),
        list(ports.values()),
        conflict=True,
    )
    if set(native.get("observed_listeners") or {}) != {str(p) for p in ports.values()} or any(
        journal.get("owned", {}).get(str(item.get("pid")))
        != {k: v for k, v in item.items() if k != "pid"}
        for item in native["observed_listeners"].values()
    ):
        raise ValueError("listener verification lost its originally observed owned listeners")
    raw = _owned_bytes(folder / "browser-qa.log")
    output = visible_output(raw.decode(errors="replace"))
    count = observed_test_count(output, qa["test_count_regex"])
    causes = rejection_causes(
        output,
        [
            *([qa["reject_regex"]] if qa.get("reject_regex") else []),
            r"(?m)^\s*\d+\s+(?:failed|skipped|flaky|did not run)\b",
        ],
        test_results=True,
    )
    if (
        browser.get("log") != str(folder / "browser-qa.log")
        or hashlib.sha256(raw).hexdigest() != browser.get("log_sha256")
        or raw != _owned_bytes(Path(native["log"]))
        or causes
        or count < qa["min_tests"]
        or browser.get("test_count") != count
        or hashlib.sha256(_owned_bytes(folder / "browser-qa.sb")).hexdigest()
        != browser.get("profile_sha256")
        or not selectors
        or browser.get("selectors")
        != [
            {**s, "path": original["path"]}
            for s, original in zip(
                _browser_selectors(qa, output, broker.checkout, gate, selectors),
                original_sources,
                strict=True,
            )
        ]
    ):
        raise ValueError("listener verification cannot override browser test or selector findings")
    for item in browser.get("artifacts", []):
        path = Path(item["path"])
        if (
            not path.is_relative_to(folder / "artifacts")
            or len(_owned_bytes(path)) != item["size"]
            or hashlib.sha256(_owned_bytes(path)).hexdigest() != item["sha256"]
        ):
            raise ValueError("listener verification browser artifact custody changed")
    return request, {
        "receipt_sha256": browser["receipt_sha256"],
        "native_journal_sha256": digest(journal),
        "runtime_identity": native["runtime_identity"],
    }


def _report_records(store, spec, recovery, row, attempts, effects, closed, resource_spec):
    from .delivery_closures import sealed_closure
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_feature_closure import completed_worker
    from .delivery_feature_revision_subject import ObservationBroker

    state, original = closed["result"], recovery["state"]
    if (
        spec.get("gate_retry_stage") != "report"
        or not recovery["seal"].get("report_observation")
        or state.get("candidate") != recovery["candidate"]
        or state.get("pull_request") != recovery["publication"]
        or state.get("iteration") != original["iteration"]
        or state.get("roles", [])[:-1] != original.get("roles")
        or state.get("checks", {}).get("failure")
        != {"classification": "terminal", "stage": "browser_qa", "reason": "repair limit exhausted"}
        or state.get("error") != "repair limit exhausted"
    ):
        raise ValueError("listener verification lost its exact completed report assessment")
    role = state["roles"][-1]
    review = state["checks"].get("review") or {}
    if (
        role.get("role") != "review"
        or role.get("iteration") != state["iteration"]
        or role.get("status") != "pass"
        or role.get("findings") != []
        or role.get("candidate") != state["candidate"]
        or review.get("state") != "passed"
        or review.get("candidate_id") != state["candidate"]["id"]
    ):
        raise ValueError("listener verification requires its authenticated independent review")
    old_attempts, old_effects = recovery["seal"]["attempts"], recovery["seal"]["effects"]
    for key, current, old in [
        ("job_key", attempts, old_attempts),
        ("effect_key", effects, old_effects),
    ]:
        records = {item[key]: item for item in current}
        if len(records) != len(current) or any(records.get(item[key]) != item for item in old):
            raise ValueError("listener verification changed an original ledger row")
    added_attempts = [
        a for a in attempts if a["job_key"] not in {a["job_key"] for a in old_attempts}
    ]
    added_effects = [
        e for e in effects if e["effect_key"] not in {e["effect_key"] for e in old_effects}
    ]
    if len(added_attempts) != 1 or len(added_effects) != 1:
        raise ValueError("listener verification requires exactly the completed report actors")

    async def read():
        sealed = await sealed_closure(store, closed["workflow_id"])
        if sealed is None:
            raise ValueError("listener verification requires its sealed report history")
        authenticated = await completed_worker(sealed["history"], store, spec, row, closed)
        calls, done, times = {}, {}, {}
        review_input = None
        names = {
            "delivery_precheck": "prepublish",
            "delivery_checks": "local",
            "delivery_role": "review",
            "delivery_browser_qa": "browser_qa",
        }
        for event in sealed["history"].events:
            times[event.event_id] = event.event_time.ToDatetime(tzinfo=UTC).isoformat()
            if event.HasField("activity_task_scheduled_event_attributes"):
                item = event.activity_task_scheduled_event_attributes
                if item.activity_type.name == "delivery_publish":
                    raise ValueError("listener verification cannot replace published product work")
                if item.activity_type.name in names:
                    values = await DELIVERY_DATA_CONVERTER.decode(item.input.payloads)
                    if (
                        len(values) != 1
                        or any(
                            values[0].get(k) != v
                            for k, v in {
                                "spec": spec,
                                "candidate": state["candidate"],
                                "iteration": state["iteration"],
                            }.items()
                        )
                        or (
                            item.activity_type.name == "delivery_role"
                            and values[0].get("role") != "review"
                        )
                    ):
                        raise ValueError("listener verification report activity authority changed")
                    if item.activity_type.name == "delivery_role":
                        review_input = values[0]
                        brief = {
                            "historical_assessment_remains_rejected": True,
                            "unresolved_findings": recovery["seal"]["attribution_observation"][
                                "unresolved_findings"
                            ],
                            "controller_attribution_proof": recovery["seal"][
                                "attribution_observation"
                            ],
                        }
                        if (
                            values[0].get("check_evidence") != state["checks"]["local"]
                            or values[0].get("resume_session") is not None
                            or values[0].get("qa_evidence") is not None
                            or values[0].get("findings")
                            != [
                                "Operator acceptance criteria "
                                "(requirements to assess, not evidence of success): "
                                + json.dumps(brief, sort_keys=True)
                            ]
                            or values[0].get("evidence_context")
                            != {
                                "role_artifacts": [
                                    r["role_artifacts"]
                                    for r in original["roles"]
                                    if r.get("role_artifacts")
                                    and r["role"] == "implement"
                                    and r["iteration"] == state["iteration"]
                                ]
                            }
                        ):
                            raise ValueError(
                                "listener verification review lost its rejected predecessor context"
                            )
                    calls[event.event_id] = item.activity_type.name
            elif event.HasField("activity_task_completed_event_attributes"):
                item = event.activity_task_completed_event_attributes
                if item.scheduled_event_id in calls:
                    done[item.scheduled_event_id] = (
                        event.event_id,
                        await DELIVERY_DATA_CONVERTER.decode(item.result.payloads),
                    )
        ids = list(calls)
        if (
            [calls[i] for i in ids] != list(names)
            or set(ids) != set(done)
            or any(
                done[i][1]
                != [role if calls[i] == "delivery_role" else state["checks"][names[calls[i]]]]
                for i in ids
            )
            or any(
                not start < done[start][0] < stop for start, stop in zip(ids, ids[1:], strict=False)
            )
        ):
            raise ValueError("listener verification report history lost its exact gate results")
        review_id = ids[2]
        return (authenticated["history_sha256"], times[review_id],
                times[done[review_id][0]], review_input)

    with ThreadPoolExecutor(max_workers=1) as pool:
        history, lower, upper, review_input = pool.submit(
            copy_context().run, lambda: asyncio.run(read())).result(35)
    root, _ = _root(spec, resource_spec)
    broker = ObservationBroker(store, spec, resource_spec=resource_spec)
    _checks(spec, state, broker, root)
    session = next(r["session_id"] for r in reversed(original["roles"]) if r["role"] == "implement")
    native_review = _native_role(
        spec,
        role,
        added_attempts[0],
        session,
        _time(lower),
        _time(upper),
        recovery["candidate"],
        resource_spec=resource_spec,
        scheduled_input=review_input,
    )
    request, browser = _browser(spec, state, broker, root)
    effect = added_effects[0]
    if (
        effect["effect_key"]
        != f"browser_qa:{spec['run_id']}:{state['iteration']}:report-gates-admission"
        or effect["run_id"] != spec["run_id"]
        or effect["kind"] != "browser_qa"
        or effect["state"] != "complete"
        or json.loads(effect["request_json"]) != request
        or json.loads(effect["observed_json"] or "null") != state["checks"]["browser_qa"]
        or _time(effect["updated_at"]) > _time(closed["closed_at"])
    ):
        raise ValueError("listener verification lost its completed report browser effect")
    return {"history_sha256": history, "review": native_review, "browser": browser}


def authority(spec, recovery):
    from .delivery_continuation import continuation_authority
    from .delivery_gate_retry import KIND, _report_layout_authority

    previous = recovery.get("original_recovery") or {}
    before = previous.get("execution_spec") or {}
    proof = recovery.get("seal", {}).get("verification_observation") or {}
    if (
        recovery.get("kind") != KIND
        or proof.get("cause") != CAUSE
        or spec.get("gate_retry_stage") != "transient"
        or spec.get("gate_retry_generation") != 1
        or spec.get("gate_retry_preflight_generation")
        or spec.get("gate_retry_prepared_handoff")
        or before.get("gate_retry_stage") != "report"
        or recovery.get("command", {}).get("additional_iterations") != 0
        or recovery["seal"].get("previous_digest") != digest(previous)
        or recovery["state"] != recovery["seal"]["closed"]["result"]
        or recovery["seal"].get("attribution_observation")
        != previous["seal"].get("attribution_observation")
        or continuation_authority(spec["policy"]) != continuation_authority(before["policy"])
        or any(
            spec.get(k) != before.get(k)
            for k in (
                "run_id",
                "work_id",
                "state_dir",
                "checkout",
                "source_path",
                "branch",
                "base_sha",
                "accepted_plan",
                "feature_worker",
                "feature_delivery",
                "feature_plan_revision",
                "config_digest",
                "plan_approval",
                "authorized_endpoint",
                "origin_url",
                "base_ref",
                "publication_base_ref",
                "expected_paths",
                "implementation_role_home_generation",
                "provider",
            )
        )
    ):
        raise ValueError("listener verification changed its singleton assessment authority")
    _report_layout_authority(before, previous)
    return previous, before, proof


def historical_records(store, recovery, resource_spec):
    """Keep whole-ledger equality, attributing only the completed report's additions."""
    from . import delivery_gate_retry as gates

    current = gates._rows(store, resource_spec["run_id"])
    row, attempts, effects, _ = current
    latest = json.loads(row["recovery_json"])
    chain = gates._assessment_ancestry(latest)
    if latest["execution_spec"] != resource_spec or recovery not in chain:
        raise ValueError("historical gate ledger lost its newest admission")
    if (
        not latest["seal"].get("verification_observation")
        and not possible(row)
        and (attempts, effects) == (recovery["seal"]["attempts"], recovery["seal"]["effects"])
    ):
        return
    if latest["seal"].get("verification_observation"):
        report, before, proof = authority(resource_spec, latest)
        capture = latest["seal"]
        if (attempts, effects) != (capture["attempts"], capture["effects"]):
            raise ValueError("historical gate ledger changed after verification admission")
        report_row = {**capture["row"], "recovery_json": canonical_json(report)}
        records = _report_records(
            store,
            before,
            report,
            report_row,
            capture["attempts"],
            capture["effects"],
            capture["closed"],
            resource_spec,
        )
        if records != proof["report"]:
            raise ValueError("historical gate ledger changed after verification admission")
    elif latest["execution_spec"].get("gate_retry_stage") == "report":
        _report_records(
            store,
            resource_spec,
            latest,
            row,
            attempts,
            effects,
            store._completed_temporal_result(
                resource_spec["run_id"], workflow_id=row["workflow_id"]
            ),
            resource_spec,
        )
    else:
        raise ValueError("historical gate ledger contains unadmitted actors")
    for key, saved, original in [
        ("job_key", attempts, recovery["seal"]["attempts"]),
        ("effect_key", effects, recovery["seal"]["effects"]),
    ]:
        rows = {item[key]: item for item in saved}
        if any(rows.get(item[key]) != item for item in original):
            raise ValueError("historical gate original ledger row changed")
    if gates._rows(store, resource_spec["run_id"]) != current:
        raise ValueError("historical gate newest full ledger changed during readback")


def observe(store, spec, row, attempts, effects, closed, previous):
    from . import delivery_gate_retry as gates
    from .delivery_feature_closure import closed_worker
    from .delivery_resources import observe_completed_resources

    authenticated = closed_worker(store, spec["run_id"], closed["workflow_id"], sealed_only=True)
    if any(authenticated[k] != closed.get(k) for k in closed):
        raise ValueError("listener verification current worker closure changed")
    report = _report_records(store, spec, previous, row, attempts, effects, closed, spec)
    observer = _observer(spec, report["browser"]["runtime_identity"])
    if observer["runtime"]["runtime_payload_sha256"] in gates._consumed_payloads(
        spec, closed["result"], previous
    ):
        raise ValueError("listener verification requires an unconsumed corrected runtime")
    _, bindings = _root(spec, spec)
    if any(admitted.get("gate_retry_stage") == "transient" for admitted in bindings.values()):
        raise ValueError("this run already consumed its singleton listener verification")
    gates.readback(store, spec, previous, resource_spec=spec)
    resources = observe_completed_resources(spec, attempts)
    if (
        resources["finalization_sha256"]
        != json.loads(row["checks_json"])["resource_cleanup"]["receipt_sha256"]
    ):
        raise ValueError("listener verification lost its current completed resource receipt")
    if gates._rows(store, spec["run_id"]) != (row, attempts, effects, None):
        raise ValueError("listener verification current row or released ownership changed")
    return {"cause": CAUSE, "report": report, "observer": observer, "resources": resources}


def validate(store, spec, recovery, *, resource_spec):
    from . import delivery_gate_retry as gates
    from .delivery_check_evidence import _owned_bytes

    previous, before, proof = authority(spec, recovery)
    capture = recovery["seal"]
    row = {**capture["row"], "recovery_json": canonical_json(previous)}
    if (
        _report_records(
            store,
            before,
            previous,
            row,
            capture["attempts"],
            capture["effects"],
            capture["closed"],
            resource_spec,
        )
        != proof["report"]
    ):
        raise ValueError("listener verification original report assessment changed")
    if _observer(before, proof["report"]["browser"]["runtime_identity"]) != proof["observer"]:
        raise ValueError("listener verification corrected measured observer changed")
    historical_records(store, previous, resource_spec)
    gates.readback(store, before, previous, resource_spec=resource_spec)
    root = Path(spec["state_dir"]) / gates.gate_namespace(1, "transient") / "predecessor-resources"
    for name, key in [
        ("manifest.json", "manifest_sha256"),
        ("finalization.json", "finalization_sha256"),
    ]:
        read_private(root / name)
        if hashlib.sha256(_owned_bytes(root / name)).hexdigest() != proof["resources"][key]:
            raise ValueError(
                "listener verification lost its completed predecessor resource custody"
            )
