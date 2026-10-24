"""One acknowledged browser backend change; original gates and evidence remain frozen."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path

from .contracts import digest
from .delivery_config import DeliveryConfig
from .delivery_execution_config import load_execution_config
from .delivery_resources import read_private

FIELD = "browser_qa_amendment"
MARKER = "browser_qa_admission"
FIELDS = {FIELD}
UNDECLARED = object()
DELTA_FIELDS = {
    "expected_recipe_sha256",
    "recipe_path",
    "recipe_sha256",
    "amended_config_path",
    "amended_config_sha256",
}
MEASURED = {"native_identity", "environment_proof_sha256", "security_binding_sha256"}
FIXTURES = Path("apps/web/e2e/fixtures")
CONFIG = Path("apps/web/e2e/playwright.config.ts")
SAFETY_SOURCES = (
    CONFIG,
    Path("apps/api/src/main.ts"),
    Path("apps/api/src/e2e-dispatch.ts"),
    Path("apps/api/test/qa-seed.ts"),
)


def _bytes(raw, root, *, maximum=4 * 1024 * 1024, private=True):
    if not isinstance(raw, str):
        raise ValueError("browser amendment evidence needs an exact absolute path")
    path = Path(raw)
    if not path.is_absolute() or not path.is_relative_to(root) or path.resolve(strict=True) != path:
        raise ValueError("browser amendment evidence left its owned root")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or before.st_nlink != 1
            or before.st_size > maximum
        ):
            raise ValueError("browser amendment evidence is not a bounded private owned file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            content = stream.read(maximum + 1)
        after = os.fstat(fd)
        if len(content) != before.st_size or any(
            getattr(before, key) != getattr(after, key)
            for key in (
                "st_dev",
                "st_ino",
                "st_mode",
                "st_uid",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
        ):
            raise ValueError("browser amendment evidence changed during observation")
        return content
    finally:
        os.close(fd)


def _reference(raw, root, expected=UNDECLARED, *, maximum=4 * 1024 * 1024):
    content = _bytes(raw, root, maximum=maximum)
    actual = hashlib.sha256(content).hexdigest()
    if expected is not UNDECLARED and actual != expected:
        raise ValueError("browser amendment declared evidence hash changed")
    return content, {"path": raw, "sha256": actual}


def fixture_sources(checkout):
    """Pin reviewed fixture owners by bytes, without interpreting project code."""
    root = checkout / FIXTURES
    if not root.is_dir() or root.is_symlink():
        raise ValueError("browser amendment needs its existing owned workspace fixtures")
    required = {
        root / name for name in ("owned-workspace.cjs", "seed-workspace.cjs", "start-api.cjs")
    }
    paths = sorted([*(checkout / name for name in SAFETY_SOURCES), *root.rglob("*")])
    if not required <= set(paths) or len(paths) > 128:
        raise ValueError("browser amendment fixture-owner inventory changed")
    found, total = {}, 0
    for path in paths:
        info = path.lstat()
        if path.is_symlink() or path.resolve(strict=True) != path:
            raise ValueError("browser amendment fixture owner follows a symlink")
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError("browser amendment fixture owner is not an owned source file")
        total += info.st_size
        if total > 4 * 1024 * 1024:
            raise ValueError("browser amendment fixture owners exceed their byte bound")
        found[path.relative_to(checkout).as_posix()] = hashlib.sha256(
            _bytes(str(path), checkout, private=False)
        ).hexdigest()
    return found


def _cases(raw):
    if len(raw) > 1024 * 1024 or b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
        raise ValueError("browser amendment JUnit is not bounded ordinary XML")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise ValueError("browser amendment JUnit is malformed") from exc
    cases = []
    for item in root.iter("testcase"):
        classname, name = item.get("classname"), item.get("name")
        if not classname or not name:
            raise ValueError("browser amendment JUnit lacks exact case identities")
        state = (
            "failed"
            if item.find("failure") is not None or item.find("error") is not None
            else "skipped"
            if item.find("skipped") is not None
            else "passed"
        )
        cases.append({"classname": classname, "name": name, "state": state})
    identities = [(c["classname"], c["name"]) for c in cases]
    if len(cases) != 2 or len(set(identities)) != 2:
        raise ValueError("browser amendment requires the same two selected JUnit cases")
    return sorted(cases, key=lambda c: (c["classname"], c["name"]))


def _failed_browser(spec, state):
    """Observe the original failure; never attach a new verdict to old evidence."""
    root, qa = Path(spec["state_dir"]), spec["policy"]["browser_qa"]
    recorded = state.get("checks", {}).get("browser_qa") or {}
    raw, receipt = _reference(recorded.get("receipt"), root, recorded.get("receipt_sha256"))
    original = json.loads(raw)
    folder = Path(receipt["path"]).parent
    if (
        folder.name != str(state["iteration"])
        or folder.parent.name != "browser-qa"
        or original != {k: v for k, v in recorded.items() if k not in {"receipt", "receipt_sha256"}}
        or original.get("state") != "failed"
        or original.get("cleanup") != "confirmed"
        or original.get("process_cleanup") != "observed-native-confirmed"
        or original.get("source_unchanged") is not True
        or original.get("rejected_output") is not True
        or original.get("test_count") != 1
        or original.get("iteration") != state["iteration"]
        or original.get("candidate_id") != state["candidate"]["id"]
        or original.get("policy_digest") != spec["policy_digest"]
        or original.get("qa_config_sha256") != digest(qa)
        or original.get("argv") != qa["argv"]
        or original.get("ports") != qa["ports"]
    ):
        raise ValueError("browser amendment lost its exact failed native gate")
    sources = [receipt]
    native = original.get("native_process") or {}
    journal_path = folder / "native/native-process.json"
    journal_raw, journal_ref = _reference(str(journal_path), root)
    launch_raw, launch_ref = _reference(str(journal_path.with_name("launch.json")), root)
    journal, launch = json.loads(journal_raw), json.loads(launch_raw)
    if not all(isinstance(value, dict) for value in (native, journal, launch)):
        raise ValueError("browser amendment original process identities are malformed")
    environment, intent = launch.get("environment") or {}, journal.get("intent") or {}
    if not isinstance(environment, dict) or not isinstance(intent, dict):
        raise ValueError("browser amendment original launch is malformed")
    if (
        native.get("journal") != str(journal_path)
        or journal.get("phase") != "finished"
        or journal.get("monitoring_complete") is not True
        or journal.get("result") != native
        or native.get("monitoring_complete") is not True
        or native.get("stdio_drained") is not True
        or type(native.get("exit_code")) is not int
        or native["exit_code"] != 0
        or native.get("cancelled") is not False
        or native.get("timed_out") is not False
        or native.get("cleanup") != "observed-native-confirmed"
        or intent.get("run_id") != spec["run_id"]
        or intent.get("policy_digest") != spec["policy_digest"]
        or intent.get("argv") != launch.get("argv")
        or intent.get("argv") != original.get("effective_argv")
        or intent.get("cwd") != original.get("cwd")
        or intent.get("timeout") != qa["timeout_seconds"]
        or intent.get("ports") != list(qa["ports"].values())
        or intent.get("environment_sha256") != digest(environment)
        or any(environment.get(key) != value for key, value in qa["env"].items())
        or any(environment.get(key) != str(value) for key, value in qa["ports"].items())
    ):
        raise ValueError("browser amendment lost its original completed launch")
    sources.extend([journal_ref, launch_ref])
    raw_log, log = _reference(original.get("log"), root, original.get("log_sha256"))
    if Path(log["path"]) != folder / "browser-qa.log":
        raise ValueError("browser amendment log left its exact gate")
    from .delivery_output import observed_test_count, rejection_causes, visible_output

    output = visible_output(raw_log.decode("utf-8", errors="replace"))
    if observed_test_count(output, qa["test_count_regex"]) != 1 or not rejection_causes(
        output, [qa["reject_regex"]], test_results=True
    ):
        raise ValueError("browser amendment original failed log disagrees with its receipt")
    sources.append(log)
    reports, total = [], 0
    artifacts = original.get("artifacts")
    if not isinstance(artifacts, list) or any(not isinstance(item, dict) for item in artifacts):
        raise ValueError("browser amendment original retained artifacts are malformed")
    for item in artifacts:
        raw, reference = _reference(
            item.get("path"), root, item.get("sha256"), maximum=50 * 1024 * 1024
        )
        total += len(raw)
        if (
            not Path(reference["path"]).is_relative_to(folder / "artifacts")
            or len(raw) != item.get("size")
            or total > 50 * 1024 * 1024
        ):
            raise ValueError("browser amendment retained artifact custody changed")
        sources.append(reference)
        if Path(reference["path"]).name == "junit.xml":
            reports.append(_cases(raw))
    selected = {Path(item).name for item in qa["argv"] if item.endswith(".spec.ts")}
    if (
        len(reports) != 1
        or len(selected) != 2
        or {c["classname"] for c in reports[0]} != selected
        or sorted(c["state"] for c in reports[0]) != ["passed", "skipped"]
    ):
        raise ValueError("browser amendment needs its original passed and skipped selected cases")
    return {"sources": sources, "cases": reports[0]}


def _failed_history(seal):
    """Authenticate every recorded failure under this unchanged browser recipe."""
    from .delivery_feature_revision_recovery import retained_recovery

    spec = seal["predecessor_spec"]
    specs = {spec["policy_digest"]: spec}
    previous = seal.get("original_recovery")
    while previous:
        details = retained_recovery(previous)
        for field in ("predecessor_spec", "original_spec", "execution_spec"):
            historical = details.get(field)
            if historical:
                if (
                    historical["run_id"] != spec["run_id"]
                    or digest(historical["policy"]) != historical["policy_digest"]
                ):
                    raise ValueError("browser failure history belongs to another execution")
                specs[historical["policy_digest"]] = historical
        previous = details.get("original_recovery")
    current = seal["state"]["checks"].get("browser_qa")
    histories, matched = [], False
    keys = {"iteration", "candidate_id", "policy_digest", "qa_config_sha256", "ports", "argv"}
    for effect in seal["effects"]:
        if effect["kind"] != "browser_qa":
            continue
        if effect["state"] != "complete" or not effect["observed_json"]:
            raise ValueError("browser amendment cannot replay an uncertain browser effect")
        request, observed = json.loads(effect["request_json"]), json.loads(effect["observed_json"])
        if (
            observed.get("qa_config_sha256") != digest(spec["policy"]["browser_qa"])
            or observed.get("state") != "failed"
        ):
            continue
        historical = specs.get(observed.get("policy_digest"))
        if (
            historical is None
            or set(request) != keys
            or any(observed.get(key) != value for key, value in request.items())
            or type(observed.get("iteration")) is not int
            or not 0 <= observed["iteration"] <= seal["state"]["iteration"]
        ):
            raise ValueError("browser amendment failed effect lost its exact request")
        state = {
            "iteration": observed["iteration"],
            "candidate": {"id": observed["candidate_id"]},
            "checks": {"browser_qa": observed},
        }
        proof = _failed_browser(historical, state)
        histories.append(
            {
                "effect_key": effect["effect_key"],
                "request_sha256": digest(request),
                "result_sha256": digest(observed),
                **proof,
            }
        )
        matched = matched or observed == current
    if not matched:
        raise ValueError("browser amendment lacks its completed original failed effect")
    return {**_failed_browser(spec, seal["state"]), "history": histories}


def proposal(seal, command, *, historical_sources=None):
    original = seal["predecessor_spec"]
    if FIELD not in command:
        return original, None
    delta = command[FIELD]
    qa = original["policy"].get("browser_qa") or {}
    if (
        not isinstance(delta, dict)
        or set(delta) != DELTA_FIELDS
        or original.get("provider") != "codex"
        or original.get("feature_worker")
        or original.get("feature_delivery")
        or original.get(MARKER)
        or original["policy"].get("host_sandbox") != "trusted-local"
        or qa.get("env", {}).get("JOBCTRL_E2E_ISOLATED") != "1"
        or command["additional_iterations"] <= 0
        or not seal.get("session_id")
        or "generation" not in (seal.get("session_custody") or {})
        or any(
            not isinstance(delta[key], str) or not re.fullmatch("[0-9a-f]{64}", delta[key])
            for key in ("expected_recipe_sha256", "recipe_sha256", "amended_config_sha256")
        )
        or delta["expected_recipe_sha256"] != digest(qa)
    ):
        raise ValueError(
            "browser amendment permits only one explicit original mode1 to mode0 grant"
        )
    expected = deepcopy(qa)
    expected["env"]["JOBCTRL_E2E_ISOLATED"] = "0"
    root = Path(original["state_dir"]).parents[1]
    recipe_raw, recipe_ref = _reference(delta["recipe_path"], root, delta["recipe_sha256"])
    config_raw, config_ref = _reference(
        delta["amended_config_path"], root, delta["amended_config_sha256"]
    )
    config = DeliveryConfig._from_raw(Path(config_ref["path"]), json.loads(config_raw))
    old = load_execution_config(original)
    before, after = deepcopy(old.raw), deepcopy(config.raw)
    repository = original["repository_key"]
    if (
        before["repositories"][repository].get("browser_qa") != qa
        or after["repositories"][repository].get("browser_qa") != expected
        or json.loads(recipe_raw) != expected
    ):
        raise ValueError(
            "browser amendment changed its frozen recipe beyond the one environment value"
        )
    after["repositories"][repository]["browser_qa"] = qa
    from .delivery_amendment_capacity import acknowledge

    acknowledge(original, config)
    before.pop("capacity", None)
    after.pop("capacity", None)
    if before != after:
        raise ValueError("browser amendment changed unrelated configuration authority")
    failed = _failed_history(seal)
    sources = (
        fixture_sources(Path(original["checkout"]))
        if historical_sources is None
        else historical_sources
    )
    execution = deepcopy(original)
    execution["policy"]["browser_qa"] = expected
    execution["implementation_role_home_generation"] = seal["session_custody"]["generation"]
    execution["policy_digest"] = digest(execution["policy"])
    execution.update(
        config_path=config_ref["path"],
        config_digest=digest(config.raw),
        **{MARKER: digest({"run_id": original["run_id"], **command})},
    )
    from .delivery_native_preparation import bind_native_spec, verify_native_spec

    verify_native_spec(original)
    execution = bind_native_spec(
        execution,
        original["policy"]["native_identity"],
        Path(original["preparation"]["environment"]["path"]),
        reused=True,
    )
    return execution, {
        "original_recipe_sha256": digest(qa),
        "effective_recipe_sha256": digest(expected),
        "recipe": recipe_ref,
        "configuration": config_ref,
        "failed_browser": failed,
        "fixture_sources": sources,
    }


def _transition(expected, actual):
    derived = {
        "policy",
        "policy_digest",
        "preparation",
        "role_home_generation",
        "gate_admission",
        "implement_admission",
    }
    if (
        actual.get("role_home_generation", "") != expected.get("role_home_generation", "")
        or {k: v for k, v in actual.items() if k not in derived}
        != {k: v for k, v in expected.items() if k not in derived}
        or {k: v for k, v in actual["policy"].items() if k not in MEASURED}
        != {k: v for k, v in expected["policy"].items() if k not in MEASURED}
    ):
        raise ValueError("browser amendment changed frozen execution authority")


def custody(db, spec, recovery):
    """Require this run's acknowledged amendment in its actual retained ancestry."""
    mode = (spec["policy"].get("browser_qa") or {}).get("env", {}).get("JOBCTRL_E2E_ISOLATED")
    marker = spec.get(MARKER)
    if mode != "0":
        if marker is not None:
            raise ValueError("browser amendment marker belongs to a different recipe")
        return None
    if not isinstance(marker, str) or not re.fullmatch("[0-9a-f]{64}", marker):
        raise ValueError("nonisolated browser QA has no acknowledged setup amendment")
    from .delivery_feature_revision_recovery import retained_recovery

    item = recovery
    while item:
        details = retained_recovery(item)
        if details.get("command_digest") == marker:
            break
        item = details.get("original_recovery")
    else:
        raise ValueError("browser amendment is absent from this execution ancestry")
    path = Path(spec["state_dir"]) / "stopped-resumes" / marker / "admission.json"
    command = db.execute(
        "SELECT request_digest,response_json FROM delivery_commands "
        "WHERE command_id=? AND run_id=?",
        (details["command"]["command_id"], spec["run_id"]),
    ).fetchone()
    if (
        details.get("kind") != "stopped_delivery_resume"
        or FIELD not in details["command"]
        or details["predecessor_spec"]["run_id"] != spec["run_id"]
        or marker != digest({"run_id": spec["run_id"], **details["command"]})
        or not command
        or command[0] != marker
        or json.loads(command[1]).get("admission_sha256") != digest(item)
        or read_private(path) != item
        or details["closed"].get("request_digest") != details["predecessor_spec"]["request_digest"]
        or details["closed"].get("recovery_digest")
        != (digest(details.get("original_recovery")) if details.get("original_recovery") else None)
        or details["predecessor_result_digest"] != digest(details["state"])
        or details["maximum_iteration"]
        != (details["state"]["iteration"] + details["command"]["additional_iterations"])
    ):
        raise ValueError("browser amendment lost its immutable command acknowledgement")
    for table, key, records in (
        ("delivery_attempts", "job_key", details["attempts"]),
        ("delivery_effects", "effect_key", details["effects"]),
    ):
        for recorded in records:
            cursor = db.execute(
                f"SELECT * FROM {table} WHERE {key}=? AND run_id=?", (recorded[key], spec["run_id"])
            )
            row = cursor.fetchone()
            observed = (
                dict(zip((column[0] for column in cursor.description), row, strict=True))
                if row is not None
                else None
            )
            if observed != recorded:
                raise ValueError("browser amendment original attempt or effect changed")
    sources = _fixture_custody(spec, details)
    expected, proof = proposal(details, details["command"], historical_sources=sources)
    if (
        details.get(FIELD) != proof
        or spec["policy"]["browser_qa"] != expected["policy"]["browser_qa"]
        or spec.get(MARKER) != expected.get(MARKER)
    ):
        raise ValueError("browser amendment lost its exact proposal or original failed evidence")
    _transition(expected, details["execution_spec"])
    from .delivery_native_preparation import verify_native_spec

    verify_native_spec(details["execution_spec"])
    if details["maximum_iteration"] > details["predecessor_spec"]["policy"]["max_repairs"]:
        raise ValueError("browser amendment exceeded the original finite ceiling")
    return proof


def _fixture_custody(spec, details):
    """Keep ended source custody readable; a removed root cannot authorize a launch."""
    checkout = Path(details["predecessor_spec"]["checkout"])
    recorded = details[FIELD]["fixture_sources"]
    if os.path.lexists(checkout):
        observed = fixture_sources(checkout)
        if observed != recorded:
            raise ValueError("browser amendment owned fixture sources changed")
        return observed
    # Finalization may remove a published candidate before its projection is
    # acknowledged. Its exact ended-custody receipt preserves historical reads
    # and finalizer replay, while launch/readback still require the real source.
    # These are the immutable admission's original hashes, not current bytes.
    from .delivery_resources import same_object

    manifest = read_private(Path(spec["state_dir"]) / "resources/manifest.json")
    entry = manifest.get("roots", {}).get(str(checkout), {})
    receipt = entry.get("receipt", {})
    if (
        manifest.get("run_id") != spec["run_id"]
        or not same_object(spec, Path(spec["state_dir"]), manifest.get("state_identity"))
        or entry.get("kind") != "checkout"
        or not entry.get("identity")
        or receipt.get("path") != str(checkout)
        or receipt.get("kind") != "checkout"
        or receipt.get("state") not in {"removed", "already_absent"}
    ):
        raise ValueError("browser amendment source disappeared without its owning cleanup")
    return recorded


def launch_authority(spec, checkout=None):
    from .delivery_store import DeliveryStore

    store = DeliveryStore(load_execution_config(spec), read_only=True)
    if store.effective_spec(spec["run_id"]) != spec:
        raise ValueError("browser amendment launch differs from current execution")
    with store._connect() as db:
        row = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        proof = custody(db, spec, json.loads(row[0]) if row and row[0] else None)
        claim = store.state.claim_for(db, spec["work_id"])
        if not claim or claim["owner"] != "external:devflow:" + spec["run_id"]:
            raise ValueError("browser amendment launch lost its current owned claim")
        from .delivery_policy_recovery import work_binding

        work_binding(store, spec, db)
    if proof and checkout is not None and fixture_sources(checkout) != proof["fixture_sources"]:
        raise ValueError("browser amendment owned fixture sources changed before launch")
    return proof


def passed_cases(spec, checkout, artifacts):
    proof = launch_authority(spec, checkout)
    if proof is None:
        return None
    reports = [item for item in artifacts if Path(item["path"]).name == "junit.xml"]
    if len(reports) != 1:
        raise ValueError("browser amendment needs both selected passing JUnit cases")
    item = reports[0]
    raw, _ = _reference(item["path"], Path(spec["state_dir"]), item["sha256"])
    cases = _cases(raw)
    expected = [{**case, "state": "passed"} for case in proof["failed_browser"]["cases"]]
    if cases != expected:
        raise ValueError(
            "browser amendment did not execute both original selected cases successfully"
        )
    return {"admission": spec[MARKER], "cases": cases}
